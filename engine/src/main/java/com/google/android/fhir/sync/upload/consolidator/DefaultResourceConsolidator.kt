/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.android.fhir.sync.upload.consolidator

import com.google.android.fhir.LocalChange
import com.google.android.fhir.LocalChangeToken
import com.google.android.fhir.db.Database
import com.google.android.fhir.logicalId
import org.hl7.fhir.r4.model.Bundle
import org.hl7.fhir.r4.model.Resource
import org.hl7.fhir.r4.model.ResourceType
import timber.log.Timber

/** Default implementation of [ResourceConsolidator] that uses the database to aid consolidation. */
internal class DefaultResourceConsolidator(private val database: Database) : ResourceConsolidator {

  override suspend fun consolidate(localChanges: List<LocalChange>, response: Resource) {
    val localChangeToken = LocalChangeToken(localChanges.flatMap { it.token.ids })
    database.deleteUpdates(localChangeToken)
    when (response) {
      is Bundle -> updateVersionIdAndLastUpdated(response)
      else -> updateVersionIdAndLastUpdated(localChanges, response)
    }
  }

  private suspend fun updateVersionIdAndLastUpdated(bundle: Bundle) {
    // TODO: Support POST in Bundle transactions. Assumption is that only PUT operations are used in
    // Bundle requests
    when (bundle.type) {
      Bundle.BundleType.TRANSACTIONRESPONSE -> {
        bundle.entry.forEach {
          when {
            it.hasResource() -> updateVersionIdAndLastUpdated(it.resource)
            it.hasResponse() -> updateVersionIdAndLastUpdated(it.response)
          }
        }
      }
      else -> {
        // Leave it for now.
        Timber.i("Received request to update meta values for ${bundle.type}")
      }
    }
  }

  private suspend fun updateVersionIdAndLastUpdated(response: Bundle.BundleEntryResponseComponent) {
    if (response.hasEtag() && response.hasLastModified() && response.hasLocation()) {
      response.resourceIdAndType?.let { (id, type) ->
        database.updateVersionIdAndLastUpdated(
          id,
          type,
          getVersionFromETag(response.etag),
          response.lastModified.toInstant(),
        )
      }
    }
  }

  private suspend fun updateVersionIdAndLastUpdated(resource: Resource) {
    if (resource.hasMeta() && resource.meta.hasVersionId() && resource.meta.hasLastUpdated()) {
      database.updateVersionIdAndLastUpdated(
        resource.id,
        resource.resourceType,
        resource.meta.versionId,
        resource.meta.lastUpdated.toInstant(),
      )
    }
  }

  private suspend fun updateVersionIdAndLastUpdated(
    localChanges: List<LocalChange>,
    resource: Resource,
  ) {
    if (localChanges.first().resourceId != resource.logicalId) {
      database.updateResourceAndId(localChanges.first().resourceId, resource)
    }
    if (resource.hasMeta() && resource.meta.hasVersionId() && resource.meta.hasLastUpdated()) {
      database.updateVersionIdAndLastUpdated(
        resource.id,
        resource.resourceType,
        resource.meta.versionId,
        resource.meta.lastUpdated.toInstant(),
      )
    }
  }

  /**
   * FHIR uses weak ETag that look something like W/"MTY4NDMyODE2OTg3NDUyNTAwMA", so we need to
   * extract version from it. See https://hl7.org/fhir/http.html#Http-Headers.
   */
  private fun getVersionFromETag(eTag: String) =
    // The server should always return a weak etag that starts with W, but if it server returns a
    // strong tag, we store it as-is. The http-headers for conditional upload like if-match will
    // always add value as a weak tag.
    if (eTag.startsWith("W/")) {
      eTag.split("\"")[1]
    } else {
      eTag
    }

  /**
   * May return a Pair of versionId and resource type extracted from the
   * [Bundle.BundleEntryResponseComponent.location].
   *
   * [Bundle.BundleEntryResponseComponent.location] may be:
   * 1. absolute path: `<server-path>/<resource-type>/<resource-id>/_history/<version>`
   * 2. relative path: `<resource-type>/<resource-id>/_history/<version>`
   */
  private val Bundle.BundleEntryResponseComponent.resourceIdAndType: Pair<String, ResourceType>?
    get() =
      location
        ?.split("/")
        ?.takeIf { it.size > 3 }
        ?.let { it[it.size - 3] to ResourceType.fromCode(it[it.size - 4]) }
}