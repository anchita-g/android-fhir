/*
 * Copyright 2021 Google LLC
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

package com.google.android.fhir.demo.data

import com.google.android.fhir.demo.DemoDataStore
import com.google.android.fhir.sync.DownloadWorkManager
import com.google.android.fhir.sync.Request
import org.hl7.fhir.exceptions.FHIRException
import org.hl7.fhir.r4.model.*
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import timber.log.Timber
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.collections.HashSet


class LimitedPatientResourcesDownloadWorkManagerImpl(dataStore: DemoDataStore) : DownloadWorkManager {
  private var downloadSize = 100
  private var maxResourceDownloadCount = hashMapOf<ResourceType, Int> (
    ResourceType.Patient to 200,
    ResourceType.Observation to 20,
    ResourceType.Encounter to 25,
    ResourceType.Immunization to 10,
  )
  private val urls = LinkedList(listOf((enforceCount("Patient", getDownloadCount(ResourceType.Patient)))))
  private var resourceDownloadedCount = 0
  private var urlsHit = 0
  private var startTime = LocalDateTime.now()
  private var currentPatientsDownloaded = 0
  private var resourcesDownloaded = HashMap<ResourceType, HashSet<String>>()
  private var resourcesDownloadedCountMap = HashMap<ResourceType, Int>()


  private fun getDownloadCount(resourceType: ResourceType): Int {
    return if (maxResourceDownloadCount.get(resourceType)!! > downloadSize) downloadSize else maxResourceDownloadCount.get(resourceType)!!
  }

  override suspend fun getNextRequest(): Request? {
    val url = urls.poll()
    urlsHit+=1
    if (url == null) {
      val nowTime = LocalDateTime.now()
      val timeElapsed = ChronoUnit.MILLIS.between(startTime, nowTime)
      Timber.i("Downloaded $resourceDownloadedCount in $timeElapsed millis over $urlsHit url hits")
      for ((key, value) in resourcesDownloaded) {
        Timber.i("$key => Unique values = ${value.size}, Total count = ${resourcesDownloadedCountMap.getOrDefault(key, 0)}")
      }
      return null
    }
    return Request.Companion.of(url)
  }

  override suspend fun getSummaryRequestUrls(): Map<ResourceType, String> {
    return emptyMap()
  }

  override suspend fun processResponse(response: Resource): Collection<Resource> {
    // As per FHIR documentation :
    // If the search fails (cannot be executed, not that there are no matches), the
    // return value SHALL be a status code 4xx or 5xx with an OperationOutcome.
    // See https://www.hl7.org/fhir/http.html#search for more details.
    if (response is OperationOutcome) {
      throw FHIRException(response.issueFirstRep.diagnostics)
    }

    // If the resource returned is a List containing Patients, extract Patient references and fetch
    // all resources related to the patient using the $everything operation.
    if (response is ListResource) {
      for (entry in response.entry) {
        val reference = Reference(entry.item.reference)
        if (reference.referenceElement.resourceType.equals("Patient")) {
          val patientUrl = "${entry.item.reference}/\$everything"
          urls.add(patientUrl)
        }
      }
    }

    // Finally, extract the downloaded resources from the bundle.
    var bundleCollection: Collection<Resource> = mutableListOf()
    if (response is Bundle && response.type == Bundle.BundleType.SEARCHSET) {
      bundleCollection = response.entry.map { it.resource }
      resourceDownloadedCount+=bundleCollection.size
    }

    // If the resource returned is a Bundle, check to see if there is a "next" relation referenced
    // in the Bundle.link component, if so, append the URL referenced to list of URLs to download.
    if (response is Bundle) {
      updateResourceCount(response)
      addNextUrls(response)
      val nextUrl = response.link.firstOrNull { component -> component.relation == "next" }?.url
      if (nextUrl != null) {
        if (shouldFetchNextUrl(response)) {
          urls.add(nextUrl)
        }
      }
    }

    return bundleCollection
  }

  private fun shouldFetchNextUrl(bundle: Bundle): Boolean {
    if (bundle.entry.all { it.resource.hasType(ResourceType.Patient.name) }) {
      return checkResourceDownloadedCountIsLesser(ResourceType.Patient)
    }
    if (bundle.entry.all { it.resource.hasType(ResourceType.Observation.name) }) {
      return checkResourceDownloadedCountIsLesser(ResourceType.Observation)
    }
    if (bundle.entry.all { it.resource.hasType(ResourceType.Encounter.name) }) {
      return checkResourceDownloadedCountIsLesser(ResourceType.Encounter)
    }
    if (bundle.entry.all { it.resource.hasType(ResourceType.Immunization.name) }) {
      return checkResourceDownloadedCountIsLesser(ResourceType.Immunization)
    }
    return true
  }

  private fun addNextUrls(bundle: Bundle) {
    if (bundle.entry.all { it.resource.hasType(ResourceType.Patient.name) }) {
      addPatientEncounterResourceDownloadUrlsFromBundle(bundle)
    }
    if (bundle.entry.all { it.resource.hasType(ResourceType.Encounter.name) }) {
      addEncounterRelatedResourceDownloadUrlsFromBundle(bundle)
    }
  }

  private fun extractId(resource: BundleEntryComponent): String {
    val urlSplit = resource.fullUrl.split("/")
    return urlSplit[urlSplit.size - 1]
  }

  private fun updateResourceCount(bundle: Bundle) {
    val downloadedResourceTypes = bundle.entry.groupBy { entry -> entry.resource.resourceType }
    downloadedResourceTypes.forEach { entry ->
      val resourceType = entry.key
      val existingResourceIds = resourcesDownloaded.getOrDefault(resourceType, hashSetOf())
      existingResourceIds.addAll(entry.value.map { extractId(it) })
      resourcesDownloaded.put(resourceType, existingResourceIds)
      val totalResourcesDownloadedCount = resourcesDownloadedCountMap.getOrDefault(resourceType, 0)
      resourcesDownloadedCountMap.put(resourceType, totalResourcesDownloadedCount + entry.value.size)
    }
  }

  private fun checkResourceDownloadedCountIsLesser(resourceType: ResourceType): Boolean {
    return resourcesDownloaded.getOrDefault(resourceType, hashSetOf()).size < maxResourceDownloadCount[resourceType]!!
  }

   private fun addPatientEncounterResourceDownloadUrlsFromBundle(bundle: Bundle) {
     currentPatientsDownloaded+=bundle.entry.size
    // Extract the patient resources from the bundle and append the URLs for the related resources to fetch for each of these patients
    bundle.entry.filter {
      ResourceType.fromCode(it.resource.fhirType()) == ResourceType.Patient
    }.forEach {
      val urlSplit = it.fullUrl.split("/")
      val patientId = urlSplit[urlSplit.size - 1]
      urls.add(enforceCount("Encounter?subject=${patientId}&_sort=_lastUpdated", getDownloadCount(ResourceType.Encounter)))
      urls.add(enforceCount("Observation?subject=${patientId}&_sort=_lastUpdated", getDownloadCount(ResourceType.Observation)))
      urls.add(enforceCount("Immunization?patient=${patientId}&_sort=_lastUpdated", getDownloadCount(ResourceType.Immunization)))
    }
  }



  private fun addEncounterRelatedResourceDownloadUrlsFromBundle(bundle: Bundle) {
    // Extract the encounter resources from the bundle and append the URLs for the encounters related resources
    val encounters = bundle.entry.filter {
      ResourceType.fromCode(it.resource.fhirType()) == ResourceType.Encounter
    }.map { it.resource as Encounter }
    val practitionerIds = encounters.map {encounter ->
      encounter.participant
        .filter { it.individual.referenceElement.hasResourceType() }
        .filter { it.individual.referenceElement.resourceType == ResourceType.Practitioner.name }
        .map { it.individual.referenceElement.idPart }
    }.flatten().distinct()
    val organizationIds = encounters.map { encounter ->
      encounter.serviceProvider.referenceElement.idPart
    }.distinct()
    urls.add("Practitioner?_id=${practitionerIds.joinToString(",")}")
    urls.add("Organization?_id=${organizationIds.joinToString(",")}")

  }
}

/**
 * Affixes the last updated timestamp to the request URL.
 *
 * If the request URL includes the `$everything` parameter, the last updated timestamp will be
 * attached using the `_since` parameter. Otherwise, the last updated timestamp will be attached
 * using the `_lastUpdated` parameter.
 */

private fun enforceCount(url: String, count: Int): String {
  val split = url.split("?")
  val prefix = split[0]
  if (split.size > 1) {
    val pairs: List<String> = split[1].split("&")
    val sb = StringBuilder()
    sb.append(prefix)
    sb.append("?")
    var countExists = false
    pairs.forEach {
      qp -> if (qp.startsWith("_count")) {
        sb.append("_count=${count}")
      countExists =true
    } else {
      sb.append(qp)
    }
      sb.append("&")
    }
    if (!countExists) {
      sb.append("_count=${count}")
    }
    if (sb.lastIndexOf("&") == sb.length -1){
      sb.deleteCharAt(sb.length -1)
    }
    return sb.toString()
  } else {
    return "${url}?_count=${count}"
  }
}
