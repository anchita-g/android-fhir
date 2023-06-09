/*
 * Copyright 2022 Google LLC
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

import ca.uhn.fhir.rest.gclient.UriClientParam
import com.google.android.fhir.FhirEngine
import com.google.android.fhir.demo.DemoDataStore
import com.google.android.fhir.demo.care.ConfigurationManager.getCareConfigurationResources
import com.google.android.fhir.logicalId
import com.google.android.fhir.search.Search
import com.google.android.fhir.sync.DownloadWorkManager
import com.google.android.fhir.workflow.CarePlanManager
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.LinkedList
import java.util.Locale
import org.hl7.fhir.exceptions.FHIRException
import org.hl7.fhir.r4.model.ActivityDefinition
import org.hl7.fhir.r4.model.Bundle
import org.hl7.fhir.r4.model.CanonicalType
import org.hl7.fhir.r4.model.CarePlan
import org.hl7.fhir.r4.model.Condition
import org.hl7.fhir.r4.model.ListResource
import org.hl7.fhir.r4.model.OperationOutcome
import org.hl7.fhir.r4.model.Patient
import org.hl7.fhir.r4.model.PlanDefinition
import org.hl7.fhir.r4.model.PractitionerRole
import org.hl7.fhir.r4.model.Reference
import org.hl7.fhir.r4.model.Resource
import org.hl7.fhir.r4.model.ResourceType
import org.hl7.fhir.r4.model.Task
import timber.log.Timber
import java.lang.UnsupportedOperationException

class TimestampBasedDownloadWorkManagerImpl(
  private val fhirEngine: FhirEngine,
  private val dataStore: DemoDataStore,
  private val carePlanManager: CarePlanManager
) : DownloadWorkManager {
  private val resourceReferences = mutableMapOf<ResourceType, MutableSet<String>>()
  private val resourceTypeOrder = listOf(ResourceType.Task, ResourceType.CarePlan, ResourceType.PlanDefinition, ResourceType.PractitionerRole, ResourceType.Condition, ResourceType.Observation, ResourceType.Encounter, ResourceType.Organization, ResourceType.Location)
  private val patientIds = mutableSetOf<String>()
  private var fetchedCompletedTasks = false
  private var fetchedConditions = false
  private var fetchedObservations = false

  private val conditionCodes = listOf<String>(
    //breast cancer symptoms
    "248816004", "248807003", "248817008", "290113009", "127189005",
    //oral cancer symptoms
    "26284000", "249403003", "285466001",
    //cervical cancer symptoms
    "248966006", "399131003", "289564005", "279039007"
  )
  private val taskIdentifiers = listOf<String>(
    "breast-cancer-screening-task", "cervical-cancer-screening-task",
    "hypertension-screening-task", "diabetes-mellitus-screening-task",
    "oral-cancer-screening-task"
  )

  private val urls =
    LinkedList(
      listOf(
        "Task?owner=PractitionerRole/81417c6a-a6ab-453c-a26b-c08791161045&status=ready&_count=100",
        "PlanDefinition",
        "Patient"
      )
    )

  override suspend fun getNextRequestUrl(): String? {
    val url = urls.poll()
    println("polled url $url")
    if (url != null) {
      return url
    }

    for (resourceType in resourceTypeOrder) {

      if (resourceType == ResourceType.Condition && !fetchedConditions) {
        val u = addAndReturnConditionRequests()
        if (u!=null) {
          return u
        }
      }

      if (resourceType == ResourceType.Observation && !fetchedObservations) {
        val u = addAndReturnObservationRequests()
        if (u!=null) {
          return u
        }
      }

      if (resourceType == ResourceType.Task && !fetchedCompletedTasks) {
        val u = addAndReturnTaskRequests()
        if (u!=null) {
          return u
        }
      }

      if (resourceReferences.getOrDefault(resourceType, emptySet<String>()).isNotEmpty()) {
        return constructUrlRequestFromReference(resourceType, resourceReferences.get(resourceType)!!)
      }
    }
    return null
  }

  private suspend fun fetchAllPatientIds() {
    if (patientIds.isEmpty()) {
      val search = Search(type = ResourceType.Patient)
      val pids = fhirEngine.search<Patient>(search).map { it.logicalId }
      patientIds.addAll(pids)
    }
  }

  private suspend fun addAndReturnConditionRequests(): String? {
    fetchedConditions = true
    return addAndReturnRequest(ResourceType.Condition, "subject", "code:coding:code="+conditionCodes.joinToString(","))
  }

  private fun constructSearchUrl(patientIds: List<String>, resourceType: ResourceType, queryString: String, patientQp: String): String {
    return resourceType.name + "?" + patientQp + "=" + patientIds.joinToString(",") + "&_count=100&" + queryString
  }

  private suspend fun addAndReturnObservationRequests(): String? {
    fetchedObservations = true
    return addAndReturnRequest(ResourceType.Observation, "subject", "code:coding:code=CBAC")
  }

  private suspend fun addAndReturnTaskRequests(): String? {
    fetchedCompletedTasks = true
    return addAndReturnRequest(ResourceType.Task, "patient", "identifier:value="+taskIdentifiers.joinToString(",")+"&status=completed")
  }

  private suspend fun addAndReturnRequest(resourceType: ResourceType, patientQp: String, queryString: String): String? {
    fetchAllPatientIds()
    var firstUrl: String? = null
    if (patientIds.isNotEmpty()) {
      patientIds.chunked(100).forEach { chunkedPatientIds ->
        val surl = constructSearchUrl(chunkedPatientIds, resourceType, queryString, patientQp)
        if (firstUrl == null) {
          firstUrl = surl
        } else {
          urls.add(surl)
        }
      }
    }
    return firstUrl
  }

  private fun constructUrlRequestFromReference(resourceType: ResourceType, set: Set<String>): String {
    val url = resourceType.name+"?_id="+set.joinToString(",")
    resourceReferences[resourceType] = mutableSetOf()
    println(url)
    return url
  }

  private fun constructUrlRequestFromQueryParams(resourceType: ResourceType, set: Set<Map<String, String>>): String {
    return resourceType.name+"/"+set.joinToString(",")
  }

  private fun constructUrlRequestFromUrls(resourceType: ResourceType, set: Set<CanonicalType>): String {
    val parameter = when (resourceType) {
      ResourceType.PlanDefinition -> "url"
      else -> throw UnsupportedOperationException("d")
    }
    return resourceType.name + "?" + "${parameter}=" + set.map { it.value }.joinToString(",")
  }

  override suspend fun getSummaryRequestUrls(): Map<ResourceType, String> {
    return emptyMap()
  }

  private fun collectConditionReferences(condition: Condition) {
    addToBeDownloadedResources(condition.encounter)
  }

  private fun collectTaskReferences(task: Task) {
    val taskReferences = mutableListOf<Reference>()
    task.basedOn.firstOrNull()?.let { ref -> taskReferences.add(ref) }
    // Questionannire downloaded as part of plan definition
    //task.focus?.let { ref -> taskReferences.add(ref) }
    // Patients will be downloaded as it is when we download all the patients assigned to this CHW
    //task.`for`?.let { ref -> taskReferences.add(ref) }
    task.requester?.let { ref -> taskReferences.add(ref) }
    task.owner?.let { ref -> taskReferences.add(ref) }
    println("task references are "+ taskReferences.joinToString(",") { it.reference })
    taskReferences.forEach { ref ->
      println("Collecting reference for "+ ref.reference)
      addToBeDownloadedResources(ref)
    }
  }

  private fun collectPractitionerRoleReferences(practitionerRole: PractitionerRole) {
    val roleReferences = mutableListOf<Reference>()
    //practitionerRole.practitioner?.let { ref -> roleReferences.add(ref) }
    practitionerRole.organization?.let { ref -> roleReferences.add(ref) }
    practitionerRole.location?.firstOrNull()?.let { ref -> roleReferences.add(ref) }
    println("role references are "+ roleReferences.joinToString(",") { it.reference })
    roleReferences.forEach { ref ->
      println("Collecting reference for "+ ref.reference)
      addToBeDownloadedResources(ref)
    }
  }

  private fun addToBeDownloadedResources(reference: Reference) {
    val resourceType = ResourceType.fromCode(reference.reference.split("/")[0])
    val resourceId = reference.reference.split("/")[1]
    val idsToBeDownloaded = resourceReferences.getOrDefault(resourceType, mutableSetOf())
    idsToBeDownloaded.add(resourceId)
    println("Adding resources for "+ resourceType + " : "+ idsToBeDownloaded.joinToString(","))
    resourceReferences[resourceType] = idsToBeDownloaded


  }

  override suspend fun processResponse(response: Resource): Collection<Resource> {
    // As per FHIR documentation :
    // If the search fails (cannot be executed, not that there are no matches), the
    // return value SHALL be a status code 4xx or 5xx with an OperationOutcome.
    // See https://www.hl7.org/fhir/http.html#search for more details.
    if (response is OperationOutcome) {
      throw FHIRException(response.issueFirstRep.diagnostics)
    }


    // If the resource returned is a Bundle, check to see if there is a "next" relation referenced
    // in the Bundle.link component, if so, append the URL referenced to list of URLs to download.
    if (response is Bundle) {
      val nextUrl = response.link.firstOrNull { component -> component.relation == "next" }?.url
      if (nextUrl != null) {
        urls.add(nextUrl)
      }
    }

    // Finally, extract the downloaded resources from the bundle.
    var bundleCollection: Collection<Resource> = mutableListOf()
    if (response is Bundle && response.type == Bundle.BundleType.SEARCHSET) {
      bundleCollection =
        response.entry
          .map { it.resource }
          .also { extractAndSaveLastUpdateTimestampToFetchFutureUpdates(it) }

      println("Bundle resources size " + response.entry.size)

      for (item in response.entry) {
        if (item.resource is PlanDefinition) {
          bundleCollection +=
            carePlanManager.getPlanDefinitionDependentResources(item.resource as PlanDefinition)
          bundleCollection += getCareConfigurationResources()
        }
        if (item.resource is Task) {
          println("Parsing task resource " + item.resource.id)
          collectTaskReferences(item.resource as Task)
        }
        if (item.resource is PractitionerRole) {
          println("Parsing PractitionerRole resource " + item.resource.id)
          collectPractitionerRoleReferences(item.resource as PractitionerRole)
        }
        if (item.resource is Condition) {
          println("Parsing Condition resource " + item.resource.id)
          collectConditionReferences(item.resource as Condition)
        }
      }
    }
    return bundleCollection
  }

  private suspend fun extractAndSaveLastUpdateTimestampToFetchFutureUpdates(
    resources: List<Resource>
  ) {
    resources
      .groupBy { it.resourceType }
      .entries.map { map ->
        dataStore.saveLastUpdatedTimestamp(
          map.key,
          map.value.maxOfOrNull { it.meta.lastUpdated }?.toTimeZoneString() ?: ""
        )
      }
  }
}

/**
 * Affixes the last updated timestamp to the request URL.
 *
 * If the request URL includes the `$everything` parameter, the last updated timestamp will be
 * attached using the `_since` parameter. Otherwise, the last updated timestamp will be attached
 * using the `_lastUpdated` parameter.
 */
private fun affixLastUpdatedTimestamp(url: String, lastUpdated: String): String {
  var downloadUrl = url

  // Affix lastUpdate to a $everything query using _since as per:
  // https://hl7.org/fhir/operation-patient-everything.html
  if (downloadUrl.contains("\$everything")) {
    downloadUrl = "$downloadUrl?_since=$lastUpdated"
  }

  // Affix lastUpdate to non-$everything queries as per:
  // https://hl7.org/fhir/operation-patient-everything.html
  if (!downloadUrl.contains("\$everything")) {
    downloadUrl = "$downloadUrl?_lastUpdated=gt$lastUpdated"
  }

  // Do not modify any URL set by a server that specifies the token of the page to return.
  if (downloadUrl.contains("&page_token")) {
    downloadUrl = url
  }

  return downloadUrl
}

private fun Date.toTimeZoneString(): String {
  val simpleDateFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.getDefault())
      .withZone(ZoneId.systemDefault())
  return simpleDateFormat.format(this.toInstant())
}
