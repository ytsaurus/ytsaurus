/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.rest

import java.lang.Boolean
import java.util.Date

import org.apache.spark.deploy.master.WorkerInfo

/**
 * An abstract response sent from the server in the REST application submission protocol.
 */
private[rest] abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  var serverSparkVersion: String = null
  var success: Boolean = null
  var unknownFields: Array[String] = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(serverSparkVersion, "serverSparkVersion")
  }
}

/**
 * A response to a [[CreateSubmissionRequest]] in the REST application submission protocol.
 */
private[spark] class CreateSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a kill request in the REST application submission protocol.
 */
private[spark] class KillSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class AppIdRestResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var appId: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class AppStatusRestResponse extends SubmitRestProtocolResponse {
  var appId: String = null
  var appState: String = null
  var appSubmittedAt: Date = null
  var appStartedAt: Long = -1L

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(appId, "appId")
    assertFieldIsSet(appState, "appState")
    assertFieldIsSet(success, "success")
    assertFieldIsSet(appSubmittedAt, "appStartedAt")
  }
}

private[spark] class AppStatusesRestResponse extends SubmitRestProtocolResponse {
  var statuses: Seq[AppStatusRestResponse] = Seq()

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(statuses, "statuses")
  }
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class SubmissionStatusResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var driverState: String = null
  var workerId: String = null
  var workerHostPort: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

private[spark] class SubmissionsStatus {
  var driverId: String = null
  var status: String = null
  var startedAt: Long = -1L
}

private[spark] class SubmissionStatusesResponse extends SubmitRestProtocolResponse {
  var statuses: Seq[SubmissionsStatus] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
    assertFieldIsSet(statuses, "statuses")
  }
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class MasterStateResponse extends SubmitRestProtocolResponse {
  var workers: Array[WorkerInfo] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(workers, "workers")
  }
}

/**
 * An error response message used in the REST application submission protocol.
 */
private[rest] class ErrorResponse extends SubmitRestProtocolResponse {
  // The highest protocol version that the server knows about
  // This is set when the client specifies an unknown version
  var highestProtocolVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(message, "message")
  }
}
