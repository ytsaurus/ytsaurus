package org.apache.spark.deploy.rest

object RestSubmissionClientWrapper {
  type Client = RestSubmissionClient

  def create(master: String): RestSubmissionClient = new RestSubmissionClient(master = master, sparkConf = None)

  def requestSubmissionStatus(client: RestSubmissionClient, id: String): SubmissionStatusResponse = {
    client.requestSubmissionStatus(id).asInstanceOf[SubmissionStatusResponse]
  }

  def killSubmission(client: RestSubmissionClient, id: String): KillSubmissionResponse = {
    client.killSubmission(id).asInstanceOf[KillSubmissionResponse]
  }
}
