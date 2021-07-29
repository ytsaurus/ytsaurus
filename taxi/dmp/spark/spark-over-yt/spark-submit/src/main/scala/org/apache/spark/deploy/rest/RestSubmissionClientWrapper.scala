package org.apache.spark.deploy.rest

object RestSubmissionClientWrapper {
  def create(master: String) = new RestSubmissionClient(master = master, sparkConf = None)

  def requestSubmissionStatus(client: RestSubmissionClient, id: String): SubmissionStatusResponse = {
    client.requestSubmissionStatus(id).asInstanceOf[SubmissionStatusResponse]
  }
}
