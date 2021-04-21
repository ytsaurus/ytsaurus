package org.apache.spark.deploy.rest

object RestSubmissionClientWrapper {
  def create(master: String) = new RestSubmissionClient(master)

  def requestSubmissionStatus(client: RestSubmissionClient, id: String): SubmissionStatusResponse = {
    client.requestSubmissionStatus(id).asInstanceOf[SubmissionStatusResponse]
  }

}
