package org.apache.spark.deploy.rest

object RestSubmissionClientWrapper {
  type Client = RestSubmissionClientSpyt

  def create(master: String): RestSubmissionClientSpyt = new RestSubmissionClientSpyt(master = master)

  def requestSubmissionStatus(client: RestSubmissionClientSpyt, id: String): SubmissionStatusResponse = {
    client.requestSubmissionStatus(id).asInstanceOf[SubmissionStatusResponse]
  }

  def requestApplicationId(client: RestSubmissionClientSpyt, id: String): AppIdRestResponse = {
    client.requestAppId(id).asInstanceOf[AppIdRestResponse]
  }

  def requestApplicationStatus(client: RestSubmissionClientSpyt, id: String): AppStatusRestResponse = {
    client.requestAppStatus(id).asInstanceOf[AppStatusRestResponse]
  }

  def killSubmission(client: RestSubmissionClientSpyt, id: String): KillSubmissionResponse = {
    client.killSubmission(id).asInstanceOf[KillSubmissionResponse]
  }
}
