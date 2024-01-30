
package org.apache.spark.deploy.ytsaurus

import tech.ytsaurus.client.rpc.YTsaurusClientAuth

object YTsaurusUtils {

  val URL_PREFIX = "ytsaurus://"

  def parseMasterUrl(masterURL: String): String = {
    masterURL.substring(URL_PREFIX.length)
  }

  def userAndToken(): (String, String) = {
    val user = sys.env.get("YT_SECURE_VAULT_YT_USER").orNull
    val token = sys.env.get("YT_SECURE_VAULT_YT_TOKEN").orNull
    if (user == null || token == null) {
      val auth = YTsaurusClientAuth.loadUserAndTokenFromEnvironment()
      (auth.getUser.orElseThrow(), auth.getToken.orElseThrow())
    } else {
      (user, token)
    }
  }
}
