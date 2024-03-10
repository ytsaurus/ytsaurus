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

package tech.ytsaurus.spark.launcher

import java.io.{File, PrintWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.ScalaObjectMapper

import org.apache.spark.internal.Logging

object AddressUtils extends Logging {
  def writeAddressToFile(name: String,
                         host: String,
                         port: Int,
                         webUiPort: Option[Int],
                         restPort: Option[Int]): Unit = {
    logInfo(s"Writing address to file: $port, $webUiPort, $restPort")

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val addressString = mapper.writeValueAsString(Map(
      "host" -> host,
      "port" -> port,
      "webUiPort" -> webUiPort,
      "restPort" -> restPort
    ))

    val pw = new PrintWriter(new File(s"${name}_address"))
    try {
      pw.write(addressString)
      require(new File(s"${name}_address_success").createNewFile())
    } finally {
      pw.close()
    }
  }

}
