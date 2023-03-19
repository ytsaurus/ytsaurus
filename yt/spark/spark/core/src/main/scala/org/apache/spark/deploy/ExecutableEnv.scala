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

package org.apache.spark.deploy

import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.mutable.ArrayBuffer

sealed abstract class ExecutableEnv(val envName: String)


object ExecutableEnv {
  case object RunMode extends ExecutableEnv("SPYT_RUN_MODE") {
    val DRIVER = "spyt_driver"
    val DAEMON = "spyt_daemon"
    val WORKER = "spyt_worker"
  }

  case object MainArgs extends ExecutableEnv("SPYT_MAIN_ARGS") {
    private val mapper = new ObjectMapper()

    def serialize(args: ArrayBuffer[String]): String = {
      mapper.writeValueAsString(args.toArray)
    }

    def deserialize(args: String): Seq[String] = {
      import scala.collection.JavaConverters._
      mapper.readTree(args).iterator().asScala.map(_.asText()).toList
    }
  }
}
