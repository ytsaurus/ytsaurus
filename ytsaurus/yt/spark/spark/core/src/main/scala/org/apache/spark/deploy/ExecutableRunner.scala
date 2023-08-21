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

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkUserAppException}
import org.apache.spark.api.python.PythonUtils
import org.apache.spark.deploy.PythonRunner.formatPaths
import org.apache.spark.internal.config.PYSPARK_PYTHON
import org.apache.spark.util.{RedirectThread, Utils}

// Copied from PythonRunner with some small changes
object ExecutableRunner {
  def main(args: Array[String]) {
    val execFile = args(0)
    val pyFiles = args(1)
    val execArgs = args.slice(2, args.length)
    val sparkConf = new SparkConf()
    val secret = Utils.createSecret(sparkConf)

    val formattedExecFile = PythonRunner.formatPath(execFile)
    val formattedPyFiles = PythonRunner.resolvePyFiles(formatPaths(pyFiles))

    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val localhost = InetAddress.getLoopbackAddress()
    val gatewayServer = new py4j.GatewayServer.GatewayServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
      .build()
    val thread = new Thread(new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions {
        gatewayServer.start()
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()

    // Wait until the gateway server has started, so that we know which port is it bound to.
    // `gatewayServer.start()` will start a new thread and run the server code there, after
    // initializing the socket, so the thread started above will end as soon as the server is
    // ready to serve connections.
    thread.join()

    // Build up a PYTHONPATH that includes the Spark assembly (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    // All client dependencies should come baked into the executable;
    // this is done for spyt and pyspark to work without having to include it in each executable
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)

    // Launch Executable
    val builder = new ProcessBuilder((Seq(formattedExecFile) ++ execArgs).asJava)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    env.put(ExecutableEnv.RunMode.envName, ExecutableEnv.RunMode.DRIVER)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    env.put("PYSPARK_GATEWAY_SECRET", secret)
    // pass conf spark.pyspark.python to python process, the only way to pass info to
    // python process is through environment variable.
    sparkConf.get(PYSPARK_PYTHON).foreach(env.put("PYSPARK_PYTHON", _))
    sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    try {
      val process = builder.start()

      new RedirectThread(process.getInputStream, System.out, "redirect output").start()

      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new SparkUserAppException(exitCode)
      }
    } finally {
      gatewayServer.shutdown()
    }
  }
}
