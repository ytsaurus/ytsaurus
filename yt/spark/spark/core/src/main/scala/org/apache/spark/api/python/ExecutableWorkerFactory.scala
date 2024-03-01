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

package org.apache.spark.api.python

import java.io._
import java.net.{InetAddress, ServerSocket, Socket}

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.api.python.PythonWorkerFactory.PROCESS_WAIT_TIMEOUT_MS
import org.apache.spark.deploy.ExecutableEnv._
import org.apache.spark.util.Utils

// Copied from PythonWorkerFactory and adjusted to work with executable files
private[spark] class ExecutableWorkerFactory(execPath: String, envVars: Map[String, String])
  extends PythonWorkerFactory(execPath, envVars) {

  override val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  override protected def createSimpleWorker(): (Socket, Option[Int]) = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))

      // Create and start the worker
      val args = MainArgs.deserialize(envVars(MainArgs.envName))
      val command = (execPath +: args).asJava
      val pb = new ProcessBuilder(command)
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
      workerEnv.put("PYTHONPATH", pythonPath)
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      workerEnv.put("PYTHON_WORKER_FACTORY_PORT", serverSocket.getLocalPort.toString)
      workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      workerEnv.put(RunMode.envName, RunMode.WORKER)

      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Wait for it to connect to our socket, and validate the auth secret.
      serverSocket.setSoTimeout(10000)

      try {
        val socket = serverSocket.accept()
        authHelper.authClient(socket)
        val pid = new DataInputStream(socket.getInputStream).readInt()
        if (pid < 0) {
          throw new IllegalStateException("Python failed to launch worker with code " + pid)
        }
        synchronized {
          simpleWorkers.put(socket, worker)
        }
        return (socket, Some(pid))
      } catch {
        case e: Exception =>
          throw new SparkException("Executable worker failed to connect back.", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  override protected def startDaemon() {
    synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val args = MainArgs.deserialize(envVars(MainArgs.envName))
        val command = (execPath +: args).asJava
        val pb = new ProcessBuilder(command)
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        workerEnv.put(RunMode.envName, RunMode.DAEMON)

        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        try {
          daemonPort = in.readInt()
        } catch {
          case _: EOFException =>
            throw new SparkException(s"No port number in $daemonModule's stdout")
        }

        // test that the returned port number is within a valid range.
        // note: this does not cover the case where the port number
        // is arbitrary data but is also coincidentally within range
        if (daemonPort < 1 || daemonPort > 0xffff) {
          val exceptionMessage = f"""
            |Bad data in $daemonModule's standard output. Invalid port number:
            |  $daemonPort (0x$daemonPort%08x)
            |Python command to execute the daemon was:
            |  ${command.asScala.mkString(" ")}
            |Check that you don't have any unexpected modules or libraries in
            |your PYTHONPATH:
            |  $pythonPath
            |Also, check if you have a sitecustomize.py module in your python path,
            |or in your python installation, that is printing to standard output"""
          throw new SparkException(exceptionMessage.stripMargin)
        }

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)
      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
              |Error from executable worker:
              |  $formattedStderr
              |PYTHONPATH was:
              |  $pythonPath
              |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }
}


