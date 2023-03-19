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

package org.apache.spark.deploy.worker

import java.io.{File, IOException}
import java.net.URI

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object FileUtils extends Logging {
  private val DOWNLOAD_SUFFIX = ".download"
  private val DOWNLOAD_FS_SUFFIX = ".downloadFs"

  def downloadFile(sourceUrl: String, destDir: File, conf: SparkConf): String = {
    val fileName = new URI(sourceUrl).getPath.split("/").last
    val localJarFile = new File(destDir, fileName)
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user file $sourceUrl to $localJarFile")
      Utils.fetchFile(
        sourceUrl,
        destDir,
        conf,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)
      if (!localJarFile.exists()) { // Verify copy succeeded
        throw new IOException(
          s"Can not find expected file $fileName which should have been loaded in $destDir")
      }
    }
    localJarFile.getAbsolutePath
  }

  def substituteFiles(opt: String, dir: File, conf: SparkConf): String = {
    def substitute(name: String, files: String, prefix: String): String = {
      val downloadedFiles = files.split(",")
        .map(file => s"$prefix${downloadFile(file, dir, conf)}")
      val newOpt = s"$name=${downloadedFiles.mkString(",")}"
      logInfo(s"Substitute $opt with $newOpt")
      newOpt
    }

    if (opt.contains("://")) {
      val name :: files :: Nil = opt.split("=", 2).toList
      name match {
        case _ if name.endsWith(DOWNLOAD_SUFFIX) =>
          substitute(name.dropRight(DOWNLOAD_SUFFIX.length), files, "")
        case _ if name.endsWith(DOWNLOAD_FS_SUFFIX) =>
          substitute(name.dropRight(DOWNLOAD_FS_SUFFIX.length), files, "file://")
        case _ => opt
      }
    } else opt
  }
}
