#!/usr/bin/env python

import yt
import os
import subprocess
import argparse
from yt.wrapper.file_commands import read_file
import shutil


def download_yt_file(yt_path, local_path):
    stream = read_file(yt_path)
    with open(local_path, "w+") as f:
        for file_bytes in stream:
            f.write(file_bytes)


def extract_tar(src_path, dst_path):
    subprocess.call(["tar", "--warning=no-unknown-keyword", "-C", dst_path, "-xf", src_path])


def add_env(name, value):
    home = os.environ["HOME"]
    with open("{}/.bash_profile".format(home), "a+") as f:
        f.write("\nexport {}={}".format(name, value))
    with open("{}/.zprofile".format(home), "a+") as f:
        f.write("\nexport {}={}".format(name, value))


parser = argparse.ArgumentParser(description="Spark installer")
parser.add_argument("--proxy", required=False)
parser.add_argument("--path", required=False)

args = parser.parse_args()

spark_name = "spark-2.4.4-bin-custom-spark"
spark_yt_base_path = "//home/sashbel/spark"
spark_defaults_name = "spark-defaults.conf"
spark_local_archive_path = "/tmp/{}.tgz".format(spark_name)
spark_local_tmp_path = "/tmp/spark"
spark_local_path = args.path or os.environ["HOME"]
spark_tmp_home = "{}/{}".format(spark_local_tmp_path, spark_name)
spark_home = "{}/{}".format(spark_local_path, spark_name)


print("Removing spark..")
shutil.rmtree(spark_home)
print("Downloading spark..")
download_yt_file("{}/{}.tgz".format(spark_yt_base_path, spark_name),
                 spark_local_archive_path)
print("Spark downloaded, extracting..")
subprocess.call(["mkdir", "-p", spark_local_tmp_path])
extract_tar(spark_local_archive_path, spark_local_tmp_path)
print("Adding spark-defaults.conf")
download_yt_file("{}/{}".format(spark_yt_base_path, spark_defaults_name),
                 "{}/conf/{}".format(spark_tmp_home, spark_defaults_name))
print("Downloading spark binaries..")
download_yt_file("{}/spark-shell-yt".format(spark_yt_base_path),
                 "{}/bin/spark-shell-yt".format(spark_tmp_home))
subprocess.call(["chmod", "+x", "{}/bin/spark-shell-yt".format(spark_tmp_home)])
download_yt_file("{}/spark-submit-yt".format(spark_yt_base_path),
                 "{}/bin/spark-submit-yt".format(spark_tmp_home))
subprocess.call(["chmod", "+x", "{}/bin/spark-submit-yt".format(spark_tmp_home)])
print("Moving spark from tmp..")
subprocess.call(["sudo", "mv", spark_tmp_home, spark_home])
