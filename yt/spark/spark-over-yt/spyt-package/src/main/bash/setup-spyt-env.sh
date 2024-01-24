#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
  case $1 in
    --spark-home)
      spark_home="$2"
      shift
      shift
      ;;
    --enable-livy)
      enable_livy=1
      shift
      ;;
    *)
      echo "Unknown argument $1"
      exit 1
      ;;
  esac
done

if [[ -z $spark_home ]]; then
  echo "Parameter --spark-home should be set"
  exit 1
fi

spyt_home="$HOME/$spark_home/spyt-package"

mkdir -p $spark_home
tar --warning=no-unknown-keyword -xf spark.tgz -C $spark_home

if [ -f spyt-package.zip ]; then
  unzip -o spyt-package.zip -d "$spark_home"
  javaagent_opt="-javaagent:$(ls $spyt_home/lib/*spark-yt-spark-patch*)"
  echo "$javaagent_opt" > $spyt_home/conf/java-opts
fi

if [ $enable_livy ]; then
  tar --warning=no-unknown-keyword -xf livy.tgz -C $spark_home
fi
