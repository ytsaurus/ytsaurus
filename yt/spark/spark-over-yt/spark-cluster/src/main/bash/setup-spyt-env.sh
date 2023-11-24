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

spark_root="$spark_home/spark"

mkdir -p $spark_home
tar --warning=no-unknown-keyword -xf spark.tgz -C $spark_home

if [ -f spark-extra.zip ]; then
  unzip -o spark-extra.zip -d $spark_root
fi

if [ $enable_livy ]; then
  tar --warning=no-unknown-keyword -xf livy.tgz -C $spark_home
fi
