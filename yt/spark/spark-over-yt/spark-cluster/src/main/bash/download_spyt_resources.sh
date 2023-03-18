#!/bin/bash

PROXY="$1.yt.yandex.net"
RELEASE_TYPE="$2"
CLIENT_VERSION="$3"
CLUSTER_VERSION="$4"
SPARK_FORK_VERSION="$5"
OUT_DIRECTORY="$6"

# TODO(alex-shishkin): Add validation

echo "Downloading resources from $PROXY."
echo "Release type: $RELEASE_TYPE"
echo "Client: $CLIENT_VERSION."
echo "Cluster: $CLUSTER_VERSION."
echo "Spark fork: $SPARK_FORK_VERSION"
echo "Output directory: $OUT_DIRECTORY"
echo ""

mkdir -p $OUT_DIRECTORY

REMOTE_SPARK_HOME="//home/spark"
LOCAL_SPARK_HOME="$OUT_DIRECTORY/home/spark"

download() {
  resource=$1
  mkdir -p "$(dirname "$LOCAL_SPARK_HOME/$resource")"
  yt read-file --proxy "$PROXY" "$REMOTE_SPARK_HOME/$resource" > "$LOCAL_SPARK_HOME/$resource" &&
    echo "Downloaded file $resource"
}

download_document() {
  resource=$1
  mkdir -p "$(dirname "$LOCAL_SPARK_HOME/$resource")"
  yt get --proxy "$PROXY" "$REMOTE_SPARK_HOME/$resource" > "$LOCAL_SPARK_HOME/$resource" &&
    echo "Downloaded document $resource"
}

download "bin/$RELEASE_TYPE/$CLUSTER_VERSION/spark-yt-launcher.jar"

download_document "conf/global"
download "conf/$RELEASE_TYPE/$CLUSTER_VERSION/metrics.properties"
download "conf/$RELEASE_TYPE/$CLUSTER_VERSION/solomon-agent.template.conf"
download "conf/$RELEASE_TYPE/$CLUSTER_VERSION/solomon-service-master.template.conf"
download "conf/$RELEASE_TYPE/$CLUSTER_VERSION/solomon-service-worker.template.conf"
download_document "conf/$RELEASE_TYPE/$CLUSTER_VERSION/spark-launch-conf"
#download "conf/$RELEASE_TYPE/$CLUSTER_VERSION/ytserver-proxy.template.yson"

#download "delta/python/layer_with_python34.tar.gz"
#download "delta/python/layer_with_python37_libs_3.tar.gz"
#download "delta/python/layer_with_python38_libs.tar.gz"
#download "delta/python/layer_with_python311_libs.tar.gz"
#download "delta/layer_with_solomon_agent.tar.gz"
#download "delta/layer_with_solomon_agent.tar.gz"

download "spark/$RELEASE_TYPE/$SPARK_FORK_VERSION/spark.tgz"

download "spyt/$RELEASE_TYPE/$CLIENT_VERSION/spark-yt-data-source.jar"
download "spyt/$RELEASE_TYPE/$CLIENT_VERSION/spyt.zip"

echo ""

echo "! Please add symlink $REMOTE_SPARK_HOME/$RELEASE_TYPE/$CLUSTER_VERSION/spark.tgz"\
  "to $REMOTE_SPARK_HOME/$RELEASE_TYPE/$SPARK_FORK_VERSION/spark.tgz"
