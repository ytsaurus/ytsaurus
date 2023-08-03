FROM ubuntu:focal

USER root

WORKDIR /tmp

ARG SPYT_VERSION=1.69.3
ARG SPARK_VERSION=1.69.0

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all

# If this list changes, also update yt_nightly/Dockerfile
RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-master && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-clock && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-discovery && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-node && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-job-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-exec && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-http-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-tools && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-scheduler && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-controller-agent && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-query-tracker

COPY ./init_operation_archive.py /usr/bin/init_operation_archive

# CHYT bits and pieces.
COPY ./chyt-controller /usr/bin/chyt-controller
COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse
COPY ./ytserver-log-tailer /usr/bin/ytserver-log-tailer

# SPYT pieces.
COPY ./spyt_release/home/spark/conf/releases/${SPARK_VERSION}/metrics.properties /usr/bin/metrics.properties
COPY ./spyt_release/home/spark/conf/releases/${SPARK_VERSION}/solomon-agent.template.conf /usr/bin/solomon-agent.template.conf
COPY ./spyt_release/home/spark/conf/releases/${SPARK_VERSION}/solomon-service-master.template.conf /usr/bin/solomon-service-master.template.conf
COPY ./spyt_release/home/spark/conf/releases/${SPARK_VERSION}/solomon-service-worker.template.conf /usr/bin/solomon-service-worker.template.conf
COPY ./spyt_release/home/spark/spark/releases/${SPARK_VERSION}/spark.tgz /usr/bin/spark.tgz
COPY ./spyt_release/home/spark/spyt/releases/${SPYT_VERSION}/spark-yt-data-source.jar /usr/bin/spark-yt-data-source.jar
COPY ./spyt_release/home/spark/spyt/releases/${SPYT_VERSION}/spyt.zip /usr/bin/spyt.zip
COPY ./spyt_release/home/spark/bin/releases/${SPARK_VERSION}/spark-yt-launcher.jar /usr/bin/spark-yt-launcher.jar

# CREDITS files with licenses for all binaries.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS
COPY ./credits/chyt-controller.CREDITS  /usr/bin/chyt-controller.CREDITS
COPY ./credits/ytserver-clickhouse.CREDITS /usr/bin/ytserver-clickhouse.CREDITS
COPY ./credits/ytserver-log-tailer.CREDITS  /usr/bin/ytserver-log-tailer.CREDITS

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  curl \
  less \
  gdb \
  lsof \
  strace \
  telnet \
  dnsutils \
  iputils-ping \
  lsb-release \
  openjdk-11-jdk \
  python3.11 \
  python3-pip \
  python3.11-distutils

RUN python3.11 -m pip install \
    ytsaurus-spyt==${SPYT_VERSION}

COPY ./ytsaurus_python /tmp/ytsaurus_python
RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    echo "DIST_DIR: ${dist_dir}"; \
    echo "WHEEL_PATH: ${wheel_path}"; \
    python3.11 -m pip install ${wheel_path}; \
  done

RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/jdk11

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.11 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f

# TODO: add opportunity to get this files in opensource
# Static binaries & shared libraries built in arcadia.
# In order to obtain them, run the following command in yt/yql/package:
#   ya package package.json --raw-package-path yql_package
# And move the resulting directory to the context of docker build.
COPY ./yql_package/ytserver-yql-agent /usr/bin/
COPY ./yql_package/mrjob /usr/bin/
COPY ./yql_package/lib*.so /usr/lib/yql/
