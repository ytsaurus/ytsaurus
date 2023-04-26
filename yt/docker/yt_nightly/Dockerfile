# TODO: download base image from the local registry.
FROM ubuntu:jammy

WORKDIR /tmp

USER root

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
  curl \
  less \
  gdb \
  lsof \
  strace \
  telnet \
  dnsutils \
  iputils-ping \
  lsb-release \
  python3-pip \
  openjdk-11-jdk

# This is required for jobs, using yson format in python 3.
RUN pip3 install -i https://pypi.org/simple/ \
    ytsaurus-client \
    ytsaurus-yson \
    ytsaurus-local

COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./init_operation_archive.py /usr/bin/init_operation_archive

COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse
COPY ./ytserver-log-tailer /usr/bin/ytserver-log-tailer
COPY ./chyt-controller /usr/bin/chyt-controller
COPY ./ytsaurus_native_driver-1.0.0-cp34-abi3-linux_x86_64.whl /tmp/ytsaurus_native_driver-1.0.0-py2.py3-none-any.whl 

RUN pip3 install /tmp/ytsaurus_native_driver-1.0.0-py2.py3-none-any.whl 

RUN ln /usr/bin/ytserver-all /usr/bin/ytserver-master && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-clock && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-discovery && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-node && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-job-proxy && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-exec && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-proxy && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-http-proxy && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-tools && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-scheduler && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-controller-agent && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-query-tracker

RUN cp /usr/local/bin/yt /usr/bin/yt
