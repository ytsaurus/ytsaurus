FROM ubuntu:focal

USER root

ARG BUILD_OUTPUT_PATH

RUN mkdir /data

COPY ${BUILD_OUTPUT_PATH}/spark_version.json /data/
COPY ${BUILD_OUTPUT_PATH}/spark.tgz /data/
