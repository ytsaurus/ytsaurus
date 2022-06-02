ARG BASE_IMAGE_TAG
FROM registry.yandex.net/taxi-dmp-spark/spyt-test-base:${BASE_IMAGE_TAG:-latest}

USER root

# This image copies whole application from its context to avoid
# exporting project from arc and mounting it as a volume
COPY . /app
RUN chown -R sbtuser:sbtuser /app

USER sbtuser
ENV USER=sbtuser
