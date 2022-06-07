ARG BASE_IMAGE_TAG
FROM registry.yandex.net/taxi-dmp-spark/spyt-test-base:${BASE_IMAGE_TAG:-latest}

USER root

RUN mkdir -p /root/.yt
RUN echo non_valid_token > /root/.yt/token
# This image copies whole application from its context to avoid
# exporting project from arc and mounting it as a volume
COPY . /app
RUN chown -R sbtuser:sbtuser /app

USER root
ENV USER=sbtuser
