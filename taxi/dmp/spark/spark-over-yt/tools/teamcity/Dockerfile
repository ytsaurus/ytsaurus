ARG BASE_IMAGE_TAG
FROM registry.yandex.net/taxi-dmp-spark/spyt-test-base:${BASE_IMAGE_TAG:-latest}

USER root

# we use these ids to bake the host user into image
# (you can use actual host ids if there is no docker mapping set)
ARG USER_ID
ENV USER_ID ${USER_ID:-1001}
ARG GROUP_ID
ENV GROUP_ID ${GROUP_ID:-1001}
ARG HOST_DOCKER_GROUP_ID
ENV HOST_DOCKER_GROUP_ID ${HOST_DOCKER_GROUP_ID:-999}

RUN groupadd --gid $HOST_DOCKER_GROUP_ID hostdocker
RUN groupadd --gid $GROUP_ID sbtuser && useradd -m --gid $GROUP_ID --uid $USER_ID sbtuser -G hostdocker --shell /bin/bash

ENV HOME=/home/sbtuser
USER sbtuser
ENV USER=sbtuser

RUN mkdir -p $HOME/.yt
RUN echo non_valid_token > $HOME/.yt/token

# This image copies whole application from its context to avoid
# exporting project from arc and mounting it as a volume
COPY . /app

USER root
RUN chown -R sbtuser:sbtuser /app
USER $USER
