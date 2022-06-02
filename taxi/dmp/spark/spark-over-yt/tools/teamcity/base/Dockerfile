# simplified version of https://github.com/hseeberger/scala-sbt/blob/master/debian/Dockerfile

ARG BASE_IMAGE_TAG
FROM registry.yandex.net/rtc-base/focal:${BASE_IMAGE_TAG:-stable}

ARG SCALA_VERSION
ENV SCALA_VERSION ${SCALA_VERSION:-2.12.14}
ARG SBT_VERSION
ENV SBT_VERSION ${SBT_VERSION:-1.5.4}

# add docker for dind tests
RUN apt-get update && apt-get -y install docker.io default-jdk

RUN \
  curl -fsL "https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz" | tar xfz - -C /usr/share && \
  chown -R root:root /usr/share/sbt && \
  chmod -R 755 /usr/share/sbt && \
  ln -s /usr/share/sbt/bin/sbt /usr/local/bin/sbt

RUN \
  curl -fsL https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /usr/share && \
  mv /usr/share/scala-$SCALA_VERSION /usr/share/scala && \
  chown -R root:root /usr/share/scala && \
  chmod -R 755 /usr/share/scala && \
  ln -s /usr/share/scala/bin/* /usr/local/bin && \
  echo "println(util.Properties.versionMsg)" > test.scala && \
  scala -nocompdaemon test.scala && rm test.scala

WORKDIR /app

RUN groupadd --gid 1001 sbtuser && useradd -m --gid 1001 --uid 1001 sbtuser -G docker --shell /bin/bash
RUN chown sbtuser:sbtuser /app
USER sbtuser

# Prepare sbt (warm cache)
RUN \
  sbt sbtVersion && \
  mkdir -p project && \
  echo "scalaVersion := \"${SCALA_VERSION}\"" > build.sbt && \
  echo "sbt.version=${SBT_VERSION}" > project/build.properties && \
  echo "// force sbt compiler-bridge download" > project/Dependencies.scala && \
  echo "case object Temp" > Temp.scala && \
  sbt compile && \
  rm -r project && rm build.sbt && rm Temp.scala && rm -r target

CMD ["sbt", "test"]
