# simplified version of https://github.com/hseeberger/scala-sbt/blob/master/debian/Dockerfile

ARG BASE_IMAGE_TAG
FROM registry.yandex.net/rtc-base/bionic:${BASE_IMAGE_TAG:-stable}

ARG SCALA_VERSION
ENV SCALA_VERSION ${SCALA_VERSION:-2.12.14}
ARG SBT_VERSION
ENV SBT_VERSION ${SBT_VERSION:-1.5.4}

# add docker for dind tests
RUN apt-get update && apt-get -y install docker docker.io default-jdk python3.7 python3-pip
RUN python3 -m pip install pip
RUN python3 -m pip install tox twine "setuptools<60.0.0" click

RUN apt-get install -y locales && locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

RUN \
   curl -fsL https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz | tar xfz - -C /usr/share && \
   chown -R root:root /usr/share/apache-maven-3.6.3 && \
   chmod -R 755 /usr/share/apache-maven-3.6.3 && \
   ln -s /usr/share/apache-maven-3.6.3/bin/mvn /usr/local/bin/mvn

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

ENTRYPOINT /bin/bash

CMD ["sbt", "test"]
