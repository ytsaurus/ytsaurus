FROM ubuntu:focal

WORKDIR /tmp

ARG YT_LOCAL_PATH=yt_local
ARG YTSERVER_ALL_PATH=ytserver-all
ARG YTSERVER_ALL_CREDITS_PATH=ytserver-all.CREDITS

COPY ${YT_LOCAL_PATH} /usr/bin/yt_local
COPY ${YTSERVER_ALL_PATH} /usr/bin/ytserver-all
COPY ${YTSERVER_ALL_CREDITS_PATH} /usr/bin/ytserver-all.CREDITS

COPY configure.sh .
RUN ./configure.sh /var/lib/yt/local-cypress
RUN rm /tmp/configure.sh

COPY start.sh /usr/bin/start.sh

VOLUME /var/lib/yt/local-cypress
EXPOSE 80

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
    python \
    pip \
    openjdk-11-jdk

RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/jdk11
ENV JAVA_HOME /opt/jdk11
RUN export JAVA_HOME

RUN pip install -i https://pypi.org/simple/ \
    ytsaurus-local \
    ytsaurus-client

ENTRYPOINT ["bash", "/usr/bin/start.sh"]
