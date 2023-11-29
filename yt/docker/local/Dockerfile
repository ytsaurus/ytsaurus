FROM ubuntu:focal

WORKDIR /tmp

COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS

COPY ./configure.sh .
RUN ./configure.sh /var/lib/yt/local-cypress
RUN rm /tmp/configure.sh

COPY ./start.sh /usr/bin/start.sh

VOLUME /var/lib/yt/local-cypress
EXPOSE 80

RUN apt-get update && apt-get install -y openjdk-11-jdk

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
    python3.8 \
    python3-pip \
    python3.8-distutils

COPY ./ytsaurus_python /tmp/ytsaurus_python
RUN for package in client local; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    python3.8 -m pip install ${wheel_path}; \
  done

RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/jdk11

ENTRYPOINT ["bash", "/usr/bin/start.sh"]
