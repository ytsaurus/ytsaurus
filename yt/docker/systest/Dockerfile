FROM ubuntu:focal

WORKDIR /tmp
USER root

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
  python3.7 \
  python3-pip \
  python3.7-distutils \
  openjdk-11-jdk

COPY ./ytsaurus_python /tmp/ytsaurus_python
COPY ./new_stress_test /usr/new_stress_test

RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    echo "DIST_DIR: ${dist_dir}"; \
    echo "WHEEL_PATH: ${wheel_path}"; \
    python3.7 -m pip install ${wheel_path}; \
  done

COPY ./systest /usr/bin/systest
COPY benchmark-0.0.1.tgz /tmp

RUN tar -C /usr/bin -xzf benchmark-0.0.1.tgz
