FROM ubuntu:focal

USER root

WORKDIR /tmp

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all

# If this list changes, also update yt_nightly/Dockerfile
RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-master && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-clock && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-discovery && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-node && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-job-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-exec && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-http-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-tools && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-scheduler && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-controller-agent && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-query-tracker && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-tcp-proxy

COPY ./init_operation_archive.py /usr/bin/init_operation_archive

COPY ./chyt-controller /usr/bin/chyt-controller

# CREDITS files with licenses for all binaries.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS
COPY ./credits/chyt-controller.CREDITS  /usr/bin/chyt-controller.CREDITS

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  containerd \
  curl \
  less \
  gdb \
  lsof \
  strace \
  telnet \
  tini \
  zstd \
  unzip \
  netcat \
  net-tools \
  mtr \
  dnsutils \
  iputils-ping \
  lsb-release \
  openjdk-11-jdk \
  libidn11-dev \
  python3.10 \
  python3.10-distutils

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

COPY ./ytsaurus_python /tmp/ytsaurus_python
RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    echo "DIST_DIR: ${dist_dir}"; \
    echo "WHEEL_PATH: ${wheel_path}"; \
    python3.10 -m pip install ${wheel_path}; \
  done

RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/jdk11

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.10 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f

