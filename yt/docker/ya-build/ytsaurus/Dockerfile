ARG BASE_UBUNTU="mirror.gcr.io/ubuntu:focal"

ARG SOURCE_ROOT="/ytsaurus"
ARG BUILD_ROOT="/build"

ARG PROTOC_VERSION="3.20.1"

FROM $BASE_UBUNTU as build-python-packages

ARG SOURCE_ROOT
ENV SOURCE_ROOT $SOURCE_ROOT

ARG BUILD_ROOT
ENV BUILD_ROOT $BUILD_ROOT

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get install -y --no-install-recommends \
      curl \
      unzip \
      python3 \
      python3-pip \
      python3.11 \
      python3.11-venv \
    && rm -rf /var/lib/apt/lists/*

ARG PROTOC_VERSION
ENV PROTOC_VERSION $PROTOC_VERSION

RUN curl -sL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc.zip -d /usr/local \
    && rm protoc.zip

RUN mkdir -p ${SOURCE_ROOT}
COPY ./yt/python ${SOURCE_ROOT}/yt/python
COPY ./yt/yt/python ${SOURCE_ROOT}/yt/yt/python
COPY ./certs/cacert.pem ${SOURCE_ROOT}/certs/cacert.pem

RUN mkdir -p ${BUILD_ROOT}
COPY ./artifacts/libyson_lib.so ${BUILD_ROOT}/yt/yt/python/yson_shared/libyson_lib.so
COPY ./artifacts/libdriver_lib.so ${BUILD_ROOT}/yt/yt/python/driver/native_shared/libdriver_lib.so
COPY ./artifacts/libdriver_rpc_lib.so ${BUILD_ROOT}/yt/yt/python/driver/rpc_shared/libdriver_rpc_lib.so

RUN python3.11 -m venv package-env
RUN . package-env/bin/activate; \
    ${SOURCE_ROOT}/yt/python/packages/build_ytsaurus_packages.sh --ytsaurus-source-path ${SOURCE_ROOT} --ytsaurus-build-path ${BUILD_ROOT} --apply-auditwheel

FROM ubuntu:focal as build-ytsaurus

USER root

WORKDIR /tmp

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all

# If this list changes, also update yt_nightly/Dockerfile
RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-master && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-master-cache && \
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
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-queue-agent && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-tcp-proxy

COPY ./init_queue_agent_state.py /usr/bin/init_queue_agent_state
COPY ./init_operations_archive.py /usr/bin/init_operations_archive
RUN ln -s /usr/bin/init_operations_archive /usr/bin/init_operation_archive

# CREDITS files with licenses for all binaries.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS

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
  dnsutils \
  iputils-ping \
  lsb-release \
  openjdk-11-jdk \
  python3.8 \
  python3-pip \
  libidn11-dev \
  python3.8-distutils

COPY --from=build-python-packages ./build/ytsaurus_python /tmp/ytsaurus_python
RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    python3.8 -m pip install ${wheel_path}; \
  done

RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/jdk11

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.8 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f
