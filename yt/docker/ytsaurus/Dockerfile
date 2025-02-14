# Arguments used in FROM statemensts need to be declared before the first FROM statement.

# Args for ytsaurus-server-override.
ARG BASE_REPOSITORY="ghcr.io/ytsaurus/ytsaurus-nightly"
ARG BASE_IMAGE="latest"

ARG PYTHON_BUILD_BASE="copy-ytsaurus-python-binaries"

##########################################################################################

FROM mirror.gcr.io/ubuntu:focal AS base

USER root

WORKDIR /tmp

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  curl \
  dnsutils \
  gdb \
  iproute2 \
  iputils-ping \
  less \
  libc6 \
  libidn11-dev \
  lsb-release \
  lsof \
  python3-pip \
  python3.8  \
  python3.8-distutils \
  python3.11 \
  python3.11-venv \
  strace \
  telnet \
  tini \
  unzip \
  zstd

##########################################################################################

FROM base AS build-ytsaurus-python-binaries

ARG PROTOC_VERSION="3.20.1"

RUN curl -sL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc.zip -d /usr/local \
    && rm protoc.zip

ARG SOURCE_ROOT=/ytsaurus

RUN mkdir -p ${SOURCE_ROOT}
COPY ./yt/python ${SOURCE_ROOT}/yt/python
COPY ./yt/yt/python ${SOURCE_ROOT}/yt/yt/python
COPY ./certs/cacert.pem ${SOURCE_ROOT}/certs/cacert.pem

ARG BUILD_ROOT=/tmp

RUN mkdir -p ${BUILD_ROOT}
COPY ./artifacts/libyson_lib.so ${BUILD_ROOT}/yt/yt/python/yson_shared/libyson_lib.so
COPY ./artifacts/libdriver_lib.so ${BUILD_ROOT}/yt/yt/python/driver/native_shared/libdriver_lib.so
COPY ./artifacts/libdriver_rpc_lib.so ${BUILD_ROOT}/yt/yt/python/driver/rpc_shared/libdriver_rpc_lib.so

RUN python3.11 -m venv package-env
RUN . package-env/bin/activate; \
    ${SOURCE_ROOT}/yt/python/packages/build_ytsaurus_packages.sh --ytsaurus-source-path ${SOURCE_ROOT} --ytsaurus-build-path ${BUILD_ROOT} --apply-auditwheel

##########################################################################################

FROM base AS copy-ytsaurus-python-binaries

COPY ./ytsaurus_python /tmp/ytsaurus_python

##########################################################################################

FROM ${PYTHON_BUILD_BASE} AS base-ytsaurus-python-packages

RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    python3.8 -m pip install ${wheel_path}; \
  done

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f

RUN rm -rf /tmp/ytsaurus_python

##########################################################################################

FROM base-ytsaurus-python-packages AS base-exec

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  containerd \
  openjdk-11-jdk

RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/jdk11

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.8 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release

##########################################################################################

FROM base-exec AS ytsaurus-bloated

# YTsaurus binary.
COPY ./ytserver-all /usr/bin/ytserver-all
RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-master && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-master-cache && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-clock && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-discovery && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-node && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-job-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-exec && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-http-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-kafka-proxy && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-tools && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-scheduler && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-controller-agent && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-queue-agent && \
    ln -s /usr/bin/ytserver-all /usr/bin/ytserver-tcp-proxy

# YTsaurus credits files.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS

# Scripts for init jobs.
COPY ./init_queue_agent_state.py /usr/bin/init_queue_agent_state
COPY ./init_operations_archive.py /usr/bin/init_operations_archive
RUN ln -s /usr/bin/init_operations_archive /usr/bin/init_operation_archive

FROM scratch AS ytsaurus
COPY --from=ytsaurus-bloated / /

##########################################################################################

FROM base AS chyt

# CHYT binaries.
COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse

# CHYT credits files.
COPY ./credits/ytserver-clickhouse.CREDITS /usr/bin/ytserver-clickhouse.CREDITS

# Install YT client for setup script.
RUN python3.8 -m pip install ytsaurus-client
RUN ln -s /usr/local/bin/yt /usr/bin/yt -f

# Setup script.
COPY ./setup_cluster_for_chyt.sh /setup_cluster_for_chyt.sh
RUN chmod 755 /setup_cluster_for_chyt.sh

##########################################################################################

FROM base-ytsaurus-python-packages AS query-tracker-bloated

# Libpython shared objects. TODO(aleksandr.gaev) use stubs instead.
RUN apt-get install -y libpython3.8 libpython3.9 libpython3.10 libpython3.11 libpython3.12

# Query tracker binaries.
COPY ./ytserver-all /usr/bin/ytserver-all
RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-query-tracker

COPY ./ytserver-yql-agent /usr/bin/ytserver-yql-agent
COPY ./mrjob /usr/bin/mrjob
COPY ./dq_vanilla_job /usr/bin/dq_vanilla_job
COPY ./dq_vanilla_job.lite /usr/bin/dq_vanilla_job.lite
COPY ./yql /usr/lib/yql

# Scripts for init jobs.
COPY ./init_query_tracker_state.py /usr/bin/init_query_tracker_state

# Query tracker credits files.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS

FROM scratch AS query-tracker
COPY --from=query-tracker-bloated / /

##########################################################################################

FROM base AS strawberry

# Strawberry binary.
COPY ./chyt-controller /usr/bin/strawberry-controller
RUN ln -s /usr/bin/strawberry-controller /usr/bin/chyt-controller

# Strawberry credits files.
COPY ./credits/chyt-controller.CREDITS /usr/bin/strawberry-controller.CREDITS

##########################################################################################

FROM base-exec AS local-bloated

COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS

FROM scratch AS local
COPY --from=local-bloated / /

WORKDIR /tmp

COPY ./configure.sh .
RUN ./configure.sh /var/lib/yt/local-cypress
RUN rm /tmp/configure.sh

COPY ./start.sh /usr/bin/start.sh

VOLUME /var/lib/yt/local-cypress
EXPOSE 80

ENTRYPOINT ["bash", "/usr/bin/start.sh"]

##########################################################################################

FROM ${BASE_REPOSITORY}:${BASE_IMAGE} AS ytsaurus-server-override-bloated

USER root

# Override binaries built from source.
COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./init_queue_agent_state.py /usr/bin/init_queue_agent_state
COPY ./init_operations_archive.py /usr/bin/init_operations_archive

FROM scratch AS ytsaurus-server-override
COPY --from=ytsaurus-server-override-bloated / /

##########################################################################################
