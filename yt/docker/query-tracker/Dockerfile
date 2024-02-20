FROM ubuntu:focal

USER root

WORKDIR /tmp

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./ytserver-yql-agent /usr/bin/ytserver-yql-agent
COPY ./mrjob /usr/bin/mrjob
COPY ./yql /usr/lib/yql

RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-query-tracker

COPY ./init_query_tracker_state.py /usr/bin/init_query_tracker_state

# CREDITS files with licenses for all binaries.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS
# TODO(nadya73): add other credits

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  curl \
  less \
  gdb \
  lsof \
  zstd \
  strace \
  telnet \
  dnsutils \
  iputils-ping \
  lsb-release \
  openjdk-11-jdk \
  python3.8 \
  python3-pip \
  libidn11-dev \
  python3.8-distutils

COPY ./ytsaurus_python /tmp/ytsaurus_python
RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    echo "DIST_DIR: ${dist_dir}"; \
    echo "WHEEL_PATH: ${wheel_path}"; \
    python3.8 -m pip install ${wheel_path}; \
  done

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.8 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f
