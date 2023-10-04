FROM ubuntu:focal

USER root

WORKDIR /tmp

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all

RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-queue-agent

COPY ./init_queue_agent_state.py /usr/bin/init_queue_agent_state

# CREDITS files with licenses for all binaries.
COPY ./credits/ytserver-all.CREDITS /usr/bin/ytserver-all.CREDITS

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  curl \
  less \
  gdb \
  lsof \
  strace \
  telnet \
  tini \
  zstd \
  dnsutils \
  iputils-ping \
  lsb-release \
  python3.7 \
  python3-pip \
  libidn11-dev \
  python3.7-distutils

COPY ./ytsaurus_python /tmp/ytsaurus_python
RUN for package in client yson local native_driver; \
  do \
    dist_dir="/tmp/ytsaurus_python/ytsaurus_${package}_dist"; \
    wheel_path="${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$")"; \
    echo "DIST_DIR: ${dist_dir}"; \
    echo "WHEEL_PATH: ${wheel_path}"; \
    python3.7 -m pip install ${wheel_path}; \
  done

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.7 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f
