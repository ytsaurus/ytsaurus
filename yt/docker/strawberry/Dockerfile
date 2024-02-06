FROM ubuntu:focal

USER root

WORKDIR /tmp

COPY ./chyt-controller /usr/bin/strawberry-controller
RUN ln -s /usr/bin/strawberry-controller /usr/bin/chyt-controller

# CREDITS files with licenses for all binaries.
COPY ./credits/chyt-controller.CREDITS /usr/bin/strawberry-controller.CREDITS

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
  openjdk-11-jdk \
  python3.8 \
  python3-pip \
  libidn11-dev \
  python3.8-distutils

# Default python to be used by python3 jobs, for compatibility with jupyter tutorial.
RUN ln -s /usr/bin/python3.8 /usr/bin/python3 -f
# Force lsb_release to use python it was born to use.
RUN sed -i 's/python3/python3.8/' /usr/bin/lsb_release
