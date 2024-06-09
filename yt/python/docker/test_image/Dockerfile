FROM odin/lib-builder as builder
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y python3 python3-pip

RUN mkdir /source

COPY --from=builder /ytsaurus/python /workdir/python_build
COPY --from=builder /ytsaurus/build/yt/yt/server/all/ytserver-all /workdir/yt/yt/server/all/ytserver-all
COPY --from=builder /ytsaurus/ytsaurus/yt/yt/scripts /workdir/scripts

RUN cp -r /workdir/python_build/yt_* /source

RUN python3 -m pip install -r ./workdir/scripts/pytest_requirements.txt
RUN python3 -m pip install "pytest-jupyter[server]"

ENV YT_BUILD_ROOT /workdir
ENV YT_TESTS_SANDBOX /tmp
ENV YTSERVER_ALL_PATH /workdir/yt/yt/server/all/ytserver-all

