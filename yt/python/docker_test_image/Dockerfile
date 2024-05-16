FROM ubuntu:20.04 as builder

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y curl gnupg software-properties-common wget build-essential zlib1g-dev zlibc

RUN curl -s https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
RUN echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-14 main" | tee /etc/apt/sources.list.d/llvm.list >/dev/null
RUN echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/kitware.list >/dev/null
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test

RUN apt-get update && apt-get install -y git-all ninja-build libidn11-dev m4 clang-14 lld-14 cmake unzip protobuf-compiler python3-pip


RUN python3 -m pip install PyYAML==6.0 conan==1.57.0 dacite

RUN mkdir -p /workdir/ytsaurus
COPY ./ /workdir/ytsaurus

RUN mkdir -p /workdir/build
WORKDIR /workdir/build
RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=/workdir/ytsaurus/clang.toolchain /workdir/ytsaurus
RUN ninja

RUN python3 -m pip install -e /workdir/ytsaurus/yt/python/packages

RUN mkdir /workdir/python_build
RUN generate_python_proto --source-root /workdir/ytsaurus --output /workdir/python_build

RUN mkdir /source

RUN prepare_python_modules \
    --source-root /workdir/ytsaurus \
    --build-root /workdir/build \
    --output-path /workdir/python_build \
    --prepare-bindings-libraries

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y python3 python3-pip

RUN mkdir /source

COPY --from=builder /workdir/python_build /workdir/python_build
COPY --from=builder /workdir/build/yt/yt/server/all/ytserver-all /workdir/yt/yt/server/all/ytserver-all
COPY --from=builder /workdir/ytsaurus/yt/yt/scripts /workdir/scripts

RUN cp -r /workdir/python_build/yt_* /source

RUN python3 -m pip install -r ./workdir/scripts/pytest_requirements.txt
RUN python3 -m pip install "pytest-jupyter[server]"

ENV YT_BUILD_ROOT /workdir
ENV YT_TESTS_SANDBOX /tmp
ENV YTSERVER_ALL_PATH /workdir/yt/yt/server/all/ytserver-all

