# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.23.2
ARG PROTOC_VERSION=3.20.3
ARG CCACHE_VERSION=4.8.2
ARG LLVM_VERSION=20
ARG LLVM_BITCODE_VERSION=18
ARG PYTHON38_VERSION=3.8.20
ARG PYTHON39_VERSION=3.9.25
ARG PYTHON310_VERSION=3.10.19
ARG PYTHON311_VERSION=3.11.14
ARG PYTHON312_VERSION=3.12.12
ARG PYTHON313_VERSION=3.13.12
ARG CONAN_VERSION=2.28.1
ARG PYYAML_VERSION=6.0.1
ARG WHEEL_VERSION=0.41.3
ARG BOTO3_VERSION=1.35.99
ARG JINJA2_VERSION=3.1.5

##########################################################################################

FROM ubuntu:22.04 AS base

ENV DEBIAN_FRONTEND=noninteractive

RUN --mount=type=cache,id=build-env-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=build-env-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && \
    apt-get install -y \
        ca-certificates \
        curl \
        gpg \
        libidn11-dev \
        m4 \
        ninja-build \
        software-properties-common \
        unzip \
        wget \
        antlr3 \
        libaio1 \
        libaio-dev \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libncurses5-dev \
        libncursesw5-dev \
        xz-utils \
        tk-dev \
        liblzma-dev \
        libffi-dev

##########################################################################################

FROM base AS system

ARG LLVM_VERSION
ARG LLVM_BITCODE_VERSION

RUN --mount=type=cache,id=build-env-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=build-env-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    curl -s https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add && \
    echo "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm.list && \
    apt-get update && \
    apt-get install -y \
        "clang-${LLVM_VERSION}" \
        "lld-${LLVM_VERSION}" \
        "libc++-${LLVM_VERSION}-dev" \
        "libc++abi-${LLVM_VERSION}-dev"

RUN update-alternatives --install /usr/bin/clang clang "/usr/bin/clang-${LLVM_VERSION}" 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ "/usr/bin/clang++-${LLVM_VERSION}" 100 && \
    update-alternatives --install /usr/bin/lld lld "/usr/bin/lld-${LLVM_VERSION}" 100 && \
    update-alternatives --install /usr/bin/llvm-link llvm-link "/usr/bin/llvm-link-${LLVM_VERSION}" 100 && \
    update-alternatives --install /usr/bin/opt opt "/usr/bin/opt-${LLVM_VERSION}" 100

RUN --mount=type=cache,id=build-env-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=build-env-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /usr/share/keyrings/kitware-archive-keyring.gpg && \
    echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' > /etc/apt/sources.list.d/kitware.list && \
    apt-get update && \
    apt-get install -y cmake

RUN --mount=type=cache,id=build-env-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=build-env-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    add-apt-repository -y ppa:git-core/ppa && \
    apt-get update && \
    apt-get install -y git

RUN --mount=type=cache,id=build-env-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=build-env-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    echo "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-${LLVM_BITCODE_VERSION} main" > /etc/apt/sources.list.d/llvm-bitcode.list && \
    apt-get update && \
    apt-get install -y \
        "clang-${LLVM_BITCODE_VERSION}" \
        "llvm-${LLVM_BITCODE_VERSION}"

##########################################################################################

FROM base AS go

ARG GO_VERSION

RUN wget "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" && \
    mkdir -p /opt/go && \
    tar -C /opt/go --strip-components=1 -xzf "go${GO_VERSION}.linux-amd64.tar.gz" && \
    rm "go${GO_VERSION}.linux-amd64.tar.gz"

##########################################################################################

FROM base AS python38

ARG PYTHON38_VERSION

RUN wget "https://www.python.org/ftp/python/${PYTHON38_VERSION}/Python-${PYTHON38_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON38_VERSION}.tgz" && \
    cd "Python-${PYTHON38_VERSION}" && \
    ./configure --prefix=/opt/python/3.8 --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON38_VERSION}" "Python-${PYTHON38_VERSION}.tgz"

##########################################################################################

FROM base AS python39

ARG PYTHON39_VERSION

RUN wget "https://www.python.org/ftp/python/${PYTHON39_VERSION}/Python-${PYTHON39_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON39_VERSION}.tgz" && \
    cd "Python-${PYTHON39_VERSION}" && \
    ./configure --prefix=/opt/python/3.9 --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON39_VERSION}" "Python-${PYTHON39_VERSION}.tgz"

##########################################################################################

FROM base AS python310

ARG PYTHON310_VERSION

RUN wget "https://www.python.org/ftp/python/${PYTHON310_VERSION}/Python-${PYTHON310_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON310_VERSION}.tgz" && \
    cd "Python-${PYTHON310_VERSION}" && \
    ./configure --prefix=/opt/python/3.10 --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON310_VERSION}" "Python-${PYTHON310_VERSION}.tgz"

##########################################################################################

FROM base AS python311

ARG PYTHON311_VERSION

RUN wget "https://www.python.org/ftp/python/${PYTHON311_VERSION}/Python-${PYTHON311_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON311_VERSION}.tgz" && \
    cd "Python-${PYTHON311_VERSION}" && \
    ./configure --prefix=/opt/python/3.11 --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON311_VERSION}" "Python-${PYTHON311_VERSION}.tgz"

##########################################################################################

FROM base AS python312

ARG PYTHON312_VERSION

RUN wget "https://www.python.org/ftp/python/${PYTHON312_VERSION}/Python-${PYTHON312_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON312_VERSION}.tgz" && \
    cd "Python-${PYTHON312_VERSION}" && \
    ./configure --prefix=/opt/python/3.12 --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON312_VERSION}" "Python-${PYTHON312_VERSION}.tgz"

##########################################################################################

FROM base AS python313

ARG PYTHON313_VERSION

RUN wget "https://www.python.org/ftp/python/${PYTHON313_VERSION}/Python-${PYTHON313_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON313_VERSION}.tgz" && \
    cd "Python-${PYTHON313_VERSION}" && \
    ./configure --prefix=/opt/python/3.13 --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON313_VERSION}" "Python-${PYTHON313_VERSION}.tgz"

##########################################################################################

FROM python313 AS python313-tools

ARG CONAN_VERSION
ARG PYYAML_VERSION
ARG WHEEL_VERSION
ARG BOTO3_VERSION
ARG JINJA2_VERSION

ENV LD_LIBRARY_PATH=/opt/python/3.13/lib

RUN /opt/python/3.13/bin/python3.13 -m ensurepip && \
    /opt/python/3.13/bin/python3.13 -m pip install --no-cache-dir --ignore-installed \
        conan==${CONAN_VERSION} \
        dacite \
        PyYAML==${PYYAML_VERSION} \
        sphinx \
        virtualenv \
        setuptools \
        twine \
        wheel==${WHEEL_VERSION}

RUN /opt/python/3.13/bin/python3.13 -m venv /root/venv_for_github_actions && \
    /root/venv_for_github_actions/bin/pip install --no-cache-dir \
        boto3==${BOTO3_VERSION} \
        Jinja2==${JINJA2_VERSION} \
        ytsaurus-client \
        ytsaurus-yson

##########################################################################################

FROM base AS protoc

ARG PROTOC_VERSION

RUN wget "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" && \
    mkdir -p /opt/protoc && \
    unzip "protoc-${PROTOC_VERSION}-linux-x86_64.zip" -d /opt/protoc && \
    rm "protoc-${PROTOC_VERSION}-linux-x86_64.zip"

##########################################################################################

FROM base AS ccache

ARG CCACHE_VERSION

RUN wget "https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz" && \
    tar xf "ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz" && \
    cd "ccache-${CCACHE_VERSION}-linux-x86_64" && \
    install -D -m 0755 ccache /opt/ccache/bin/ccache && \
    cd .. && \
    rm -rf "ccache-${CCACHE_VERSION}-linux-x86_64" "ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz"

##########################################################################################

FROM system AS build-env

COPY --from=go /opt/go/ /usr/local/go/
COPY --from=python38 /opt/python/3.8/ /opt/python/3.8/
ENV PATH=/opt/python/3.8/bin:$PATH
COPY --from=python39 /opt/python/3.9/ /opt/python/3.9/
ENV PATH=/opt/python/3.9/bin:$PATH
COPY --from=python310 /opt/python/3.10/ /opt/python/3.10/
ENV PATH=/opt/python/3.10/bin:$PATH
COPY --from=python311 /opt/python/3.11/ /opt/python/3.11/
ENV PATH=/opt/python/3.11/bin:$PATH
COPY --from=python312 /opt/python/3.12/ /opt/python/3.12/
ENV PATH=/opt/python/3.12/bin:$PATH
COPY --from=python313-tools /opt/python/3.13/ /opt/python/3.13/
ENV PATH=/opt/python/3.13/bin:$PATH
COPY --from=python313-tools /root/venv_for_github_actions/ /root/venv_for_github_actions/
COPY --from=protoc /opt/protoc/ /usr/local/
COPY --from=ccache /opt/ccache/bin/ccache /usr/local/bin/ccache

ENV GOPATH=/opt/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH

RUN set -eux; \
    : > /etc/ld.so.conf.d/python-build-env.conf; \
    for version in 3.8 3.9 3.10 3.11 3.12 3.13; do \
        for executable in "/opt/python/${version}/bin/"*; do \
            ln -sf "${executable}" "/usr/bin/$(basename "${executable}")"; \
        done; \
        echo "/opt/python/${version}/lib" >> /etc/ld.so.conf.d/python-build-env.conf; \
    done; \
    ldconfig

WORKDIR /workspace
