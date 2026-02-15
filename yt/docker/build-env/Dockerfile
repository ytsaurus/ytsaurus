FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

ARG GO_VERSION=1.23.2
ARG NODE_VERSION=16.20.1
ARG PROTOC_VERSION=3.20.3
ARG CCACHE_VERSION=4.8.2
ARG PYTHON38_VERSION=3.8.20
ARG PYTHON39_VERSION=3.9.25
ARG PYTHON310_VERSION=3.10.19
ARG PYTHON311_VERSION=3.11.14
ARG PYTHON312_VERSION=3.12.12
ARG CONAN_VERSION=2.4.1
ARG PYYAML_VERSION=6.0.1
ARG WHEEL_VERSION=0.41.3
ARG BOTO3_VERSION=1.35.99
ARG JINJA2_VERSION=3.1.5

RUN apt-get update && \
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
        liblzma-dev

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    curl -s https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add && \
    echo 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal-18 main' >> /etc/apt/sources.list.d/llvm.list && \
    apt-get update && \
    apt-get install -y clang-18 lld-18 libc++-18-dev libc++abi-18-dev

RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-18 100 && \
    update-alternatives --install /usr/bin/lld lld /usr/bin/lld-18 100 && \
    update-alternatives --install /usr/bin/llvm-link llvm-link /usr/bin/llvm-link-18 100 && \
    update-alternatives --install /usr/bin/opt opt /usr/bin/opt-18 100

RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /usr/share/keyrings/kitware-archive-keyring.gpg && \
    echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ focal main' > /etc/apt/sources.list.d/kitware.list && \
    apt-get update && \
    apt-get install -y cmake

RUN add-apt-repository -y ppa:git-core/ppa && \
    apt-get update && \
    apt-get install -y git

ENV GOPATH=/opt/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
RUN wget "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" && \
    tar -C /usr/local -xzf "go${GO_VERSION}.linux-amd64.tar.gz" && \
    rm "go${GO_VERSION}.linux-amd64.tar.gz"

RUN wget "https://www.python.org/ftp/python/${PYTHON38_VERSION}/Python-${PYTHON38_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON38_VERSION}.tgz" && \
    cd "Python-${PYTHON38_VERSION}" && \
    ./configure --prefix=/usr --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON38_VERSION}" "Python-${PYTHON38_VERSION}.tgz"

RUN wget "https://www.python.org/ftp/python/${PYTHON39_VERSION}/Python-${PYTHON39_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON39_VERSION}.tgz" && \
    cd "Python-${PYTHON39_VERSION}" && \
    ./configure --prefix=/usr --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON39_VERSION}" "Python-${PYTHON39_VERSION}.tgz"

RUN wget "https://www.python.org/ftp/python/${PYTHON310_VERSION}/Python-${PYTHON310_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON310_VERSION}.tgz" && \
    cd "Python-${PYTHON310_VERSION}" && \
    ./configure --prefix=/usr --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON310_VERSION}" "Python-${PYTHON310_VERSION}.tgz"

RUN wget "https://www.python.org/ftp/python/${PYTHON311_VERSION}/Python-${PYTHON311_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON311_VERSION}.tgz" && \
    cd "Python-${PYTHON311_VERSION}" && \
    ./configure --prefix=/usr --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON311_VERSION}" "Python-${PYTHON311_VERSION}.tgz"

RUN wget "https://www.python.org/ftp/python/${PYTHON312_VERSION}/Python-${PYTHON312_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON312_VERSION}.tgz" && \
    cd "Python-${PYTHON312_VERSION}" && \
    ./configure --prefix=/usr --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --enable-shared && \
    make -j "$(nproc)" && \
    make altinstall && \
    cd .. && \
    rm -rf "Python-${PYTHON312_VERSION}" "Python-${PYTHON312_VERSION}.tgz"

RUN python3.11 -m ensurepip && \
    python3.11 -m pip install --no-cache-dir --ignore-installed \
        conan==${CONAN_VERSION} \
        dacite \
        PyYAML==${PYYAML_VERSION} \
        sphinx \
        virtualenv \
        setuptools \
        twine \
        wheel==${WHEEL_VERSION}

RUN python3.11 -m venv /root/venv_for_github_actions && \
    /root/venv_for_github_actions/bin/pip install --no-cache-dir \
        boto3==${BOTO3_VERSION} \
        Jinja2==${JINJA2_VERSION} \
        ytsaurus-client \
        ytsaurus-yson

RUN wget "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" && \
    unzip "protoc-${PROTOC_VERSION}-linux-x86_64.zip" -d /usr/local && \
    rm "protoc-${PROTOC_VERSION}-linux-x86_64.zip"

RUN wget "https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz" && \
    tar xf "ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz" && \
    cd "ccache-${CCACHE_VERSION}-linux-x86_64" && \
    make install && \
    cd .. && \
    rm -rf "ccache-${CCACHE_VERSION}-linux-x86_64" "ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz"

RUN rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
