FROM base_img

ARG ROOT="/ytsaurus"
ARG SOURCE_ROOT="${ROOT}/ytsaurus"
ARG BUILD_ROOT="${ROOT}/build"
ARG PYTHON_ROOT="${ROOT}/python"

ARG PROTOC_VERSION="3.20.1"

ARG BUILD_TARGETS

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      python3 \
      python3-pip \
      python3-pip-whl \
      ninja-build \
      libidn11-dev \
      m4 \
      clang-14 \
      lld-14 \
      cmake \
      unzip \
      llvm-14-dev \
      gcc \
      make \
      python3-dev \
      git \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install PyYAML==6.0 conan==1.57.0 dacite

RUN curl -sL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc.zip -d /usr/local \
    && rm protoc.zip

COPY --link ./ ${SOURCE_ROOT}/

WORKDIR ${ROOT}

RUN mkdir -p ${BUILD_ROOT} ; cd ${BUILD_ROOT} \
    && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=${SOURCE_ROOT}/clang14.toolchain ${SOURCE_ROOT} \
    && ninja ${BUILD_TARGETS}

RUN mkdir ${PYTHON_ROOT} \
    && cd ${SOURCE_ROOT} && pip install -e yt/python/packages \
    && cd "${PYTHON_ROOT}" \
    && generate_python_proto --source-root "${SOURCE_ROOT}" --output "${PYTHON_ROOT}" \
    && prepare_python_modules --source-root "${SOURCE_ROOT}" --build-root "${BUILD_ROOT}" --output-path "${PYTHON_ROOT}" --prepare-bindings-libraries \
    && for PKG in "ytsaurus-client"; do cp ${SOURCE_ROOT}/yt/python/packages/${PKG}/setup.py ./ && python3 setup.py bdist_wheel --universal; done \
    && for PKG in "ytsaurus-yson" "ytsaurus-rpc-driver"; do cp ${SOURCE_ROOT}/yt/python/packages/${PKG}/setup.py ./ && python3 setup.py bdist_wheel --py-limited-api cp34; done
