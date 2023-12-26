ARG ROOT="/ytsaurus"
ARG SOURCE_ROOT="${ROOT}/ytsaurus"
ARG ODIN_ROOT="${SOURCE_ROOT}/yt/odin"
ARG BUILD_ROOT="${ROOT}/build"
ARG PYTHON_ROOT="${ROOT}/python"
ARG VIRTUAL_ENV="${ROOT}/virtualenv"

ARG PROTOC_VERSION="3.20.1"

ARG ODIN_RUNTIME_ROOT="/var/odin"
ARG ODIN_CHECKS_DIR="${ODIN_RUNTIME_ROOT}/checks"
ARG ODIN_CHECKS_DATA_DIR="${ODIN_RUNTIME_ROOT}/checks-data"

FROM ubuntu:jammy as lib-builder

ARG ROOT
ENV ROOT $ROOT

ARG SOURCE_ROOT
ENV SOURCE_ROOT $SOURCE_ROOT

ARG ODIN_ROOT
ENV ODIN_ROOT $ODIN_ROOT

ARG BUILD_ROOT
ENV BUILD_ROOT $BUILD_ROOT

ARG PYTHON_ROOT
ENV PYTHON_ROOT $PYTHON_ROOT

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

ARG PROTOC_VERSION
ENV PROTOC_VERSION $PROTOC_VERSION

RUN curl -sL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
  && unzip protoc.zip -d /usr/local \
  && rm protoc.zip

COPY --link ./ ${SOURCE_ROOT}/

WORKDIR ${ROOT}

RUN mkdir -p ${BUILD_ROOT} ; cd ${BUILD_ROOT} \
  && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=${SOURCE_ROOT}/clang.toolchain ${SOURCE_ROOT} \
  && ninja yson_lib driver_lib driver_rpc_lib

RUN mkdir ${PYTHON_ROOT} \
  && cd ${SOURCE_ROOT} && pip install -e yt/python/packages \
  && cd "$PYTHON_ROOT" \
  && generate_python_proto --source-root "$SOURCE_ROOT" --output "$PYTHON_ROOT" \
  && prepare_python_modules --source-root "$SOURCE_ROOT" --build-root "$BUILD_ROOT" --output-path "$PYTHON_ROOT" --prepare-bindings-libraries \
  && for PKG in "ytsaurus-client"; do cp ${SOURCE_ROOT}/yt/python/packages/${PKG}/setup.py ./ && python3 setup.py bdist_wheel --universal; done \
  && for PKG in "ytsaurus-yson" "ytsaurus-rpc-driver"; do cp ${SOURCE_ROOT}/yt/python/packages/${PKG}/setup.py ./ && python3 setup.py bdist_wheel --py-limited-api cp34; done \
  && ${ODIN_ROOT}/packages/prepare_python_modules.py --source-root "$SOURCE_ROOT" --output-path "$PYTHON_ROOT" \
  && cp "${ODIN_ROOT}/packages/setup.py" ./ && python3 setup.py bdist_wheel --universal

FROM python:3.10-slim as odin-installer

ARG PYTHON_ROOT
ENV PYTHON_ROOT $PYTHON_ROOT

ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV

ARG ODIN_ROOT
ENV ODIN_ROOT $ODIN_ROOT

ARG ODIN_RUNTIME_ROOT
ENV ODIN_RUNTIME_ROOT $ODIN_RUNTIME_ROOT

ARG ODIN_CHECKS_DIR
ENV ODIN_CHECKS_DIR $ODIN_CHECKS_DIR

ARG ODIN_CHECKS_DATA_DIR
ENV ODIN_CHECKS_DATA_DIR $ODIN_CHECKS_DATA_DIR

COPY --from=lib-builder ${PYTHON_ROOT}/dist/* ${PYTHON_ROOT}/dist/
COPY --from=lib-builder ${ODIN_ROOT} ${ODIN_ROOT}
COPY --from=lib-builder ${ODIN_ROOT}/../yt/scripts ${ODIN_ROOT}/../yt/scripts

RUN pip install virtualenv \
    && virtualenv ${VIRTUAL_ENV}

RUN . ${VIRTUAL_ENV}/bin/activate \
    && pip install -r ${ODIN_ROOT}/requirements.txt \
    && pip install $PYTHON_ROOT/dist/*

RUN mkdir -p ${ODIN_CHECKS_DIR} \
    && mkdir -p ${ODIN_CHECKS_DATA_DIR} \
    && for CHECK in $(find ${ODIN_ROOT}/checks/bin -mindepth 1 -maxdepth 1 -type d -not -name "ya.make"); do \
      CHECK=$(basename ${CHECK}); \
      mkdir -p ${ODIN_CHECKS_DIR}/${CHECK}; \
      ln -sf ${VIRTUAL_ENV}/bin/${CHECK} ${ODIN_CHECKS_DIR}/${CHECK}/${CHECK}; \
    done

FROM python:3.10-slim as odin

ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV

ARG ODIN_RUNTIME_ROOT
ENV ODIN_RUNTIME_ROOT $ODIN_RUNTIME_ROOT

COPY --from=odin-installer ${VIRTUAL_ENV}/ ${VIRTUAL_ENV}/
COPY --from=odin-installer ${ODIN_RUNTIME_ROOT}/ ${ODIN_RUNTIME_ROOT}/

RUN mkdir -p /logs \
    && ln -sf /dev/stdout /logs/odin.log \
    && ln -sf /dev/stdout /logs/webservice.log

ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR ${ODIN_RUNTIME_ROOT}
