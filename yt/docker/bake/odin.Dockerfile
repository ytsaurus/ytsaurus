ARG ROOT="/ytsaurus"
ARG SOURCE_ROOT="${ROOT}/ytsaurus"
ARG ODIN_ROOT="${SOURCE_ROOT}/yt/odin"
ARG PYTHON_ROOT="${ROOT}/python"
ARG VIRTUAL_ENV="${ROOT}/virtualenv"

ARG ODIN_RUNTIME_ROOT="/var/odin"
ARG ODIN_CHECKS_DIR="${ODIN_RUNTIME_ROOT}/checks"
ARG ODIN_CHECKS_DATA_DIR="${ODIN_RUNTIME_ROOT}/checks-data"
ARG PYTHON_RUNTIME_VERSION="3.10"

# Prepare python modules for Odin.

FROM built_yson as odin_prepared_python_libs

ARG SOURCE_ROOT

ARG PYTHON_ROOT
ENV PYTHON_ROOT $PYTHON_ROOT

ARG ODIN_ROOT
ENV ODIN_ROOT $ODIN_ROOT

WORKDIR $PYTHON_ROOT

RUN $ODIN_ROOT/packages/prepare_python_modules.py --source-root "$SOURCE_ROOT" --output-path "$PYTHON_ROOT" \
  && cp "$ODIN_ROOT/packages/setup.py" ./ && python3 setup.py bdist_wheel --universal


# Install Odin Python libraries into the virtual env.

FROM python:${PYTHON_RUNTIME_VERSION}-slim AS odin_libs

ARG SOURCE_ROOT

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

ARG ODIN_CHECKS_DIR
ARG ODIN_CHECKS_DATA_DIR="${ODIN_RUNTIME_ROOT}/checks-data"

WORKDIR $PYTHON_ROOT

COPY --from=odin_prepared_python_libs ${PYTHON_ROOT}/dist/* ${PYTHON_ROOT}/dist/
COPY --from=odin_prepared_python_libs ${ODIN_ROOT} ${ODIN_ROOT}
COPY --from=odin_prepared_python_libs ${ODIN_ROOT}/../yt/scripts ${ODIN_ROOT}/../yt/scripts

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

# Copy virtual env and odin checks from the previous ones

FROM python:${PYTHON_RUNTIME_VERSION}-slim AS odin

ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV

ARG ODIN_RUNTIME_ROOT
ENV ODIN_RUNTIME_ROOT $ODIN_RUNTIME_ROOT

COPY --from=built_odin ${VIRTUAL_ENV}/ ${VIRTUAL_ENV}/
COPY --from=built_odin ${ODIN_RUNTIME_ROOT}/ ${ODIN_RUNTIME_ROOT}/

RUN mkdir -p ${ODIN_RUNTIME_ROOT}/log \
    && ln -sf /dev/stdout ${ODIN_RUNTIME_ROOT}/log/odin.log \
    && ln -sf /dev/stdout ${ODIN_RUNTIME_ROOT}/log/webservice.log

ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR ${ODIN_RUNTIME_ROOT}
