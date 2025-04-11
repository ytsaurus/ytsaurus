ARG ROOT="/ytsaurus"
ARG VIRTUAL_ENV="${ROOT}/virtualenv"

ARG PYTHON_RUNTIME_VERSION="3.11"

ARG YT_CRON_ROOT="${ROOT}/cron"

FROM python:${PYTHON_RUNTIME_VERSION}-slim

ARG ROOT
ENV ROOT=$ROOT

ARG PYTHON_RUNTIME_VERSION
ENV PYTHON_RUNTIME_VERSION=$PYTHON_RUNTIME_VERSION

ARG VIRTUAL_ENV
ENV VIRTUAL_ENV=$VIRTUAL_ENV

ARG YT_CRON_ROOT
ENV YT_CRON_ROOT=$YT_CRON_ROOT

COPY requirements.txt /tmp/requirements.txt

RUN pip install virtualenv \
    && virtualenv ${VIRTUAL_ENV} \
    && . ${VIRTUAL_ENV}/bin/activate \
    && pip install -r /tmp/requirements.txt

COPY ./ ${YT_CRON_ROOT}/

RUN rm ${YT_CRON_ROOT}/requirements.txt

RUN . ${VIRTUAL_ENV}/bin/activate ; \
    SITE_PACKAGES="${VIRTUAL_ENV}/lib/python${PYTHON_RUNTIME_VERSION}/site-packages"; \
    ln -sd "${YT_CRON_ROOT}" "${SITE_PACKAGES}/yt/cron" \
    && for R in $(find ${YT_CRON_ROOT} -mindepth 2 -maxdepth 2 -type f -name requirements.txt); do \
      pip install -r ${R}; \
    done \
    && mkdir "${SITE_PACKAGES}/yt_cron" \
    && for L in $(find ${YT_CRON_ROOT} -mindepth 2 -maxdepth 2 -type d -name lib); do \
      NAME=$(basename $(dirname ${L})); \
      ln -sd ${L} ${SITE_PACKAGES}/yt_cron/${NAME}; \
    done

ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR ${YT_CRON_ROOT}

COPY --from=docker entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
