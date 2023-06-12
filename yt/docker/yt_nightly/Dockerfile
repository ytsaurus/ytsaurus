FROM cr.yandex/crpl84hkh3k16ihscv0j/yt_nightly_base:latest

WORKDIR /tmp

USER root

ARG DEBIAN_FRONTEND=noninteractive

COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./init_operation_archive.py /usr/bin/init_operation_archive

COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse
COPY ./ytserver-log-tailer /usr/bin/ytserver-log-tailer
COPY ./chyt-controller /usr/bin/chyt-controller

COPY ./ytsaurus_python /tmp/ytsaurus_python
RUN for package in native_driver; \
  do \
    dist_dir=/tmp/ytsaurus_python/ytsaurus_${package}_dist \
    wheel_path=${dist_dir}/$(ls ${dist_dir} | grep "^ytsaurus_$package.*whl$") \
    echo "DIST_DIR: ${dist_dir}" \
    echo "WHEEL_PATH: ${wheel_path}" \
    pip3 install ${wheel_path}; \
  done

RUN ln /usr/bin/ytserver-all /usr/bin/ytserver-master && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-clock && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-discovery && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-node && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-job-proxy && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-exec && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-proxy && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-http-proxy && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-tools && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-scheduler && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-controller-agent && \
    ln /usr/bin/ytserver-all /usr/bin/ytserver-query-tracker

RUN cp /usr/local/bin/yt /usr/bin/yt
