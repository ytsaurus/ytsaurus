FROM ytsaurus/ytsaurus

USER root

RUN rm /usr/bin/ytserver-all /usr/bin/ytserver-master /usr/bin/ytserver-clock /usr/bin/ytserver-discovery /usr/bin/ytserver-node /usr/bin/ytserver-job-proxy /usr/bin/ytserver-exec /usr/bin/ytserver-proxy /usr/bin/ytserver-http-proxy /usr/bin/ytserver-tools /usr/bin/ytserver-scheduler /usr/bin/ytserver-controller-agent /usr/bin/ytserver-query-tracker

COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse
COPY ./ytserver-log-tailer /usr/bin/ytserver-log-tailer
COPY ./chyt-controller /usr/bin/chyt-controller

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
