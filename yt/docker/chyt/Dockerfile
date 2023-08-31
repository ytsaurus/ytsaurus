FROM ubuntu:focal

USER root

WORKDIR /tmp

# CHYT binaries.
COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse

# Credits.
COPY ./credits/ytserver-clickhouse.CREDITS /usr/bin/ytserver-clickhouse.CREDITS

# Setup script.
COPY ./setup_cluster_for_chyt.sh /setup_cluster_for_chyt.sh
RUN chmod 755 /setup_cluster_for_chyt.sh
