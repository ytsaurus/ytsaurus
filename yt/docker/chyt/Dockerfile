FROM ubuntu:focal

USER root

WORKDIR /tmp

# CHYT binaries.
COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse

# Credits.
COPY ./credits/ytserver-clickhouse.CREDITS /usr/bin/ytserver-clickhouse.CREDITS

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  curl \
  less \
  python3.8 \
  python3-pip

RUN python3 -m pip install ytsaurus-client

RUN ln -s /usr/local/bin/yt /usr/bin/yt -f

# Setup script.
COPY ./setup_cluster_for_chyt.sh /setup_cluster_for_chyt.sh
RUN chmod 755 /setup_cluster_for_chyt.sh
