FROM ubuntu:noble AS flow-pipeline

RUN apt-get update -q --error-on=any \
    && apt-get install -y --no-install-recommends python3-minimal tini \
    && rm -rf /var/lib/apt/lists/*

COPY ./noop_pipeline /usr/bin/noop_pipeline
COPY ./yt_sync       /usr/bin/yt_sync
COPY ./flow-start.sh /usr/bin/flow-start.sh
COPY ./yt-sync-start.sh /usr/bin/yt-sync-start.sh

RUN chmod +x /usr/bin/noop_pipeline /usr/bin/yt_sync /usr/bin/flow-start.sh /usr/bin/yt-sync-start.sh

EXPOSE 9001 9002 10001 10002
