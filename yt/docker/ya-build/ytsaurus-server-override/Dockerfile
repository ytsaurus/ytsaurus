ARG BASE_IMAGE="dev"
FROM ytsaurus/ytsaurus:${BASE_IMAGE}

USER root

# Override binaries built from source.
COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./init_queue_agent_state.py /usr/bin/init_queue_agent_state
COPY ./init_operations_archive.py /usr/bin/init_operations_archive
COPY ./chyt-controller /usr/bin/chyt-controller
