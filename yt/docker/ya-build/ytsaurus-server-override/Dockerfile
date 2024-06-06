# ARG BASE_REPOSITORY="ghcr.io/ytsaurus/ytsaurus"
# ARG BASE_IMAGE="stable-23.2.0-relwithdebinfo"

ARG BASE_REPOSITORY="ghcr.io/ytsaurus/ytsaurus-nightly"
ARG BASE_IMAGE="dev-23.2-relwithdebinfo-01698e0cb1fbbad0234392ca019e8fb21899f795"

FROM ${BASE_REPOSITORY}:${BASE_IMAGE} AS base

USER root

# Override binaries built from source.
COPY ./ytserver-all /usr/bin/ytserver-all
COPY ./init_queue_agent_state.py /usr/bin/init_queue_agent_state
COPY ./init_operations_archive.py /usr/bin/init_operations_archive

# FROM scratch AS squash
# COPY --from=base / /
