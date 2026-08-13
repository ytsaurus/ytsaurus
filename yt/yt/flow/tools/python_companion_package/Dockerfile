# Base image for YT Flow Python companions: an interpreter with the companion SDK and
# ytsaurus-client installed, ready to be named as a job's `docker_image`.
#
# The build context is the ytsaurus checkout root, so that the SDK is built from the same sources
# as the flow_server it will talk to — the companion and the worker speak companion_service.proto,
# and an image built this way matches the binary by construction:
#
#   docker build -f yt/yt/flow/tools/python_companion_package/Dockerfile \
#       -t <registry>/ytflow-python-companion:<tag> .
#
# Push it to a registry your cluster can pull from and name it in the pipeline spec:
#
#   "worker" = {... "docker_image" = "<registry>/ytflow-python-companion:<tag>";};
#
# A pipeline whose computations need no third-party packages can use this image as it is and ship
# its own .py files as job files. Otherwise inherit from it and add what you need.

ARG BASE_IMAGE=docker.io/library/python:3.12-slim
FROM ${BASE_IMAGE}

# Only what the package build reads: its own sources, the python packages it ships, the Flow protos
# it compiles, and the yt_proto protos those import.
COPY yt/yt/flow/tools/python_companion_package /src/yt/yt/flow/tools/python_companion_package
COPY yt/yt/flow/library/python/companion /src/yt/yt/flow/library/python/companion
COPY yt/yt/flow/library/python/runner /src/yt/yt/flow/library/python/runner
COPY yt/yt/flow/library/cpp/companion/proto /src/yt/yt/flow/library/cpp/companion/proto
COPY yt/yt/flow/library/cpp/common/proto /src/yt/yt/flow/library/cpp/common/proto
COPY yt/yt_proto/yt/core/misc/proto /src/yt/yt_proto/yt/core/misc/proto
COPY yt/yt_proto/yt/core/ytree/proto /src/yt/yt_proto/yt/core/ytree/proto
COPY yt/yt_proto/yt/core/yson/proto /src/yt/yt_proto/yt/core/yson/proto

RUN set -eux; \
    pip install --no-cache-dir /src/yt/yt/flow/tools/python_companion_package; \
    rm -rf /src; \
    python -c 'from yt.yt.flow.library.python.companion import Pipeline, server'

# The worker spawns the companion itself through the spec's `entrypoint`, so there is no CMD.
