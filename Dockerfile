FROM --platform=linux/amd64 ubuntu:22.04
RUN apt-get update && apt-get install -y \
    python3 \
    wget \
    g++ \
    git \
    gdb
