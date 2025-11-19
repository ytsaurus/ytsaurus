FROM ubuntu:jammy

RUN apt-get update && \
    apt-get install -y \
    curl \
    ca-certificates \
    netcat \
    && rm -rf /var/lib/apt/lists/*

COPY generate_tutorial /usr/local/bin/app
COPY scripts ./scripts
COPY data ./data

RUN chmod +x /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
