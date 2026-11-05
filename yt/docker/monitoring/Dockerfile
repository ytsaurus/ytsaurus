FROM python:3.12-slim

ENV YT_MONITORING_ROOT=/ytsaurus/monitoring

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl jq && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${YT_MONITORING_ROOT}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=generated grafana ./generated/grafana
COPY --chmod=755 ./scripts ./scripts

CMD ["/bin/bash"]
