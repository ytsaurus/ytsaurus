## Flow


One release covers every Flow component: the server as docker images, plain and with a Java or a
Python runtime, the Java SDK in Maven Central, the Python SDK in PyPI and the Go SDK as a Go module,
all at the same version.




**Releases:**

{% cut "**0.3.0**" %}

**Release date:** 2026-09-28


**Release page:** [0.3.0](https://github.com/ytsaurus/ytsaurus/releases/tag/flow/0.3.0)


**Docker image:** [ghcr.io/ytsaurus/flow:0.3.0](https://github.com/orgs/ytsaurus/packages/container/flow/1301893105?tag=0.3.0)


**Docker image with JRE 17:** [ghcr.io/ytsaurus/flow-java:0.3.0](https://github.com/orgs/ytsaurus/packages/container/flow-java/1301893505?tag=0.3.0)


**Docker image with Python 3:** [ghcr.io/ytsaurus/flow-python:0.3.0](https://github.com/orgs/ytsaurus/packages/container/flow-python/1301893891?tag=0.3.0)


**Java SDK in Maven Central:** [0.3.0](https://central.sonatype.com/artifact/tech.ytsaurus/flow-core/0.3.0)


**Python SDK in PyPI:** [0.3.0](https://pypi.org/project/ytsaurus-flow-companion/0.3.0/)


**yt_sync_mini in PyPI:** [0.3.0](https://pypi.org/project/ytsaurus-flow-yt-sync-mini/0.3.0/)


**Go SDK module:** [0.3.0](https://pkg.go.dev/go.ytsaurus.tech/yt/go/flow@v0.3.0)


YTsaurus Flow is a framework for streaming cross-DC event processing with exactly-once guarantees within the YTsaurus ecosystem, with APIs for C++, Java and Kotlin, Python, and Go. Its closest external counterparts are Google Cloud Dataflow and Apache Flink.

This is the first Flow release with published artifacts. It includes the server, the SDKs, and the tools, all versioned 0.3.0 and built from a single commit.

#### Artifacts

Docker images; each one is also published with a `-relwithdebinfo` tag that adds debug symbols:

- `ghcr.io/ytsaurus/flow:0.3.0`: the `flow_server` binary, which runs as the runner, the controller, or a worker. `YT_FLOW_BIN=/usr/bin/flow_server`, working directory `/app/pipeline`, entrypoint `flow_server`.
- `ghcr.io/ytsaurus/flow-java:0.3.0`: the same image with an Eclipse Temurin 17 JRE at `/opt/java/openjdk`, entrypoint `java`. Use it for pipelines with Java or Kotlin computations.
- `ghcr.io/ytsaurus/flow-python:0.3.0`: the same image with Python 3 and `ytsaurus-flow-companion` 0.3.0 preinstalled, entrypoint `python3`. Use it for pipelines with Python computations.

Java SDK in Maven Central, group `tech.ytsaurus`, version `0.3.0`:

- `flow-core`: the computation API (`Computation`, `SourceComputation`, row and batch process functions, states, timers).
- `flow-runner`: `FlowApplication`, the entry point that launches the pipeline.
- `flow-server`: the gRPC companion server that runs your computations inside a worker job.
- `flow-spring-boot-starter`: Spring Boot autoconfiguration with `@FlowComputation`.
- `flow-test-utils`: a harness for unit testing computations without a cluster.
- `flow-proto-common`, `flow-proto-companion`: the protocol classes the other modules depend on.

Python packages on PyPI, version `0.3.0`:

- `ytsaurus-flow-companion`: the Python SDK (`Pipeline`, computations, states, timers) and its launcher.
- `ytsaurus-flow-yt-sync-mini`: creates and updates the pipeline object, its system tables, and the tables your pipeline uses. It works independently of the Python SDK.

Go module: `go get go.ytsaurus.tech/yt/go/flow@v0.3.0`.

#### Features

- Exactly-once processing
- Correct results on failures
- Per-key state
- External state in YTsaurus dynamic tables
- Watermarks and timers
- Complex pipeline graphs
- Automatic adaptation of the pipeline to the data flow
- Connectors for YTsaurus queues, static tables, and dynamic tables
- SDKs for Java/Kotlin, Python, and Go
- Deployment as a YTsaurus vanilla operation
- Docker images for job environments
- Pipeline management with the CLI

#### Documentation

- [What is Flow](https://ytsaurus.tech/docs/en/flow/about)
- [Getting started](https://ytsaurus.tech/docs/en/flow/start) and [Quick start](https://ytsaurus.tech/docs/en/flow/quickstart)
- Language guides: [Java](https://ytsaurus.tech/docs/en/flow/java/getting-started), [Python](https://ytsaurus.tech/docs/en/flow/python/getting-started), [Go](https://ytsaurus.tech/docs/en/flow/go/getting-started)
- [Running in a docker environment](https://ytsaurus.tech/docs/en/flow/devops/docker-environment)
- [Flow CLI](https://ytsaurus.tech/docs/en/flow/tools/cli)


{% endcut %}

