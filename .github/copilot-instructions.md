# GitHub Copilot Instructions for YTsaurus

## Project Overview

YTsaurus is a scalable and fault-tolerant open-source big data platform.

## Branch Naming Convention

Development branch: `main`

YTsaurus release branches: `stable/<major>.<minor>`
CHYT release branches: `chyt/<major>.<minor>`

**Format for PR branches:** `members/<user-name>/<short-description>`

**Rules:**
- Lowercase only
- Hyphens for word separation
- Description max 30 characters

This format is **required** (enforced by branch protection rule) in upstream repo and recommended in forks.

**Examples:**
```
members/khlebnikov/copilot-instructions
```

## Additional Resources

- Project README: `README.md`
- Project ROADMAP: `ROADMAP.md`
- Building and testing instructions: `BUILD.md`
- Documentation README: `yt/docs/README.md`
- C++ style guideline: `yt/styleguide/cpp.md`
- Python style guideline: `yt/styleguide/python.md`

## Repository Structure

- `util/`           - Common utility functions
- `library/`        - Common libraries and utilities
- `contrib/`        - Third-party (C, C++, Python) libraries
- `vendor/`         - Third-party Go libraries
- `tools/`          - Development and build tools
- `yql/`            - YQL (Yandex Query Language)
- `yt/`             - YTsaurus code (see detailed structure below)

## YTsaurus Code Structure

- `yt/admin/`         - Administrative tools and utilities
- `yt/benchmarks/`    - Performance benchmarks
- `yt/chyt/`          - CHYT (ClickHouse over YT) integration
- `yt/chyt/server/`   - CHYT server (C++)
- `yt/chyt/controller/` - Strawberry (CHYT, SPYT, etc) controller (Go)
- `yt/chyt/tests/`    - CHYT tests
- `yt/cpp/`           - C++ client library and core components
- `yt/cron/`          - Scheduled job management
- `yt/docker/`        - Docker configurations
- `yt/docs/`          - YTsaurus Documentation (see Documentation README)
- `yt/examples/`      - Code examples for users
- `yt/go/`            - Go SDK and client
- `yt/java/`          - Java client library
- `yt/microservices/` - Microservices components
- `yt/odin/`          - Monitoring and health check service
- `yt/python/`        - Python SDK and client
- `yt/styleguide/`    - Code style guide
- `yt/yql/`           - YQL integration
- `yt/yt/`            - YTsaurus components (C++ code)
- `yt/yt/core/`       - Core functionality (memory, concurrency, etc.)
- `yt/yt/library/`    - Internal libraries
- `yt/yt/client/`     - Client implementations
- `yt/yt/python/`     - Python client driver modules
- `yt/yt_proto/`      - Protobuf definitions
- `yt/yt/ytlib/`      - YT library components
- `yt/yt/server/`     - Server components
- `yt/yt/server/lib/`                - Shared server libraries
- `yt/yt/server/all/`                - Universal multi-call binary
- `yt/yt/server/master/`             - Master server (metadata)
- `yt/yt/server/master_cache/`       - Master cache server (metadata)
- `yt/yt/server/timestamp_provider/` - Timestamp provider server
- `yt/yt/server/node/`               - Worker node servers
- `yt/yt/server/node/cellar_node/`   - Cellar node
- `yt/yt/server/node/chaos_node/`    - Chaos node
- `yt/yt/server/node/cluster_node/`  - Common worker node server
- `yt/yt/server/node/data_node/`     - Data node (data chunks)
- `yt/yt/server/node/exec_node/`     - Exec node (compute)
- `yt/yt/server/node/job_agent/`     - Job agent code
- `yt/yt/server/node/query_agent/`   - Query agent code
- `yt/yt/server/node/tablet_node/`   - Tablet node (dynamic tables)
- `yt/yt/server/scheduler/`          - Job scheduler
- `yt/yt/server/controller_agent/`   - Job controllers (map-reduce)
- `yt/yt/server/exec/`               - Job execution trampoline
- `yt/yt/server/job_proxy/`          - Job execution sidecar
- `yt/yt/server/query_tracker/`      - Query tracker server (CHYT, YQL)
- `yt/yt/server/queue_agent/`        - Queue agent server
- `yt/yt/server/http_proxy/`         - HTTP API proxy server
- `yt/yt/server/rpc_proxy/`          - RPC API proxy server
- `yt/yt/server/tcp_proxy/`          - TCP proxy server
- `yt/yt/tests/`                     - YTsaurus tests
- `yt/yt/tests/integration/`         - YTsaurus integration tests (Python)
