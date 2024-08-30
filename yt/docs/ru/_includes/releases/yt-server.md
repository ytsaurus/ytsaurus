## {{product-name}} server

All main components are released as a docker image.

{% note warning "Important" %}

The k8s operator version must be at least 0.6.0.

{% endnote %}

**Current release:** {{yt-server-version}} (`ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo`)

**All releases:**

{% cut "**23.2.1**" %}

#### Scheduler and GPU

Features:

- Disable writing `//sys/scheduler/event_log` by default.
- Add lightweight running operations.

Fixes:

- Various optimizations in scheduler
- Improve total resource usage and limits profiling.
- Do not account job preparation time in GPU statistics.

#### Queue Agent

Fixes:

- Normalize cluster name in queue consumer registration.

#### Proxy

Features:

- RPC proxy API for Query Tracker.
- Changed format and added metadata for issued user tokens.
- Support rotating TLS certificates for HTTP proxies.
- Compatibility with recent Query Tracker release.

Fixes:

- Do not retry on Read-Only response error.
- Fix standalone authentication token revokation.
- Fix per-user memory tracking (propagate allocation tags to child context).
- Fix arrow format for optional types.

#### Dynamic Tables

Features:

- Shared write locks.
- Increased maximum number of key columns to 128.
- Implemented array join in {{product-name}} QL.

Fixes:

- Cap replica lag time for tables that are rarely written to.
- Fix possible journal record loss during journal session abort.
- Fix in backup manager.
- Fix some bugs in chaos dynamic table replication.

#### MapReduce

Features:

- Combined per-locaiton throttlers limiting total in+out bandwidth.
- Options in operation spec to force memory limits on user job containers.
- Use codegen comparator in SimpleSort & PartitionSort if possible.

Fixes:

- Better profiling tags for job proxy metrics.
- Fixes for remote copy with erasure repair.
- Fix `any_to_composite` converter when multiple schemas have similarly named composite columns.
- Fixes for `partition_table` API method.
- Fixes in new live preview.
- Do not fail jobs with supervisor communication failures.
- Multiple retries added in CRI executor/docker image integration.
- Cleaned up job memory statistics collection, renamed some statistics.

#### Master Server

Features:

- Parallelize and offload virtual map reads.
- Emergency flag to disable attribute-based access control.
- Improved performance of transaction commit/abort.
- Enable snapshot loading by default.

Fixes:

- Fixes and optimizations for Sequoia chunk replica management.
- Fix multiple possible master crashes.
- Fixes for master update with read-only availability.
- Fixes for jammed incremental hearbeats and lost replica update on disabled locations.
- Fix per-account sensors on new account creation.

#### Misc

Features:

- Config exposure via orchid became optional.
- Support some c-ares options in {{product-name}} config.
- Support IP addresses in RPC TLS certificate verification.

Fixes:

- Fix connection counter leak in http server.
- Track and limit memory used by queued RPC requests.
- Better memory tracking for RPC connection buffers.
- Fix address resolver configuration.

{% endcut %}

{% cut "**23.2.0**" %}

`ytsaurus/ytsaurus:stable-23.2.0-relwithdebinfo`

#### Scheduler

Many internal changes driven by developing new scheduling mechanics that separate jobs from resource allocations at exec nodes. These changes include modification of the protocol of interaction between schedulers, controller agents and exec nodes, and adding tons of new logic for introducing allocations in exec nodes, controller agents and schedulers.

List of significant changes and fixes:
  - Optimize performance of scheduler's Control and NodeShard threads.
  - Optimize performance of the core scheduling algorithm by considering only a subset of operations in most node heartbeats.
  - Optimize operation launch time overhead by not creating debug transaction if neither stderr or core table have been specified.
  - Add priority scheduling for pools with resource guarantees.
  - Consider disk usage in job preemption algorithm.
  - Add operation module assignment preemption in GPU segments scheduling algorithm.
  - Add fixes for GPU scheduling algorithms.
  - Add node heartbeat throttling by scheduling complexity.
  - Add concurrent schedule job exec duration throttling.
  - Reuse job monitoring descriptors within a single operation.
  - Support monitoring descriptors in map operations.
  - Support filtering jobs with monitoring descriptors in `list_jobs` command.
  - Fix displaying jobs which disappear due to a node failure as running and "stale" in UI.
  - Improve ephemeral subpools configuration.
  - Hide user tokens in scheduler and job proxy logs.
  - Support configurable max capacity for pipes between job proxy and user job.

#### Queue Agent

Aside small improvements, the most significant features include the ability to configure periodic exports of partitioned data from queues into  static tables and the support for using replicated and chaos dynamic tables as queues and consumers.

Features:
- Support chaos replicated tables as queues and consumers.
- Support snapshot exports from queues into static tables.
- Support queues and consumers that are symbolic links for other queues and consumers.
- Support trimming of rows in queues by lifetime duration.
- Support for registering and unregistering of consumer to queue from different cluster.

Fixes:
- Trim queues by its `object_id`, not by `path`.
- Fix metrics of read rows data weight via consumer.
- Fix handling frozen tablets in queue.

#### Proxy
Features:
- Add ability to call `pull_consumer` without specifying `offset`, it will be taken from `consumer` table.
- Add `advance_consumer` handler for queues.
- Early implementation of `arrow` format to read/write static tables.
- Support type conversions for inner fields in complex types.
- Add new per user memory usage monitoring sensors in RPC proxies.
- Use ACO for RPC proxies permission management.
- Introduce TCP Proxies for SPYT.
- Support of OAuth authorisation.

Fixes:
- Fix returning requested system columns in `web_json` format.


#### Dynamic Tables
Features:
- DynTables Query language improvments:
    - New range inferrer.
    - Add various SQL operators (<>, string length, ||, yson_length, argmin, argmax, coalesce).
- Add backups for tables with hunks.
- New fair share threadpool for select operator and network.
- Add partial key filtering for range selects.
- Add overload controller.
- Distribute load among rpc proxies more evenly.
- Add per-table size metrics.
- Store heavy chunk meta in blocks.


#### MapReduce

Features:
- RemoteСopy now supports cypress file objects, in addition to tables.
- Add support for per job experiments.
- Early implementation of CRI (container runtime interface) job environment & support for external docker images.
- New live preview for MapReduce output tables.
- Add support for arrow as an input format for MapReduce.
- Support GPU resource in exec-nodes and schedulers.

Enhancements:
- Improve memory tracking in data nodes (master jobs, blob write sessions, p2p tracking).
- Rework memory acccounting in controller agents.

#### Master Server

Noticeable/Potentially Breaking Changes:
  - Read requests are now processed in a multithreaded manner by default.
  - Read-only mode now persists between restarts. `yt-admin master-exit-read-only` command should be used to leave it.
  - `list_node` type has been deprecated. Users are advised to use `map_node`s or `document`s instead.
  - `ChunkService::ExecuteBatch` RPC call has been deprecated and split into individual calls. Batching chunk service has been superseded by proxying chunk service.
  - New transaction object types: `system_transaction`, `nested_system_transaction`. Support for transaction actions in regular Cypress transactions is now deprecated.
  - Version 2 of the Hydra library is now enabled by default. Version 1 is officially deprecated.

Features:
  - It is now possible to update master-servers with no read downtime via leaving non-voting peers to serve read requests while the main quorum is under maintenance.
  - A data node can now be marked as pending restart, hinting the replicator to ignore its absence for a set amount of time to avoid needless replication bursts.
  - The `add_maintenance` command now supports HTTP- and RPC-proxies.
  - Attribute-based access control: a user may now be annotated with a set of tags, while an access-control entry (ACE) may be annotated with a tag filter.

Optimizations & Fixes:
  - Response keeper is now persistent. No warm-up period is required before a peer may begin leading.
  - Chunk metadata now include schemas. This opens up a way to a number of significant optimizations.
  - Data node heartbeat size has been reduced.
  - Chunks and chunk lists are now loaded from snapshot in parallel.
  - Fixed excessive memory consumption in multicell configurations.
  - Accounting code has been improved to properly handle unlimited quotas and avoid negative master memory usage.

Additionally, advancements have been made in the Sequoia project dedicated to scaling master server by offloading certain parts of its state to dynamic tables. (This is far from being production-ready yet.)

#### Misc

Enhancements:
- Add rpc server config dynamization.
- Add support for peer alternative hostname for Bus TLS.
- Properly handle Content-Encoding in monitoring web-server.
- Bring back "host" attribute to errors.
- Add support for --version option in ytserver binaries.
- Add additional metainformation in yson/json server log format (fiberId, traceId, sourceFile).

{% endcut %}

{% cut "**23.1.0**" %}

`ytsaurus/ytsaurus:stable-23.1.0-relwithdebinfo`

{% endcut %}