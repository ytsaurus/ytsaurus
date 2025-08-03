## YTsaurus server


All main components are released as a docker image.




**Releases:**

{% cut "**24.2.0**" %}

**Release date:** 2025-03-19


_To install YTsaurus Server 24.2.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) the k8s-operator to version 0.22.0._

---
### Known Issue
- Expanding a cluster with new master cells is temporarily disabled due to a bug. This issue will be resolved in the upcoming 25.1 version.

---

### Scheduler and GPU
New Features & Changes:
- Added support for ACO-based access control in operations.
- Introduced `get_job_trace` method in the jobs API.
- Added an option to fail an operation if started in a non-existent pool.
- Enriched configuration options for offloading operations to pool trees.

Fixes & Optimizations:
- Fixed scheduling issues with new allocations immediately after operation suspension.
- Optimized fair share updates within the control thread.

### Queue Agent
New Features & Changes:
- Queue exports are now considered when trimming replicated and chaos replicated table queues.
- Introduced sum aggregation for lag metrics in consumer partitions.
- Refactor queue exports: add retries and rate limiting.

Fixes & Optimizations:
- Fixed a possible queue controller suspension by adding a timeout to `GetOrderedTabletSafeTrimRowCount` requests.
- Corrected lock options when acquiring a shared lock for the export directory.
- Resolved expiration issues of clients for chaos queues and consumers when the cluster connection changes.

### Proxy
New Features & Changes:
- Supported YAML format for structured data. See details in the RFC: [YAML-format support](https://github.com/ytsaurus/ytsaurus/wiki/%5BRFC%5D-YAML-format-support).
- Introduced create_user_if_not_exists config flag to disable automatic user creation during OAuth authentication. [Issue](https://github.com/ytsaurus/ytsaurus/issues/930).
- Added `cache_key_mode` parameter for controlling credential caching granularity.
- Implemented a new handler for retrieving job trace events.

Fixes & Optimizations:
- The `discover_proxies` handler now returns an error when the proxy type is `http`.
- Fixed a heap buffer overflow in the Arrow parser.
- If insufficient memory is available to handle RPC responses, a retryable `Unavailable` error will now be returned instead of the non-retryable `MemoryPressure` error.
- Optimized the `concatenate` method.

### Kafka proxy
Introduce a new component: the Kafka Proxy. This MVP version enables integration with YTsaurus queues using the Kafka protocol. It currently supports a minimal API for writing to queues and reading with load balancing through consumer groups.

### Dynamic Tables
New Features & Changes:
- Introduced Versioned Remote Copy for tables with hunks.

Fixes & Optimizations:
- Fixed issues with secondary indices in multi-cell clusters (especially large clusters).
- Improved stability and performance of chaos replication.
  
### MapReduce
New Features & Changes:
- Disallowed cluster_connection in remote copy operations.
- Introduced single-chunk teleportation for auto-merge operations.
- Merging of tables with compatible (not necessarily matching) schemas is supported.
- Refactored code in preparation for gang operation introduction.
- Refactored code to enable job-level allocation reuse.
- Improved per-job directory logging in job-proxy mode.

Fixes & Optimizations:
- Optimized job resource acquisition in exec nodes.
- Fixed cases of lost metrics in exec nodes.

### Master Server
New Features & Changes:
- Added an option to fetch input/output table schemas from an external cell using a schema ID.
- Deprecated the list node type; its creation is now forbidden.
- Introduced a new write target allocation strategy using the “two random choices” algorithm.
- Implemented an automatic mechanism to disable replication to data nodes in failing data centers. This can be configured in `//sys/@config/chunk_manager/data_center_failure_detector`.
- Introduced pessimistic validation for resource usage increases when changing the primary medium.
- Forbidden certain erasure codecs in remote copy operations.
- Added node groups attribute for node

Fixes & Optimizations:
- Fixed a race condition between transaction coordinator commit and cell unref for exported objects, [8d6721a](https://github.com/ytsaurus/ytsaurus/commit/8d6721a16bb6a1bc26c9f0d1dc5506f32635e6b6).
- Fixed manual Cypress node merging for Scheduler transactions, [f87a2ad](https://github.com/ytsaurus/ytsaurus/commit/f87a2ad466c2352be9ba7bfee6e7d93796a9eb6a).
- Fixed master crash when setting a YSON dictionary with duplicate keys in a custom attribute, [0cfad80](https://github.com/ytsaurus/ytsaurus/commit/0cfad80f415c23233ca748e345cd9af91169f4c3).
- Fixed row comparison in shallow merge validation to prevent job failures, [3c282d4](https://github.com/ytsaurus/ytsaurus/commit/3c282d4e9f50aa00d861b7a6f1ca388fea18e51d).
- Fixed a crash when reading `@local_scan_flags` attribute, [5b4c954](https://github.com/ytsaurus/ytsaurus/commit/5b4c954c09ac6e1adc55aa6a5d7baff8f894fb61).
- Fixed non-deterministic YSON struct field loading that could cause a “state hashes differ” alert due to inconsistent error messages when multiple required fields were missing, [0553e21](https://github.com/ytsaurus/ytsaurus/commit/0553e2182a0df502592abdd1fcd8ac3c6afd64ad).
- Fixed an issue where nodes held by transactions interfered with cleanup triggered by `expiration_time` attribute.
- Fixed a bug that caused account metrics to break when adding a new account.
- Fixed a bug in attribute-based access control that caused the first entry to always be evaluated.
- Fixed an issue where Hydra followers could become indefinitely stuck after a lost mutation.
- Limited chunk list count per chunk merger session to prevent master overload.
- Fixed an incorrect check for the state of a node during the removal process.
- Improved incremental heartbeat reporting to prevent chunks from getting stuck in the destroyed queue.
- Optimized chunk merger by reducing unnecessary requisition updates.



{% endcut %}


{% cut "**24.1.0**" %}

**Release date:** 2024-11-07


_To install YTsaurus Server 24.1.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases) the k8s-operator to version 0.17.0._

### Scheduler and GPU
Features and changes:
- Support prioritization of pools during strong guarantee adjustment due to insufficient total cluster resources.
- Support prioritization of operations during module assignment stage of the GPU scheduling algorithm.
- Support job resource demand restrictions per pool tree.
- Add custom TTL for jobs in operation archive.
- Add user job trace collection in Trace Event Format.
- Refactor exec node config and Orchid.
- Logically separate jobs and allocations.
- Add configurable input data buffer size in jobs for more efficient interrupts.

Fixes and optimizations:
- Fix exec node heartbeat tracing throughout scheduler and controller agents.
- Optimize general allocation scheduling algorithm and fair share computation.
- Optimize scheduler <-> CA and exec node <-> CA heartbeats processing.

### Queue Agent
Features:
- Treat static queue export the same way as vital consumer during queues trimming, so not exported rows will not be trimmed.
- Add functionality for banning queue agent instances via cypress attribute.
- Take cumulative data weight and timestamp from consumer meta for consumer metrics.


Fixes:
- Fix bug in handling of queues/consumers with invalid attributes (e.g. `auto_trim_config`).
- Fix alerts visibility from `@queue_status` attribute.
- Do not ignore consumers higher than queue size.
- Rename `write_registration_table_mapping` -> `write_replicated_table_mapping` in dynamic config.
- Take shared lock instead of exclusive lock on static export destination directories.

### Proxy
Features:
- Implement queue producer handlers for exactly once pushing in queues (`PushQueueProducer`, `CreateQueueProducerSession`).
- Add `queue_consumer` and `queue_producer` object type handler, so they can be created without explicitly schema specification. Example: `yt create queue_consumer <path>`.
- Support retries of cross cell copying.
- Add float and date types in Arrow format.
- Add memory tracking for `read_table` requests.
- Drop heavy requests if there is no more memory.
- Send `bytes_out` and `bytes_in` metrics during request execution.
- Store `cumulative_data_weight` and `timestamp` in consumer meta.
- Rename `PullConsumer` -> `PullQueueConsumer` and `AdvanceConsumer` -> `AdvanceQueueConsumer`. Old handlers continue to exists for now for backward compatibility reasons.

CHYT:
- Add authorization via X-ClickHouse-Key HTTP-header.
- Add sticky query distribution based on session id/sticky cookie.
- Add a new "/chyt" http handler for chyt queries ("/query" handler is deprecated but still works for backward compatibility).
- Add ability to allocate a separate port for the new http handler to support queries without a custom URL path.
- The clique alias may be specified via "chyt.clique_alias" or "user" parameters (only for new handlers).
- Make HTTP GET requests read-only for compatibility with ClickHouse  (only for new handlers).

Fixes:
- Fill dictionary encoding index type in Arrow format.
- Fix null, void and optional composite columns in Arrow format.
- Fix `yt.memory.heap_usage` metrics.

### Dynamic Tables
Features:
- Secondary Indexes: basic, partial, list, and unique indexes.
- Optimize queries which group and order by same keys.
- Balance tablets using load factor (requires standalone tablet balancer).
- Shared write lock - write to same row from different transactions without blocking.
- Rpc proxy client balancer based on power of two choices algorithm.
- Compression dictionary for Hunks and Hash index.
  
### MapReduce
Features:
- Support input tables from remote clusters in operations.
- Improve control over how data is split into jobs for ML training applications.
- Support read by latest timestamp in MapReduce operations over dynamic tables.
- Disclose less configuration information to a potential attacker.

Fixes:
- Fix teleportation of a single chunk in an unordered pool.
- Fix agent disconnect on removal of an account.
- Fix the inference of intermediate schemas for inputs with column filters.
- Fix controller agent crash on incompatible user statistic paths.

Optimizations:
- Add JobInputCache: in-memory cache on exe nodes, storing data read by multiple jobs running on the same node.

### Master Server

Features:
- Tablet cells Hydra persistence data is now primarily stored at the new location `//sys/hydra_persistence` by default. The duality with the previous location (`//sys/tablet_cells`) will be resolved in the future releases.
- Support inheritance of `@chunk_merger_mode` after copy into directory with set `@chunk_merger_mode`.
- Add backoff rescheduling for nodes merged by chunk merger in case of a transient failure to merge them.
- Add an option to use the two random choices algorithm when allocating write targets.
- Add the add-maintenance command to CLI.
- Support intra-cell cross-shard link nodes.
- Propagate transaction user to transaction replicas for the sake of proper accounting of the cpu time spent committing or aborting them.
- Propagate knowledge of new master cells dynamically to other cluster components and shorten downtime when adding new master cells.

Optimizations:
- Reduce master server memory footprint by reducing the size of table nodes.
- Speed up removal jobs on data nodes.
- Move exec node tracker service away from automaton thread.
- Non-data nodes are now disposed immediately (instead of per location disposal) and independently from data-nodes.
- Offload invoking transaction replication requests from automaton thread.

Fixes:
- Fix nullptr dereferencing in resolution of queue agent and yql agent attributes.
- Respect medium override in IO engine on node restart.
- Fix rebalancing mode in table's chunk tree after merging branched tables.
- Fix sanitizing hostnames in errors for cellar nodes.
- Fix losing trace context for some callbacks and rpc calls.
- Fix persistence of `@last_seen_time` attribute for users.
- Fix handling unknown chunk meta extensions by meta aggregating writer.
- Fix nodes crashing on heartbeat retries when masters are down for a long time.
- Fix table statistics being inconsistent between native and external cells after copying the table mid statistics update.
- Fix logical request weight being accidentally dropped in proxying chunk service.
- Fix a crash that occasionally occurred when exporting a chunk.
- Fix tablet cell lease transactions getting stuck sometimes.
- Native client retries are now more reliable.
- Fix primary cell chunk hosting for multicell.
- Fix crash related to starting incumbency epoch until recovery is complete.
- Restart elections if changelog store for a voting peer is locked in read-only (Hydra fix for tablet nodes).
- Fix crashing on missing schema when importing a chunk.
- Fix an epoch restart-related crash in expiration tracker.
- In master cell directory, alert on an unknown cell role instead of crashing.

### Misc
Features:
- Add ability to redirect stdout to stderr in user jobs (`redirect_stdout_to_stderr` option in operation spec).
- Add dynamic table log writer.

{% endcut %}


{% cut "**23.2.1**" %}

**Release date:** 2024-07-31


### Scheduler and GPU
Features:
  * Disable writing `//sys/scheduler/event_log` by default.
  * Add lightweight running operations.

Fixes:
  * Various optimizations in scheduler
  * Improve total resource usage and limits profiling.
  * Do not account job preparation time in GPU statistics.

### Queue Agent
Fixes:
  * Normalize cluster name in queue consumer registration.

### Proxy
Features:
  * RPC proxy API for Query Tracker.
  * Changed format and added metadata for issued user tokens.
  * Support rotating TLS certificates for HTTP proxies.
  * Compatibility with recent Query Tracker release.

Fixes:
  * Do not retry on Read-Only response error.
  * Fix standalone authentication token revokation.
  * Fix per-user memory tracking (propagate allocation tags to child context).
  * Fix arrow format for optional types.

### Dynamic Tables
Features:
  * Shared write locks.
  * Increased maximum number of key columns to 128.
  * Implemented array join in YT QL.

Fixes:
  * Cap replica lag time for tables that are rarely written to.
  * Fix possible journal record loss during journal session abort.
  * Fix in backup manager.
  * Fix some bugs in chaos dynamic table replication.
  
### MapReduce
Features:
  * Combined per-locaiton throttlers limiting total in+out bandwidth.
  * Options in operation spec to force memory limits on user job containers.
  * Use codegen comparator in SimpleSort & PartitionSort if possible.

Fixes:
  * Better profiling tags for job proxy metrics.
  * Fixes for remote copy with erasure repair.
  * Fix any_to_composite converter when multiple schemas have similarly named composite columns.
  * Fixes for partition_table API method.
  * Fixes in new live preview.
  * Do not fail jobs with supervisor communication failures.
  * Multiple retries added in CRI executor/docker image integration.
  * Cleaned up job memory statistics collection, renamed some statistics.

### Master Server
Features:
  * Parallelize and offload virtual map reads.
  * Emergency flag to disable attribute-based access control.
  * Improved performance of transaction commit/abort.
  * Enable snapshot loading by default.

Fixes:
  * Fixes and optimizations for Sequoia chunk replica management.
  * Fix multiple possible master crashes.
  * Fixes for master update with read-only availability.
  * Fixes for jammed incremental hearbeats and lost replica update on disabled locations.
  * Fix per-account sensors on new account creation.

### Misc
Features:
  * Config exposure via orchid became optional.
  * Support some c-ares options in YT config.
  * Support IP addresses in RPC TLS certificate verification.

Fixes:
   * Fix connection counter leak in http server.
   * Track and limit memory used by queued RPC requests.
   * Better memory tracking for RPC connection buffers.
   * Fix address resolver configuration.


{% endcut %}


{% cut "**23.2.0**" %}

**Release date:** 2024-02-29


### Scheduler

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

### Queue Agent

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

### Proxy
Features:
- Add ability to call `pull_consumer` without specifying `offset`, it will be taken from `consumer` table.
- Add `advance_consumer` handler for queues.
- Early implementation of `arrow` format to read/write static tables.
- Support type conversions for inner fields in complex types.
- Add new per user memory usage monitoring sensors in RPC proxies.
- Use ACO for RPC proxies permission management.
- Introduce TCP Proxies for SPYT.
- Support of OAuth authorization.

Fixes:
- Fix returning requested system columns in `web_json` format.


### Dynamic Tables
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


### MapReduce

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

### Master Server

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

### Misc

Enhancements:
- Add rpc server config dynamization.
- Add support for peer alternative hostname for Bus TLS.
- Properly handle Content-Encoding in monitoring web-server.
- Bring back "host" attribute to errors.
- Add support for --version option in ytserver binaries.
- Add additional metainformation in yson/json server log format (fiberId, traceId, sourceFile).


{% endcut %}

