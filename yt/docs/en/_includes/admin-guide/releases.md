# Releases

## {{product-name}} server

All main components are releases as a docker image.

**Current release:** {{yt-server-version}} (`ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo`)

**All releases:**

{% cut "**23.2.0**" %}

`ytsaurus/ytsaurus:stable-23.2.0-relwithdebinfo`

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
- Support of OAuth authorisation.

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

{% cut "**23.1.0**" %}

`ytsaurus/ytsaurus:stable-23.1.0-relwithdebinfo`

{% endcut %}

## Query Tracker

Publishes as docker images.

**Current release:** 0.0.6 (`ytsaurus/query-tracker:0.0.6-relwithdebinfo`)

**All releases:**

{% cut "**0.0.6**" %}

- Fixed authorization in complex cluster-free YQL queries
- Fixed a bug that caused queries with large queries to never complete
- Fixed a bag caused possibility of SQL injection in query tracker
- Reduced the size of query_tracker docker images

**Related issues:**
- [Problems with QT ACOs](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/176)

In case of an error when starting query
```
Access control object "nobody" does not exist
```
You need to run commands by admin
```
yt create access_control_object_namespace --attr '{name=queries}'
yt create access_control_object --attr '{namespace=queries;name=nobody}'
```

{% endcut %}

{% cut "**0.0.5**" %}

- Added access control to queries
- Added support for the in‑memory DQ engine that accelerates small YQL queries
- Added execution mode setting to query tracker. This allows to run queries in validate and explain modes
- Fixed a bug that caused queries to be lost in query_tracker
- Fixed a bug related to yson parsing in YQL queries
- Reduced the load on the state dyntables by QT
- Improved authentication in YQL queries.
- Added authentication in SPYT queries
- Added reuse of spyt sessions. Speeds up the sequential launch of SPYT queries from a single user
- Changed the build type of QT images from cmake to ya make

NB:
- Compatible only with operator version 0.6.0 and later
- Compatible only with proxies version 23.2 and later
- Before updating, please read the QT documentation, which contains information about the new query access control.

New related issues:
- [Problems with QT ACOs](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/176)

In case of an error when starting query
```
Access control object "nobody" does not exist
```
You need to run commands by admin
```
yt create access_control_object_namespace --attr '{name=queries}'
yt create access_control_object --attr '{namespace=queries;name=nobody}'
```

{% endcut %}

{% cut "**0.0.4**" %}

- Applied YQL defaults from the documentation
- Fixed a bag in YQL queries that don't use YT tables
- Fixed a bag in YQL queries that use aggregate functions
- Supported common UDF functions in YQL

NB: This release is compatible only with the operator 0.5.0 and newer versions.

{% endcut %}

{% cut "**0.0.3**" %}

- Fixed a bug that caused the user transaction to expire before the completion of the yql query on IPv4 only networks.
- System query_tracker tables have been moved to sys bundle

{% endcut %}

{% cut "**0.0.2**" %}

—

{% endcut %}

{% cut "**0.0.1**" %}

- Added authentication, now all requests are run on behalf of the user that initiated them.
- Added support for v3 types in YQL queries.
- Added the ability to set the default cluster to execute YQL queries on.
- Changed the format of presenting YQL query errors.
- Fixed a bug that caused errors during the execution of queries that did not return any result.
- Fixed a bug that caused errors during the execution of queries that extracted data from dynamic tables.
- Fixed a bug that caused memory usage errors. YqlAgent no longer crashes for no reason under the load.

{% endcut %}

## Strawberry

Publishes as docker images.

**Current release:**  0.0.11 (`ytsaurus/strawberry:0.0.11`)

**All releases:**

{% cut "**0.0.11**" %}

`ytsaurus/strawberry:0.0.11`

- Improve strawberry cluster initializer to set up JupYT.

{% endcut %}

{% cut "**0.0.10**" %}

`ytsaurus/strawberry:0.0.10`

- Support cookie credentials in strawberry.

{% endcut %}

{% cut "**0.0.9**" %}

`ytsaurus/strawberry:0.0.9`

{% endcut %}

{% cut "**0.0.8**" %}

`ytsaurus/strawberry:0.0.8`

- Support builin log rotation for CHYT controller.
- Improve strawberry API for UI needs.

{% endcut %}

## CHYT

Publishes as docker images.

**Current release:** 2.14.0 (`ytsaurus/chyt:2.14.0-relwithdebinfo`)

**All releases:**

{% cut "**2.14.0**" %}

`ytsaurus/chyt:2.14.0-relwithdebinfo`

- Support SQL UDFs.
- Support reading dynamic and static tables via concat-functions.

{% endcut %}

{% cut "**2.13.0**" %}

`ytsaurus/chyt:2.13.0-relwithdebinfo`

- Update ClickHouse code version to the latest LTS release (22.8 -> 23.8).
- Support for reading and writing ordered dynamic tables.
- Move dumping query registry debug information to a separate thread.
- Configure temporary data storage.

{% endcut %}

{% cut "**2.12.4**" %}

`ytsaurus/chyt:2.12.4-relwithdebinfo`

{% endcut %}

## SPYT

Publishes as docker images.

**Current release:** 1.77.0 (`ytsaurus/spyt:1.77.0`)

**All releases:**

{% cut "**1.77.0**" %}

- Support for Spark Streaming using ordered dynamic tables;
- Support for CREATE TABLE AS, DROP TABLE and INSERT operations;
- Session reuse for QT SPYT engine;
- SPYT compilation using vanilla Spark 3.2.2;
- Minor perfomance optimizations

{% endcut %}

{% cut "**1.76.1**" %}

- Fix IPV6 for submitting jobs in cluster mode;
- Fix Livy configuration;
- Support for reading ordered dynamic tables.

{% endcut %}

{% cut "**1.76.0**" %}

- Support for submitting Spark tasks to YTsaurus via spark-submit;
- Shrinking SPYT distributive size up to 3 times by separating SPYT and Spark dependencies;
- Fix reading nodes with a lot (>32) of dynamic tables ([Issue #240](https://github.com/ytsaurus/ytsaurus/issues/240));
- Assembling sorted table from parts uses concatenate operation instead of merge ([Issue #133](https://github.com/ytsaurus/ytsaurus/issues/133)).

{% endcut %}

{% cut "**1.75.4**" %}

- Fix backward compatibility for ytsaurus-spyt
- Optimizations for count action
- Include livy in SPYT deploying pipeline
- Update default configs

{% endcut %}

{% cut "**1.75.3**" %}

- Added random port attaching for Livy server.
- Disabled YTsaurus operation stderr tables by default.
- Fix nested schema pruning bug.

{% endcut %}

{% cut "**1.75.2**" %}

- More configurable TCP proxies support: new options --tcp-proxy-range-start and --tcp-proxy-range-size.
- Added aliases for type v3 enabling options: spark.yt.read.typeV3.enabled and spark.yt.write.typeV3.enabled.
- Added option for disabling tmpfs: --disable-tmpfs.
- Fixed minor bugs.

{% endcut %}

{% cut "**1.75.1**" %}

- Extracting YTsaurus file system bundle outside Spark Fork
- Fix reading arrow tables from Spark SQL engine
- Binding Spark standalone cluster Master and Worker RPC/REST endpoints to wildcard network interface
- Add configurable thread pool size of internal RPC Job proxy

{% endcut %}

## Kubernetes operator

Publishes as a helm-chart on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).

**Current release:** 0.6.0

**All releases:**

{% cut "**0.6.0**" %}

- added support for updating masters of 23.2 versions;
- added the ability to bind masters to the set of nodes by node hostnames;
- added the ability to configure the number of stored snapshots and changelogs in master spec;
- added the ability for users to create access control objects;
- added support for volume mount with mountPropagation = Bidirectional mode in execNodes;
- added access control object namespace "queries" and object "nobody". They are necessary for query\_tracker versions 0.0.5 and higher;
- added support for the new Cliques CHYT UI;
- added the creation of a group for admins (admins);
- added readiness probes to component statefulset specs;
- improved ACLs on master schemas;
- master and scheduler init jobs do not overwrite existing dynamic configs anymore;
- `exec_agent` was renamed to `exec_node` in exec node config, if your specs have `configOverrides` please rename fields accordingly.

{% endcut %}

{% cut "**0.5.0**" %}

- added minReadyInstanceCount into Ytsaurus components which allows not to wait when all pods are ready;
- support queue agent;
- added postprocessing of generated static configs;
- introduced separate UseIPv4 option to allow dualstack configurations;
- support masters in host network mode;
- added spyt engine in query tracker by default;
- enabled both ipv4 and ipv6 by default in chyt controllers;
- default CHYT clique creates as tracked instead of untracked;
- don't run full update check if full update is not enabled (enable_full_update flag in spec);
- update cluster algorithm was improved. If full update is needed for already running components and new components was added, operator will run new components at first, and only then start full update. Previously such reconfiguration was not supported;
- added optional TLS support for native-rpc connections;
- added possibility to configure job proxy loggers.
- changed how node resource limits are calculated from resourceLimits and resourceRequests;
- enabled debug logs of YTsaurus go client for controller pod;
- supported dualstack clusters in YQL agent;
- supported new config format of YQL agent;
- supported NodePort specification for HTTP proxy (http, https), UI (http) and RPC proxy (rpc port). For TCP proxy NodePorts are used implicitly when NodePort service is chosen. Port range size and minPort are now customizable;
- fixed YQL agents on ipv6-only clusters;
- fixed deadlock in case when UI deployment is manually deleted.

{% endcut %}

{% cut "**0.4.1**" %}

04.10.2023

**Features**
- Support per-instance-group config override.
- Support TLS for RPC proxies.

**Bug fixes**
- Fixed an error during creation of default `CHYT` clique (`ch_public`).

{% endcut %}

{% cut "**0.4.0**" %}

26.09.2023

**Features**

- The operations archive will be updated when the scheduler image changes.
- Ability to specify different images for different components.
- Cluster update without full downtime for stateless components was supported.
- Updating of static component configs if necessary was supported.
- Improved SPYT controller. Added initialization status (`ReleaseStatus`).
- Added CHYT controller and the ability to load several different versions on one {{product-name}} cluster.
- Added the ability to specify the log format (`yson`, `json` or `plain_text`), as well as the ability to enable recording of structured logs.
- Added more diagnostics about deploying cluster components in the `Ytsaurus` status.
- Added the ability to disable the launch of a full update (`enableFullUpdate` field in `Ytsaurus` spec).
- The `chyt` spec field was renamed to `strawberry`. For backward compatibility, it remains in `crd`, but it is recommended to rename it.
- The size of `description` in `crd` is now limited to 80 characters, greatly reducing the size of `crd`.
- `Query Tracker` status tables are now automatically migrated when it is updated.
- Added the ability to set privileged mode for `exec node` containers.
- Added `TCP proxy`.
- Added more spec validation: checks that the paths in the locations belong to one of the volumes, and also checks that for each specified component there are all the components necessary for its successful work.
- `strawberry controller` and `ui` can also be updated.
- Added the ability to deploy `http-proxy` with TLS.
- Odin service address for the UI can be specified in spec.
- Added the ability to configure `tags` and `rack` for nodes.
- Supported OAuth service configuration in the spec.
- Added the ability to pass additional environment variables to the UI, as well as set the theme and environment (`testing`, `production`, etc.) for the UI.
- Data node location mediums are created automatically during the initial deployment of the cluster.

{% endcut %}

{% cut "**0.3.1**" %}

14.08.2023

**Features**

- Added the ability to configure automatic log rotation.
- `tolerations` and `node selectors` can be specified in instance specs of components.
- Types of generated objects are specified in controller configuration, so operator respond to modifications of generated objects by reconciling.
- Config maps store data in text form instead of binary, so that you can view the contents of configs through `kubectl describe configmap <configmap-name>`.
- Added calculation and setting of `disk_usage_watermark` and `disk_quota` for `exec node`.
- Added a SPYT controller and the ability to load the necessary for SPYT into Cypress using a separate resource, which allows you to have several versions of SPYT on one cluster.

**Bug fixes**

- Fixed an error in the naming of the `medium_name` field in static configs.

{% endcut %}

## SDK

### Python

Published as packages to [PyPI](https://pypi.org/project/ytsaurus-client/).

**Current release:** 0.13.12

**All releases:**

{% cut "**0.13.12**" %}

—

{% endcut %}

{% cut "**0.13.7**" %}

TBD

{% endcut %}

### Java

Publishes as Java packages to the [Maven Central Repository](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).

**Current release:** 1.2.0

**All releases:**

{% cut "**1.2.0**" %}

18.09.2023

- Fixed a bug that caused `SyncTableReaderImpl` internal threads would not terminate.
- In the `WriteTable` request, the `needRetries` option is set to `true` by default.
- The `WriteTable` request has `builder(Class)` now; using it, you can omit the `SerializationContext` if the class is marked with the `@Entity` annotation, implements the `com.google.protobuf.Message` or `tech.ytsaurus.ysontree.YTreeMapNode` interface (serialization formats will be `skiff`, `protobuf` or `wire` respectively).
- The `setPath(String)` setters in the `WriteTable` and `ReadTable` builders are `@Deprecated`.
- The interfaces of the `GetNode` and `ListNode` request builders have been changed: `List<String>` is passed to the `setAttributes` method instead of `ColumnFilter`, the `null` argument represents `universal filter` (all attributes should be returned).
- Added the `useTLS` flag to `YTsaurusClientConfig`, if set to `true` `https` will be used for `discover_proxies`.

{% endcut %}

{% cut "**1.1.1**" %}

06.09.2023

- Fixed validation of `@Entity` schemas: reading a subset of table columns, a superset of columns (if the types of the extra columns are `nullable`), writing a subset of columns (if the types of the missing columns are `nullable`).
- The following types are supported in `@Entity` fields:
     - `utf8` -> `String`;
     - `string` -> `byte[]`;
     - `uuid` -> `tech.ytsaurus.core.GUID`;
     - `timestamp` -> `java.time.Instant`.
- If an operation started by `SyncYTsaurusClient` fails, an exception will be thrown.
- Added the `ignoreBalancers` flag to `YTsaurusClientConfig`, which allows to ignore balancer addresses and find only rpc proxy addresses.

{% endcut %}
