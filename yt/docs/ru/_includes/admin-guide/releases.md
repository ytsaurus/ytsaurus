# Релизы

## {{product-name}} server

Основные серверные компоненты релизятся в виде единого докер-образа.

{% note warning "Внимание!" %}

Версия k8s-оператора должна быть не ниже 0.6.0.

{% endnote %}

**Актуальный релиз:** {{yt-server-version}} (`ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo`)

**Все релизы:**

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

Выкладывается в виде докер-образа.

**Актуальный релиз:** 0.0.6 (`ytsaurus/query-tracker:0.0.6-relwithdebinfo`)

**Все релизы:**

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

Выкладывается в виде докер-образа.

**Актуальный релиз:** 0.0.11 (`ytsaurus/strawberry:0.0.11`)

**Все релизы:**

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

Выкладывается в виде докер-образа.

**Актуальный релиз:** 2.14.0 (`ytsaurus/chyt:2.14.0-relwithdebinfo`)

**Все релизы:**

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

Выкладывается в виде докер-образа.

**Актуальный релиз:** 1.77.0 (`ytsaurus/spyt:1.77.0`)

**Все релизы:**

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

Выкладывается в виде helm-чартов в [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).

**Актуальный релиз:** 0.6.0

**Все релизы:**

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

- поддержали возможность переопределять статические конфиги per-instance, а не только на всю компоненту;
- поддержали TLS для RPC-проксей;
- починили падение оператора при поднятии CHYT-клики по-умолчанию - `ch_public`.

{% endcut %}

{% cut "**0.4.0**" %}

- архив операций теперь обновляется при изменении образа;
- возможность указать разные образы для разных компонент;
- поддержано обновление кластера без полного даунтайма для stateless компонент;
- поддержано обновление статических конфигов компонент при необходимости;
- улучшен SPYT-контроллер. Добавлен статус инициализации (`ReleaseStatus`);
- добавлен CHYT-контроллер и возможность загружать несколько разных версия на один кластер. Подробности можно узнать [по ссылке](../../admin-guide/install-additional-components/#chyt);
- добавлена возможность указать формат логов (`yson`, `json` или `plain_text`), а также возможность включить запись структурированных логов. Подробнее можно почитать на странице [Логирование](../../admin-guide/logging);
- добавлено больше диагностики о поднятии компонент в статусе `Ytsaurus`;
- добавлена возможность отключить запуск полного обновления (`enableFullUpdate`);
- поле `chyt` спеки переименовано в `strawberry`. Для обратной совместимости оно остается в `crd`, но рекомендуется его переименовать;
- размер `description` в `crd` теперь ограничивается 80 символами, что сильно уменьшило размер `crd`;
- таблицы состояния `Query Tracker` теперь автоматически мигрируются при его обновлениях;
- добавлена возможность задать привилегированный режим для контейнеров `exec node`;
- добавлена `TCP proxy`;
- добавлено больше валидации спеки, а именно проверки того, что пути в локациях принадлежат какому-то из вольюмов, а также некоторые проверки, что для каждой указанной компоненты есть все компоненты, необходимые для ее успешной работы;
- теперь `strawberry controller` и `ui` также можно обновлять;
- добавлена возможность поднимать http-proxy с TLS;
- можно указать адрес Odin сервиса для UI;
- добавлена возможность конфигурировать `tags` и `rack` для нод;
- поддержена конфигурация OAuth сервиса в спеке;
- добавлена возможность передавать дополнительные переменные окружение в UI, а также задавать тему и окружение (`testing`, `production` итп) для UI;
- медиумы локаций data node создаются автоматически при первичном разворачивании кластера.

{% endcut %}

{% cut "**0.3.1**" %}

- добавлена возможность настроить автоматическую ротацию логов;
- добавлена возможность указать `tolerations` и `node selector` в спеке инстансов компонент;
- исправлена ошибка в наименовании поля `medium_name` в статических конфигах;
- добавлен вызов `Reconcile` оператора при изменении созданных им объектов;
- ConfigMap-ы хранят данные в текстовом виде вместо бинарного, чтобы позволяет смотреть содержимое конфигов через `kubectl describe configmap <configmap-name>`;
- добавлено вычисление и установка `disk_usage_watermark` и `disk_quota` для `exec node`;
- добавлен SPYT-контроллер и возможность загружать в кипарис необходимое для `spyt` с помощью отдельного ресурса, что позволяет иметь несколько версий `SPYT` на одном кластере. Подробности можно узнать [на отдельной странице](../../admin-guide/install-additional-components/#spyt);

{% endcut %}

## SDK

### Python

Выкладывается в виде пакета в [PyPI](https://pypi.org/project/ytsaurus-client/).

**Актуальный релиз:** 0.13.12

**Все релизы:**

{% cut "**0.13.12**" %}

—

{% endcut %}

{% cut "**0.13.7**" %}

—

{% endcut %}

### Java

Выкладывается в виде пакетов в [maven](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).

**Актуальный релиз:** 1.2.1

**Все релизы:**

{% cut "**1.2.1**" %}

29.01.2024

- поддержаны сериализуемые mapper/reducer (реализующие интерфейс `Serializable`);
- добавлен метод `completeOperation`;
- поддержано несколько методов YT Queues API: `registerQueueConsumer`, `advanceConsumer`, `pullConsumer`;
- в возвращаемых методом `partitionTables` объектах `MultiTablePartition` теперь есть `AggregateStatistics`.

{% endcut %}

{% cut "**1.2.0**" %}

18.09.2023

- исправлен баг с закрытием внутренних потоков `SyncTableReaderImpl`;
- в запросе `WriteTable` опция `needRetries` установлена в `true` по умолчанию;
- в запроса `WriteTable` появился `builder(Class)`, при его использовании можно не указывать `SerializationContext`, если класс помечен аннотацией `@Entity`, реализует интерфейс `com.google.protobuf.Message` или является `tech.ytsaurus.ysontree.YTreeMapNode` (для них будут выбраны форматы сериализации `skiff`, `protobuf` или `wire` соответственно);
- сеттеры `setPath(String)` в билдерах `WriteTable` и `ReadTable` объявлены `@Deprecated`;
- изменился интерфейс билдеров запросов `GetNode` и `ListNode`: в метод `setAttributes` вместо `ColumnFilter` теперь передаётся `List<String>`, аргумент `null` означает `universal filter` (вернутся все атрибуты);
- в `{{product-name}}ClientConfig` добавлен флаг `useTLS`, при выставлении которого в `true` для `discover_proxies` будет использоваться `https`.

{% endcut %}

{% cut "**1.1.1**" %}

06.09.2023

- исправлена валидация схем `@Entity`: можно читать подмножество колонок таблицы, надмножество колонок (если типы лишних колонок `nullable`), писать подмножество колонок (если типы недостающих колонок `nullable`);
- в `@Entity` полях поддержаны типы:
    - `utf8` -> `String`;
    - `string` -> `byte[]`;
    - `uuid` -> `tech.ytsaurus.core.GUID`;
    - `timestamp` -> `java.time.Instant`.
- при падении операции, запущенной `Sync{{product-name}}Client`, бросается исключение;
- в `{{product-name}}ClientConfig` добавлен флаг `ignoreBalancers`, позволяющий проигнорировать адреса балансеров и найти только адреса rpc проксей.

{% endcut %}

