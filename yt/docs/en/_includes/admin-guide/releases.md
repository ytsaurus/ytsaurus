# Releases

## {{product-name}} server

All main components are releases as a docker image.

**Current release:** 23.1.0 (`ytsaurus/ytsaurus:stable-23.1.0-relwithdebinfo`)

**All releases:**

{% cut "**23.1.0**" %}

`ytsaurus/ytsaurus:stable-23.1.0-relwithdebinfo`

{% endcut %}

## Query Tracker

Publishes as docker images.

**Current release:** 0.0.1 (`ytsaurus/query-tracker:0.0.1`)

**All releases:**

{% cut "**0.0.1**" %}

`ytsaurus/query-tracker:0.0.1`

{% endcut %}

## Strawberry

Publishes as docker images.

**Current release:**  (`ytsaurus/strawberry:0.0.5`)

**All releases:**

{% cut "**0.0.5**" %}

`ytsaurus/strawberry:0.0.5`

{% endcut %}

## CHYT

Publishes as docker images.

**Current release:** 2.10 (`ytsaurus/chyt:2.10`)

**All releases:**

{% cut "**2.10**" %}

`ytsaurus/chyt:2.10`

{% endcut %}

## SPYT

Publishes as docker images.

**Current release:** 1.76.1 (`ytsaurus/spyt:1.76.1`)

**All releases:**

{% cut "**1.76.1**" %}

`ytsaurus/spyt:1.76.1`

{% endcut %}

{% cut "**1.71.0**" %}

`ytsaurus/spyt:1.71.0`

{% endcut %}

## Kubernetes operator

Publishes as a helm-chart on [docker hub](https://hub.docker.com/r/ytsaurus/ytop-chart/tags).

**Current release:** 0.4.1

**All releases:**

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

**Current release:** 0.13.7

**All releases:**

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
