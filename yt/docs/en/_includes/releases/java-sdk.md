## Java SDK


Is released as packages in [Maven Central](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).




**Releases:**

{% cut "**1.2.15**" %}

**Release date:** 2026-04-29


#### Features:
- Support `AttachTransaction` [30d8fa3bd2fd17ca2277517f2928dec06da15199]
- Implement `SuppressableAccessTrackingOptions` [45cea080eace9c29a05fe693166abc9250d8f5b1]
- Support compressed data size parameter in `PartitionTables` method [212229494209279cdea52254cc1af67369ff08f2]

#### Fixes:
- Fixed NPE in stream writer when `onPayload` arrives before `onStartStream` [7e625a51db05eb717f010ce00b33e135de029268]

{% endcut %}


{% cut "**1.2.14**" %}

**Release date:** 2026-03-25


#### Features:
- Support optional `expressionBuilderVersion` in `SelectRowsRequest` [1736128d2d5b83f9881112d71e5b203bdd3c65d0]
- Add `GetCurrentUser` method [b5f3560971dc1eafe530180df21d7f8eefd91d8b]
- Support non-optional list elements in Entity from table schema [4635b29bb95c2bfe2f2b4f66fb6f6e45d8c7adb4]
- Support listener for tracking bytes received by the client [e7c49e711236d3dcf81c27c9877f72deb17fce40]

#### Fixes:
- Fixed repeated fields accumulation in `TableAttachmentProtobufReader` [3d58a41edb1bdfe62533f9ad8c253f2f992cb5ba]
- Fixed `CheckPermission` column parameters to allow nulls, since it was failing at API side with "Cannot specify columns for full_read permission check" [b87a0ade9bf289f89f91b02bb996e00fd3b90b13]
- Added `MessageLite` support for `TiType` column definition and `EntitySkiffSerializer` [2675dd884e8779457856c68870c72646533c8c18]

{% endcut %}


{% cut "**1.2.13**" %}

**Release date:** 2026-02-16


#### Features:
- Support `Short` in `YTreeBuilder`.
- Add `findByCode` to `YTsaurusErrorCode`.

#### Fixes:
- Fix empty attachment size (1 instead of 0).
- Optimize `ColumnValueType` internal representation for performance (replace `Map` with `Array`).
- Pre-compute column type and wire type in `ColumnSchema`.

{% endcut %}


{% cut "**1.2.12**" %}

**Release date:** 2025-12-11


#### Features
* Add methods lookupRowsV2, versionedLookupRowsV2 and multiLookupRowsV2 with partial result support.
* Support 'omit\_inaccessible\_rows' flag in read\_table and read\_table\_partition API calls.

#### Fixes
* Make query statistics aggregates public.
* Update log4j and log4j-slf4j versions in order to fix vulnerability in log4j.

{% endcut %}


{% cut "**1.2.11**" %}

**Release date:** 2025-10-23


#### Features
* Add `execute` method to `MultiYTsaurusClient`.
* Support `YPath` empty root designator for operation spec.
* Support `PatchOperationSpec` method.
* Add API for distributed reading: `createTablePartitionReader`.
* Add `PullQueue` method.
* Implement a pretty-print mode for YSON text serialization.
* Add API for distributed writing: `startDistributedWriteSession`, `writeTableFragment`, `finishDistributedWriteSession`.
* Support listener (`RpcClientListener`) for tracking bytes sent by the client.
* Add `sortOrder` flag to `ListQueries`.

#### Fixes
* Fix `OperationContext.getTableIndex()` when using `ReducerWithKey`.

{% endcut %}


{% cut "**1.2.10**" %}

**Release date:** 2025-07-17


**Release page:** [1.2.10](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.10)


**Maven Central:** [1.2.10](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.10)


* Add `range` to `CreateShuffleReader`.
* Add `writerIndex`, `overwriteExistingWriterData` to `CreateShuffleWriter`.
* Minor fixes and improvements to error messages.

{% endcut %}


{% cut "**1.2.9**" %}

**Release date:** 2025-04-08


**Release page:** [1.2.9](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.9)


**Maven Central:** [1.2.9](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.9)


* Add `fullResult` to `QueryResult`.
* Add `QueryStatistics` to `SelectRowsResult`.
* Set type 'composite' instead of 'any' for complex types in `UnversionedValue`.
* Support 'sort_by' in ReduceSpec.
* Add API methods for working with Shuffle Service.
* Support retries for cross cell copy/move.
* Support additional secrets in `StartQuery`.
* Add retry for timeout `YTsaurusError`.
* Don't fail on 'cluster' attribute in queue and consumer paths in the listQueueConsumerRegistrations.

{% endcut %}


{% cut "**1.2.8**" %}

**Release date:** 2025-01-23


**Release page:** [1.2.8](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.8)


**Maven Central:** [1.2.8](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.8)


* Update protobuf to 3.25.5 version.
* Support for the ability to use a custom `TableRowsSerializer` provided through the `SerializationContext`.
* Change protobuf type of `YPath` and `RichYPath` parameters: `string` -> `bytes`.
* Remove "command" from "started_by" spec field.

{% endcut %}


{% cut "**1.2.7**" %}

**Release date:** 2024-11-25


**Release page:** [1.2.7](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.7)


**Maven Central:** [1.2.7](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.7)


* Add the `RequestMiddleware` interface to subscribe on request start.
* Support `ListQueueConsumerRegistrations`.
* Add monitoring callback interface for `MultiYTsaurusClient`.
* Refactor `MultiYTsaurusClient`.
* Support `YT_BASE_LAYER`.
* Fix resource leak in the `ClientPool`.

{% endcut %}


{% cut "**1.2.6**" %}

**Release date:** 2024-09-05


**Release page:** [1.2.6](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.6)


**Maven Central:** [1.2.6](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.6)


* `YsonJsonConverter` has been released.
* Support for `Date32`, `Datetime64`, `Timestamp64`, `Interval64` types.
* Fixed a bug that caused `writeTable` to fail if the table schema did not match the user-specified schema.

{% endcut %}


{% cut "**1.2.5**" %}

**Release date:** 2024-08-20


**Release page:** [1.2.5](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.5)


**Maven Central:** [1.2.5](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.5)


* Added MultiYTsaurusClient.
* Support for MultiLookupRows request.
* Fixed a bug that caused an infinite wait for proxy discovery when the connection failed.
* Fixed a bug that caused the operation output table to be created without a user-specified transaction.

{% endcut %}


{% cut "**1.2.4**" %}

**Release date:** 2024-06-18


**Release page:** [1.2.4](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.4)


**Maven Central:** [1.2.4](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.4)


* Support for JPA `@Embedded`/`@Embeddable` annotations.
* Support for URL schema to detect the usage of TLS.
* Implemented YT Query Tracker API methods.


{% endcut %}


{% cut "**1.2.3**" %}

**Release date:** 2024-05-27


**Release page:** [1.2.3](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.3)


**Maven Central:** [1.2.3](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.3)


* Introduced `DiscoveryClient`.
* The following types are supported in `@Entity` fields (use `@Column(columnDefinition=“...”)` to specify type):
    * enum -> utf8/string; 
    * String -> string;
    * Instant -> int64;
    * YsonSerializable -> yson.
* Fixed a bug due to which `YTsaurusClient` did not terminate.

{% endcut %}


{% cut "**1.2.2**" %}

**Release date:** 2024-04-11


**Release page:** [1.2.2](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.2)


**Maven Central:** [1.2.2](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.2)


* Supported placeholder values in SelectRowsRequest.
* Supported specifying the proxy network name.
* Supported set(Input/Output)Format in CommandSpec.
* Fixed a bug that caused NoSuchElementException in SyncTableReader.
* Fixed a bug that caused the table to be recreated when writing without "append".

{% endcut %}


{% cut "**1.2.1**" %}

**Release date:** 2024-01-29


**Release page:** [1.2.1](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.1)


**Maven Central:** [1.2.1](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.1)


* Supported serializable mapper/reducer.
* Added completeOperation method.
* Implemented three YT Queues API methods: registerQueueConsumer, advanceConsumer, pullConsumer.
* Added AggregateStatistics to MultiTablePartition.
* Some minor bug fixes.

{% endcut %}


{% cut "**1.2.0**" %}

**Release date:** 2023-09-18


**Release page:** [1.2.0](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.0)


**Maven Central:** [1.2.0](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.0)


- Fixed a bug that caused `SyncTableReaderImpl` internal threads would not terminate.
- In the `WriteTable` request, the `needRetries` option is set to `true` by default.
- The `WriteTable` request has `builder(Class)` now; using it, you can omit the `SerializationContext` if the class is marked with the `@Entity` annotation, implements the `com.google.protobuf.Message` or `tech.ytsaurus.ysontree.YTreeMapNode` interface (serialization formats will be `skiff`, `protobuf` or `wire` respectively).
- The `setPath(String)` setters in the `WriteTable` and `ReadTable` builders are `@Deprecated`.
- The interfaces of the `GetNode` and `ListNode` request builders have been changed: `List<String>` is passed to the `setAttributes` method instead of `ColumnFilter`, the `null` argument represents `universal filter` (all attributes should be returned).
- Added the `useTLS` flag to `YTsaurusClientConfig`, if set to `true` `https` will be used for `discover_proxies`.

{% endcut %}


{% cut "**1.1.1**" %}

**Release date:** 2023-07-26


**Release page:** [1.1.1](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.1.1)


**Maven Central:** [1.1.1](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.1.1)


- Fixed validation of `@Entity` schemas: reading a subset of table columns, a superset of columns (if the types of the extra columns are `nullable`), writing a subset of columns (if the types of the missing columns are `nullable`).
- The following types are supported in `@Entity` fields:
     - `utf8` -> `String`;
     - `string` -> `byte[]`;
     - `uuid` -> `tech.ytsaurus.core.GUID`;
     - `timestamp` -> `java.time.Instant`.
- If an operation started by `SyncYTsaurusClient` fails, an exception will be thrown.
- Added the `ignoreBalancers` flag to `YTsaurusClientConfig`, which allows to ignore balancer addresses and find only rpc proxy addresses.

{% endcut %}

