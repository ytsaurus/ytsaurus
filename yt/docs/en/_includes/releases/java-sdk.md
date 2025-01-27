## Java SDK


Is released as packages in [maven](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).




**Releases:**

{% cut "**1.2.8**" %}

**Release date:** 2025-01-23


* Update protobuf to 3.25.5 version.
* Support for the ability to use a custom `TableRowsSerializer` provided through the `SerializationContext`.
* Change protobuf type of `YPath` and `RichYPath` parameters: `string` -> `bytes`.
* Remove "command" from "started_by" spec field.

{% endcut %}


{% cut "**1.2.7**" %}

**Release date:** 2024-11-25


* Add the `RequestMiddleware` interface to subscribe on request start.
* Support `ListQueueConsumerRegistrations`.
* Add monitoring callback interface for `MultiYTsaurusClient`.
* Refactor `MultiYTsaurusClient`.
* Support `YT_BASE_LAYER`.
* Fix resource leak in the `ClientPool`.

{% endcut %}


{% cut "**1.2.6**" %}

**Release date:** 2024-09-05


* `YsonJsonConverter` has been released.
* Support for `Date32`, `Datetime64`, `Timestamp64`, `Interval64` types.
* Fixed a bug that caused `writeTable` to fail if the table schema did not match the user-specified schema.

{% endcut %}


{% cut "**1.2.5**" %}

**Release date:** 2024-08-20


* Added MultiYTsaurusClient.
* Support for MultiLookupRows request.
* Fixed a bug that caused an infinite wait for proxy discovery when the connection failed.
* Fixed a bug that caused the operation output table to be created without a user-specified transaction.

{% endcut %}


{% cut "**1.2.4**" %}

**Release date:** 2024-06-18


* Support for JPA `@Embedded`/`@Embeddable` annotations.
* Support for URL schema to detect the usage of TLS.
* Implemented YT Query Tracker API methods.


{% endcut %}


{% cut "**1.2.3**" %}

**Release date:** 2024-05-27


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


* Supported placeholder values in SelectRowsRequest.
* Supported specifying the proxy network name.
* Supported set(Input/Output)Format in CommandSpec.
* Fixed a bug that caused NoSuchElementException in SyncTableReader.
* Fixed a bug that caused the table to be recreated when writing without "append".

{% endcut %}


{% cut "**1.2.1**" %}

**Release date:** 2024-01-29


* Supported serializable mapper/reducer.
* Added completeOperation method.
* Implemented three YT Queues API methods: registerQueueConsumer, advanceConsumer, pullConsumer.
* Added AggregateStatistics to MultiTablePartition.
* Some minor bug fixes.

{% endcut %}


{% cut "**1.2.0**" %}

**Release date:** 2023-09-18


- Fixed a bug that caused `SyncTableReaderImpl` internal threads would not terminate.
- In the `WriteTable` request, the `needRetries` option is set to `true` by default.
- The `WriteTable` request has `builder(Class)` now; using it, you can omit the `SerializationContext` if the class is marked with the `@Entity` annotation, implements the `com.google.protobuf.Message` or `tech.ytsaurus.ysontree.YTreeMapNode` interface (serialization formats will be `skiff`, `protobuf` or `wire` respectively).
- The `setPath(String)` setters in the `WriteTable` and `ReadTable` builders are `@Deprecated`.
- The interfaces of the `GetNode` and `ListNode` request builders have been changed: `List<String>` is passed to the `setAttributes` method instead of `ColumnFilter`, the `null` argument represents `universal filter` (all attributes should be returned).
- Added the `useTLS` flag to `YTsaurusClientConfig`, if set to `true` `https` will be used for `discover_proxies`.

{% endcut %}


{% cut "**1.1.1**" %}

**Release date:** 2023-07-26


- Fixed validation of `@Entity` schemas: reading a subset of table columns, a superset of columns (if the types of the extra columns are `nullable`), writing a subset of columns (if the types of the missing columns are `nullable`).
- The following types are supported in `@Entity` fields:
     - `utf8` -> `String`;
     - `string` -> `byte[]`;
     - `uuid` -> `tech.ytsaurus.core.GUID`;
     - `timestamp` -> `java.time.Instant`.
- If an operation started by `SyncYTsaurusClient` fails, an exception will be thrown.
- Added the `ignoreBalancers` flag to `YTsaurusClientConfig`, which allows to ignore balancer addresses and find only rpc proxy addresses.

{% endcut %}

