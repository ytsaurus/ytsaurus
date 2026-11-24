# Working with YDB in {{product-name}} Flow

The YDB extension reads topic messages and writes pipeline results to topics. For an ordinary topic, specify the server endpoint, database, and topic path explicitly. Reading also requires an existing consumer. The account must have permission to read or write the corresponding topic.

The extension classes belong to `NYT::NFlow`. The [source code]({{source-root}}/yt/yt/flow/extensions/ydb) contains connectors, the driver resource, and C++ and Python helpers.

A `flow_server` built from this source includes these registrations. A custom C++ binary built with `ya` must link the extension:

```text
PEERDIR(yt/yt/flow/extensions/ydb/cpp)
```

Older released servers may not contain these classes. These connectors have no public YQL interface.

## Transport requirements {#transport}

The built-in `TYdbTopicDriver` uses plaintext transport. Set `endpoint` to `host:port`, for example `localhost:2135`. It exposes no TLS, CA certificate, or client certificate setting. A `grpcs://` prefix does not enable TLS on this driver path, so the resource cannot connect directly to a TLS-only endpoint.

Token and login authentication do not encrypt the connection. Do not send tokens or passwords over an untrusted plaintext network.

## Preparing topics {#prerequisites}

The server must support Topic API read sessions pinned to individual partitions. Automatic partition splits also require parent-child partition metadata and partition-end events. The Flow account needs `SELECT ROW` on the source topic.

Install the YDB CLI and configure a connection and authentication profile named `flow` using the [YDB CLI connection instructions](https://ydb.tech/docs/en/reference/ydb-cli/connect). Select the same database as in the Flow specification. CLI transport settings are independent: TLS in the CLI does not enable TLS in the Flow resource.

Use an account allowed to create topics and add consumers for this setup. For the examples below, [create the topics](https://ydb.tech/docs/en/reference/ydb-cli/topic-create) and [add consumers](https://ydb.tech/docs/en/reference/ydb-cli/topic-consumer-add):

```bash
ydb --profile flow topic create events --supported-codecs raw,gzip,zstd
ydb --profile flow topic consumer add events --consumer flow-reader --supported-codecs raw,gzip,zstd
ydb --profile flow topic create processed-events --supported-codecs raw,gzip,zstd
ydb --profile flow topic consumer add processed-events --consumer flow-check --supported-codecs raw,gzip,zstd
```

Use new topic names or substitute existing topics and consumers with the required permissions and codecs. `flow-check` is a separate consumer for output verification. A new consumer without a starting timestamp begins at the oldest retained message.

## Binding the driver {#driver}

The `NYT::NFlow::TYdbTopicDriver` resource creates YDB clients. Declare it in `Spec.resources`. Independent resources let readers and writers use different credentials:

```yson
resources = {
    ReadYdb = {
        resource_class_name = "NYT::NFlow::TYdbTopicDriver";
        parameters = {
            auth_mode = "login";
            username = "flow-reader";
            password_env_var = "FLOW_READ_PASSWORD";
        };
        dependencies = {};
    };
    WriteYdb = {
        resource_class_name = "NYT::NFlow::TYdbTopicDriver";
        parameters = {
            auth_mode = "token";
            token_env_var = "FLOW_WRITE_TOKEN";
        };
        dependencies = {};
    };
};
```

Each computation using a connector must bind the resource through `required_resource_ids`. The outer key names a resource in `Spec.resources`; `alias = "YdbDriver"` names it locally inside the computation, where the connector looks it up. Controller and worker resources are mapped independently. Enable both flags for YDB connectors:

```yson
required_resource_ids = {
    ReadYdb = {
        alias = "YdbDriver";
        controller = %true;
        worker = %true;
    };
};
```

This fragment belongs to the reading computation. For the writing computation, use `WriteYdb` with the same local alias, `YdbDriver`. If the global resource is already named `YdbDriver`, the alias can be omitted. See [Resources](../../../flow/cpp/resources.md) for the general rules.

Driver settings and defaults: [`TYdbTopicDriverConfig`](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TYdbTopicDriverConfig).

### Authentication {#authentication}

`token_env_var` and `password_env_var` contain **environment variable names**, never resolved tokens or passwords.

#|
|| **`auth_mode`** | **Credential selection** ||
|| `auto` (default) | Read the token from `token_env_var`, which defaults to `YDB_TOKEN`. An empty variable disables authentication. ||
|| `token` | Read the token from `token_env_var`; an empty value is an error. ||
|| `login` | Require `username` and `password_env_var`. Read only the named password variable; an empty value is an error. ||
|| `anonymous` | Disable authentication without reading token or password variables. ||
|#

In C++, the `TYdbTopicDriver` constructor also accepts a `std::shared_ptr<NYdb::ICredentialsProviderFactory>`. An injected factory takes precedence over credential selection from configuration and the environment.

For a Vanilla operation, list secret variable names in `vanilla.secret_env` in the launch configuration:

```yson
vanilla = {
    secret_env = ["FLOW_READ_PASSWORD"; "FLOW_WRITE_TOKEN";];
};
```

Before launching or reanimating the operation, supply nonempty values in the launching process's environment. The launcher transfers them through the secure vault; node processes restore the environment variables. Keep resolved secrets out of `Spec`. See [Secrets](../../../flow/devops/vanilla/security.md#secrets).

## Reading a topic {#source}

Choose a source class based on the content of one YDB message:

#|
|| **Class** | **Output** ||
|| `NYT::NFlow::TYdbSource` | One decompressed YDB message becomes one Flow message. ||
|| `NYT::NFlow::TYdbMultiLineSource` | Split the message on `row_delimiter`, which defaults to `\n`. ||
|| `NYT::NFlow::TYdbFramingSource` | Split the message into frames; `framing_format` defaults to `auto`. ||
|#

Add the source to the source computation's `source_streams`. This fragment configures an ordinary topic:

```yson
source_streams = {
    events = {
        source_class_name = "NYT::NFlow::TYdbSource";
        parameters = {
            endpoint = "localhost:2135";
            database = "/Root";
            topic = "events";
            consumer = "flow-reader";
            attributes_column = "topic_attributes";
        };
    };
};
```

Replace `endpoint`, `database`, `topic`, and `consumer` with your connection values. All four fields are required; driver configuration does not supply the topic address. The computation also needs the resource binding described in [Binding the driver](#driver).

To verify reading, publish a known message to `events` and check that the source output contains its bytes in `data` and its topic offset in `offset`.

The source produces these columns:

- `data` (`string`): the output message data;
- `unparsed` (`any`): a YSON representation of a malformed message under the `keep` policy;
- `offset` (`int64`): the original YDB message offset;
- `sub_offset` (`int64`): the zero-based message index within the split result.

The optional `attributes_column` adds a YSON column containing user attributes. Its name must differ from `data`, `unparsed`, `offset`, and `sub_offset`. Flow service attributes and reserved YDB names are hidden, except for `__key`, which contains the Kafka key.

Set `read_from_instant`, `max_time_lag`, and `malformed_message_policy` under `DynamicSpec.computations/<computation>/source_streams/<source>/parameters`. The default `fail` policy fails processing of a malformed message; `drop` skips it, and `keep` preserves it in `unparsed`. For framing, agree on the format with the writer: `auto` cannot detect `light_protoseq` and may misidentify an ambiguous `lenval` prefix. See [Framing library](../../../flow/extensions/framing.md).

Parameters and defaults:

- `TYdbSource`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbSource), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbSource).
- `TYdbMultiLineSource`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbMultiLineSource), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbMultiLineSource).
- `TYdbFramingSource`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbFramingSource), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbFramingSource).

`finite` defaults to false. With `finite = %true` in the source parameters, the source can finish when its committed progress reaches the confirmed current end offsets of its partitions. These are observed end offsets, not a fixed snapshot captured at startup. A temporary lack of messages is not end-of-data, and continued writes can delay completion.

### Offsets and recovery boundaries {#source-recovery}

On first start, a read session starts from the consumer's committed position. To exclude older messages after registering a consumer, set `read_from_instant` in the dynamic source parameters before starting the pipeline. This timestamp and `max_time_lag` impose additional lower bounds; neither rewinds before the committed consumer offset.

Flow persists reading progress in pipeline state. If the consumer offset committed on the server is ahead of that progress, messages between Flow's persisted position and that offset are not read by this session. An independent reader that commits offsets for the same consumer can cause Flow to skip those messages. Use a separate consumer for such readers.

When replaying saved progress stalls or encounters a gap, Flow can call `DescribePartition` with statistics enabled to check the retention boundary. This recovery path requires the server to support that request and return partition statistics containing the oldest retained start offset. Verify that your deployment returns these statistics.

When a partition splits, Flow drains the parent through its `Completed` lifecycle before starting its children. Flow persists parent-child relationships and completion state, so the barrier survives restarts and repeated splits. If a parent is no longer reported by the server, Flow drains its existing saved work as a finite partition before releasing its children. This preserves the order of saved work across the split. Messages already removed by retention cannot be recovered.

For a successful drain, Flow's saved progress and the consumer's committed offset must match.

### Federation {#federation}

A YDB topic federation lets Flow use topics with the same path on several named YDB clusters. It requires a federation discovery service provided by your deployment operator or YDB vendor. Service setup depends on the distribution. For a federated source or `TYdbTwoPhaseCommitSink`, set `endpoint` to the gRPC address serving `Ydb.FederationDiscovery.V1.FederationDiscoveryService/ListFederationDatabases`. The service must return a successful, ready operation with a `ListFederationDatabasesResult` that lists the named members in `federation_databases`. A single unnamed entry is the SDK's fallback to ordinary single-cluster operation. See the [gRPC service definition](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/grpc/ydb_federation_discovery_v1.proto) and the [response definition](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_federation_discovery.proto).

#### Deployment requirements

- Advertise a distinct `name` and a stable, unique `endpoint` for each member. The `path` of every member must exactly match the connector's `database`. The Flow controller and workers must be able to reach the discovery address, each member address, and the YDB service addresses that members advertise to clients. The [plaintext transport requirements](#transport) apply to both discovery and member connections.
- Report the current status of each member. `AVAILABLE` makes a member eligible for new two-phase commit writes. `READ_ONLY` and `UNAVAILABLE` exclude it from new writes by that sink. Flow keeps these members in the source topology while discovery lists them. Actual reading depends on whether Flow can reach the member and open a Topic API read session there.
- Each `TYdbTopicDriver` uses the same [authentication settings](#authentication) for discovery and all members. Check that the discovery service and each member accept the configured credentials. In `login` mode, the discovery address must also support `Ydb.Auth.V1.AuthService/Login`. On each member, the Flow account needs `CONNECT` on the database and `DESCRIBE SCHEMA` on the topic. It also needs `SELECT ROW` on topics it reads and `UPDATE ROW` on topics it writes. See the [YDB access rights reference](https://ydb.tech/docs/en/yql/reference/syntax/grant).
- Provision each topic independently on every member. For each connector, the same `topic` value must resolve to the same topic path in the configured database on all members. Keep the partition count and layout consistent, including when changing partitioning. Add a streaming consumer with the same name on every member for each source. Match topic and consumer codecs to those used by producers. Set retention to cover expected read lag and outages. For two-phase commit, also allow enough time to finish pending publications after an outage.
- For `TYdbTwoPhaseCommitSink`, enable [Topic Deferred Publish](#delivery) on every member that can become writable. Its operations require an authenticated identity. When rotating credentials, preserve the publication owner's security identifier (SID) so that Flow can resume a checkpointed publication.

#### Prepare and verify the members

For example, set `endpoint = "discovery.example.org:2135"` and `database = "/Root/flow"` in the source and two-phase commit sink. The discovery address returns these members:

#|
|| **`name`** | **`id`** | **`endpoint`** | **`path`** | **`status`** ||
|| `east` | `flow-east` | `ydb-east.example.org:2135` | `/Root/flow` | `AVAILABLE` ||
|| `west` | `flow-west` | `ydb-west.example.org:2135` | `/Root/flow` | `AVAILABLE` ||
|#

Create YDB CLI profiles `flow-east` and `flow-west` as in [Preparing topics](#prerequisites), pointing each to its member address and `/Root/flow`. On both members, use a provisioning account to create the source topic `events` with its Flow consumer `flow-reader` and the sink topic `processed-events` with a separate verification consumer `flow-check`. The example uses two partitions, disables automatic partitioning, and retains messages for 24 hours. Adjust retention to your workload and the limits on both members. See [Creating a topic](https://ydb.tech/docs/en/reference/ydb-cli/topic-create) and [Adding a topic consumer](https://ydb.tech/docs/en/reference/ydb-cli/topic-consumer-add).

```bash
for profile in flow-east flow-west; do
    ydb --profile "$profile" topic create --partitions-count 2 --auto-partitioning-strategy disabled \
        --retention-period 24h --supported-codecs raw,gzip,zstd events
    ydb --profile "$profile" topic consumer add --consumer flow-reader --supported-codecs raw,gzip,zstd events
    ydb --profile "$profile" topic create --partitions-count 2 --auto-partitioning-strategy disabled \
        --retention-period 24h --supported-codecs raw,gzip,zstd processed-events
    ydb --profile "$profile" topic consumer add --consumer flow-check --supported-codecs raw,gzip,zstd processed-events
done
```

Before starting the pipeline, call `ListFederationDatabases` from the Flow network using the service definition. The request body is empty. Send the database path in the `x-ydb-database` metadata and, if authentication is enabled, the token in `x-ydb-auth-ticket`. For `login`, use the token returned by login. Verify that the response lists the expected members, addresses, database path, and statuses.

Run `ydb --profile flow-east scheme describe events` and repeat for both topics and both profiles. Compare partition counts, codecs, and retention, and verify that both members list `flow-reader` on `events` and `flow-check` on `processed-events`. Repeat the checks using the Flow identities to confirm that each can connect and describe its topic. See [Getting information about schema objects](https://ydb.tech/docs/en/reference/ydb-cli/commands/scheme-describe).

In a test pipeline, set `topic = "events"` and `consumer = "flow-reader"` on the source and `topic = "processed-events"` on the sink. Use the write command in [Examples and verification](#examples) with each member's profile and a distinct marker. Verify that Flow reads both markers. Start a fresh test pipeline for each two-phase commit target with only that member marked `AVAILABLE`. Use a distinct marker for each run. After the Flow epoch commits, use the read command with that member's profile and the `flow-check` consumer. For repeated checks with that dedicated consumer, set `--commit true` to advance its offset.

#### Read and write behavior

Sources read each member's original topic independently. Flow stores reading progress by member endpoint and partition. Flow provides no topic replication, cross-member deduplication, or synchronization of consumer offsets. To avoid processing the same logical event twice, publish it to one member or deduplicate it in the pipeline.

Ordinary sinks connect to one YDB deployment through the configured `endpoint` and send payloads to that deployment's Topic API endpoints. If you enable `write_flow_queue_meta`, the controller can also send heartbeats to discovered members. `TYdbTwoPhaseCommitSink` uses federation discovery to choose a writable member for new publications. Flow can choose any eligible member when it needs a new target.

A pending publication stays on the endpoint and database where it was prepared, even if Flow chooses a different target for new work. If the original member becomes unreachable, publication can wait for it to recover. In strict ordering mode, an unfinished partition can delay its successor. When the sink switches targets, messages from different epochs can become visible out of order. Set `distribution_ordering = "relaxed"` in the computation specification if the pipeline can tolerate this behavior. In strict mode Flow reports an ordering warning. Availability and retention on the original member still determine whether its pending data can be published.

Keep member addresses stable. Changing an advertised address changes the source partition keys and leaves pending publications tied to the old address. Before replacing or removing a member, stop sending it new data and drain outstanding reads and publications while its endpoint is still reachable. Follow the [saved state compatibility rules](#compatibility) for changes to the source or discovery address.

#### Diagnose discovery and member errors

- If you see only the configured endpoint where you expected named members, check whether the discovery RPC returned `UNIMPLEMENTED` or `BAD_REQUEST`. Both can lead to single-cluster fallback.
- For `Failed to discover topic clusters`, check the nested error for the discovery endpoint, authentication, and whether the response contains member entries.
- For `Failed to update cluster info`, check the nested error, member address, exact database path, topic path, and `DESCRIBE SCHEMA` permission. If members report different partition counts, reconcile the topic layouts. Flow cannot infer a missing count when the known counts disagree. A member with neither known topology nor an obtainable or inferable partition count prevents refreshing the partition listing for the whole computation. Flow keeps the existing source partition layout until it can refresh the list.
- For `Federation discovery returned no writable cluster`, check the advertised statuses, pending publications, and the nested errors for preceding write attempts. For `UNSUPPORTED` during deferred publication, verify server support and the feature flag on the selected member.
- For `Topic discovery semantics changed during the sink lifetime`, check whether the discovery service switched between named members and the unnamed fallback. Restore the expected response before resuming the sink.

## Writing to a topic {#sink}

Every sink requires `endpoint`, `database`, and `topic`. Sinks read serialized bytes from `payload_column`, which defaults to `data`.

#|
|| **Class** | **Packing and delivery** ||
|| `NYT::NFlow::TYdbSink` | One Flow message per YDB message; asynchronous delivery through `output_messages`. ||
|| `NYT::NFlow::TYdbFramingSink` | Multiple Flow messages in one framing block; asynchronous delivery through `output_messages`. ||
|| `NYT::NFlow::TYdbTwoPhaseCommitSink` | Deferred publication after epoch commit; requires server support. Optional `framing_format` enables packing. ||
|| `NYT::NFlow::TYdbAtLeastOnceSink` | Direct delivery of individual messages; at least once. ||
|| `NYT::NFlow::TYdbAtLeastOnceFramingSink` | Direct delivery of framing blocks; at least once. ||
|#

For example, use this fragment in the writing computation's `sinks`:

```yson
sinks = {
    ydb = {
        sink_class_name = "NYT::NFlow::TYdbSink";
        input_stream_ids = ["events";];
        parameters = {
            endpoint = "localhost:2135";
            database = "/Root";
            topic = "processed-events";
            payload_column = "data";
        };
    };
};
```

The `events` stream must exist in the pipeline and contain a string column named `data`. Replace the address and topic path with your values, and bind `WriteYdb` to the computation with the `YdbDriver` alias.

To verify writing, pass a known `data` value through `events`, wait for the Flow epoch to commit, and read `processed-events` with a separate YDB consumer. The topic message should contain that value.

Sink defaults are `codec = "zstd"` and `compression_level = 4`. `ParseYdbCodec` and `DecompressYdbTopicMessage` in `cpp/helpers.h` convert codec names and decompress messages. Framing is selected independently of compression: `TYdbFramingSink` and `TYdbAtLeastOnceFramingSink` default to `lenval`.

Framing sinks use the dynamic thresholds `max_rows_per_batch = 1000` and `max_bytes_per_batch = 5242880` (5 MiB) by default. Set them in the sink parameters under `DynamicSpec.computations/<computation>/sinks/<sink>/parameters`.

Parameters and defaults:

- `TYdbSink`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbSink), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbSink).
- `TYdbFramingSink`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbFramingSink), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbFramingSink).
- `TYdbTwoPhaseCommitSink`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbTwoPhaseCommitSink), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbTwoPhaseCommitSink).
- `TYdbAtLeastOnceSink`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbAtLeastOnceSink), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbAtLeastOnceSink).
- `TYdbAtLeastOnceFramingSink`: [static](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbAtLeastOnceFramingSink), [dynamic](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbAtLeastOnceFramingSink).

The bundled public client supports `raw`, `gzip`, and `zstd` with its default codec registrations. Although the codec-name parser accepts `lzop`, the default client does not register an LZOP encoder. Using it requires application-level registration of a compatible implementation through `NYdb::NTopic::TCodecMap::Set` and server support for that codec.

### Delivery guarantees {#delivery}

The asynchronous `TYdbSink` and `TYdbFramingSink` persist output in `output_messages` with the Flow epoch commit. Retries use stable `producer_id` and `seqNo` values so YDB can deduplicate messages. Exactly-once delivery relative to Flow epochs assumes intact pipeline state and topic-side deduplication support.

The asynchronous sinks retain their `producer_id` in pipeline state and use it as the YDB producer and message-group ID. After the topic's partition count increases, an existing producer ID can in some cases prevent writes to new partitions. Contact the Flow maintainers for safe regeneration. Do not clear state, reset `seqNo`, or manually replace the ID as a generic recovery procedure.

At-least-once variants write directly. If an epoch fails after a write, replay may write the same messages again.

`TYdbTwoPhaseCommitSink` uses **deferred publication**, rather than conventional topic transactions. Message content is prepared on the server and becomes visible after the Flow epoch commits. Persisted state allows publication to resume after restart. The sink does not remove duplicates already present in its input.

The server must support preparation, publication, and cancellation. The verified configuration for YDB release `26.3.1.16-rc` enables `config.feature_flags.enable_topic_deferred_publish`; without that flag, the server returns `UNSUPPORTED`. The exact version, binary and configuration identities, and capability results are recorded in [server.lock.json]({{source-root}}/yt/yt/flow/extensions/ydb/test/server.lock.json). Verify other deployments separately before choosing this sink.

The [public test configuration renderer]({{source-root}}/yt/yt/flow/extensions/ydb/test/public_ydb.py) adds `feature_flags` under the existing root `config` mapping when deferred publication is enabled. The corresponding MainConfig fragment is shown below; merge these settings into the existing `config` mapping:

```yaml
config:
  feature_flags:
    enable_topic_deferred_publish: true
```

Unpublished messages must remain on the server until publication completes: once retention removes them, Flow state cannot restore their content. Waiting for an unavailable target may delay partition completion. Do not delete publication state to bypass the wait: deletion does not cancel server-side publication.

### Attributes and Flow metadata {#attributes}

For variants without framing, `attributes_column` names a YSON column containing user attributes. `TYdbTopicAttributes` is a map of string names to string values; `TYdbMessageMeta` is a list of string pairs. Helpers in `cpp/attributes.h` include:

- `PackYdbTopicAttributes`: create a column value and validate names and limits;
- `UnpackYdbTopicAttributes`: read that value, including source output;
- `ParseYdbTopicAttributesColumn`: parse the sink column; `null` means no attributes.

The `_ytflow_` and `__` prefixes are reserved, with `__key` allowed for the Kafka key. Framing sinks reject `attributes_column`; `TYdbTwoPhaseCommitSink` also rejects it when `framing_format` is set.

`write_flow_queue_meta` defaults to false. It transfers `EventTimestamp` and `EventWatermark` to the next pipeline. The source accepts both `flow_queue_meta` and `_ytflow_queue_meta`; the sink writes `_ytflow_queue_meta`. The dynamic `flow_queue_meta_heartbeat_period` defaults to `10s`.

## Configuration errors {#configuration-errors}

- `Static resource is not found` with `resource_id=YdbDriver`: check the computation's `required_resource_ids`, the `YdbDriver` alias, and the `controller` or `worker` flag for the failing process. Both flags must be enabled.
- `Configured token environment variable ... is empty` or `Configured password environment variable ... is empty`: provide a nonempty value in the named variable and pass it through `vanilla.secret_env` when using Vanilla.
- `Column ... is already present in the topic source schema`: choose an `attributes_column` name different from the source output columns.
- `Framing does not support the "attributes_column" parameter`: in `TYdbTwoPhaseCommitSink`, `framing_format` and `attributes_column` cannot both be set. Remove `attributes_column` to keep framing enabled.
- `Failed to init session in time`: initialization polled without receiving an event after `start_session_timeout` elapsed (default `300s`).
- `Session is closed`: the SDK closed the read session; Flow propagates the error with the SDK status and issues in the `event` attribute. A partition-session closure is reported separately as `Partition session is closed`.

For read-session failures, check that the topic and consumer exist, credentials permit reading, and the endpoint is reachable. Inspect the SDK status and issues when present. The outer error message alone does not establish which of these conditions caused the failure.

## Persisted state compatibility {#compatibility}

The source identity of `TYdbSource`, `TYdbMultiLineSource`, and `TYdbFramingSource` is derived from `ydb-topic/v1`, `database`, and `topic`. The consumer and source class are not part of this identity. A partition key contains the source identity, an endpoint, and a partition index. The source engine connects to the endpoint stored in that key. Changing between these three classes alone does not start independent reading.

For an ordinary topic, newly listed partition keys use the configured `endpoint`; changing it changes those keys. In a named federation, keys use the discovered cluster endpoints. Changing the federation discovery address leaves keys unchanged only if the discovered cluster endpoints remain unchanged; it does not by itself establish progress compatibility.

Changing `database` or `topic` changes the source identity and therefore the partition keys. Follow the [source-change rules](../../../flow/connectors/about.md#source-change): stop the pipeline before changing the static specification; old partitions complete and new partitions start independently. Replacing a source outside these three public YDB classes requires a complete drain and a pipeline in `Stopped`. Offsets from that source are not imported, and partial reuse of its persisted state is unsupported.

Before changing an endpoint, stop the pipeline jobs. If compatibility with persisted keys and state has not been established, complete a drain to `Stopped` and consult the Flow maintainers. Do not rewrite keys or offsets manually. An unchanged source specification continues to use its existing state.

## Examples and verification {#examples}

The public [integration test]({{source-root}}/yt/yt/flow/extensions/ydb/test/integration/test_ydb.py), [pipeline template]({{source-root}}/yt/yt/flow/extensions/ydb/test/pipeline/pipeline.yson), and [C++ computation]({{source-root}}/yt/yt/flow/extensions/ydb/test/pipeline/main.cpp) demonstrate reading protobuf messages, processing them, and writing results. The test harness supplies addresses, credentials, and the sink class. The template contains substitutions and is not a ready deployment file.

For a pipeline using `TYdbSource` and `TYdbSink` that forwards `data` unchanged from `events` to `processed-events`, use the topics and consumers prepared above. After starting that pipeline, [write one plain message](https://ydb.tech/docs/en/reference/ydb-cli/topic-write):

```bash
printf '%s' 'flow-check-message' | ydb --profile flow topic write events --format single-message
```

After the Flow epoch commits, [read the output](https://ydb.tech/docs/en/reference/ydb-cli/topic-read) with the separate consumer:

```bash
ydb --profile flow topic read processed-events --consumer flow-check --format single-message --limit 1 --wait --commit false
```

For fresh topics, the output should be `flow-check-message`. The read waits if no output is available; interrupt it with Ctrl+C if needed and check the pipeline and [configuration errors](#configuration-errors). These commands use plain message bytes and do not exercise protobuf or framing.

## See also {#see-also}

[Flow processing guarantees](../../../flow/concepts/guarantees.md) describe how connector guarantees combine with pipeline processing.
