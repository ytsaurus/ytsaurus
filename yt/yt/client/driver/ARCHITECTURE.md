# YTsaurus Driver Interface Architecture

This document describes the architecture of `yt/yt/client/driver`, the component that exposes a command-oriented YTsaurus API on top of `NApi::IClient`.

It is intended for development work, especially for AI agents that need to modify or extend the driver safely.

## Purpose

The driver is the command execution layer used by higher-level frontends such as proxies and clients that speak in terms of:

- command name
- structured parameter map
- optional input stream
- optional output stream
- authenticated user context

The driver converts this generic request envelope into concrete `NApi::IClient` calls.

In practice, the driver is the boundary between transport-level request handling and the typed client API.

## Key files

- `driver.h`
  - Public request/descriptor/driver interfaces.
- `driver.cpp`
  - Concrete `TDriver` implementation.
  - Command registration table.
  - Request execution flow.
- `command.h`
  - Base command interfaces.
  - Reusable command mixins for common option families.
- `command-inl.h`
  - Shared parameter registration for mixins.
  - Transaction attachment and tablet transaction helpers.
- `command.cpp`
  - Generic command execution, request deserialization, response-parameter handling, API-version-aware output helpers.
- `config.h` / `config.cpp`
  - Driver-level config and defaults.
- `*_commands.h` / `*_commands.cpp`
  - Command families grouped by subsystem.
- `proxy_discovery_cache.h` / `.cpp`
  - Cache used by `discover_proxies`.
- `helpers.h` / `.cpp`
  - ETag parsing and trace helper utilities.

## Public abstraction model

### `TDriverRequest`

`TDriverRequest` is the full per-request envelope.

Important fields:

- `CommandName`
  - Registered command string, e.g. `get`, `read_table`, `start_query`.
- `Parameters`
  - Structured YTree map with command parameters.
- `InputStream`
  - Required for commands whose descriptor input type is not `Null`.
- `OutputStream`
  - Required for commands whose descriptor output type is not `Null`.
- `AuthenticatedUser`
  - Effective user for the request.
- `UserTag`, `UserRemoteAddress`, `UserToken`, `ServiceTicket`
  - Authentication and attribution metadata.
- `ResponseParametersConsumer`
  - Separate structured side-channel for metadata emitted before streamed output.
- `ResponseParametersFinishedCallback`
  - Called after response parameters are produced and before first output write.
- `MemoryUsageTracker`
  - Propagated resource accounting hook.

`TDriverRequest` may hold an external ref-counted object to keep request-owned resources alive until completion.

### `TCommandDescriptor`

Each command has a static descriptor with:

- `CommandName`
- `InputType`
- `OutputType`
- `Volatile`
- `Heavy`

These descriptors are both runtime metadata and an important compatibility contract.

### `IDriver`

Main operations:

- `Execute`
- `FindCommandDescriptor`
- `GetCommandDescriptors`
- `ClearMetadataCaches`
- `GetStickyTransactionPool`
- `GetProxyDiscoveryCache`
- `GetConnection`
- `GetSignatureValidator`
- `Terminate`

The interface is explicitly thread-safe and reentrant.

## Driver construction

`CreateDriver(connection, config, signatureValidator)` constructs `TDriver` with:

- the supplied `NApi::IConnection`
- `TDriverConfig`
- a `TClientCache`
- a root client (`TClientOptions::Root()`)
- a proxy discovery cache backed by the root client
- a sticky transaction pool

The root client is used for functionality that must not depend on per-user authentication, e.g. proxy discovery support.

## Command registration model

All command registration lives in the `TDriver` constructor in `driver.cpp`.

This registration table is the authoritative API surface of the driver.

Each registered entry binds:

- a command string
- a command class
- input/output `EDataType`
- volatility/heaviness metadata
- optional API version gating

Two macros are used:

- `REGISTER(...)`
  - Registers a command only for a specific driver API version.
- `REGISTER_ALL(...)`
  - Registers a command for all supported driver API versions.

### Important invariant

If a command exists in a `*_commands.*` file but is not registered in `driver.cpp`, it is not part of the exposed driver API.

## Request execution flow

The execution path is:

1. `IDriver::Execute(request)` is called.
2. The driver looks up the command descriptor by `request.CommandName`.
3. A per-request `TClientAuthenticationIdentity` is built from:
   - `AuthenticatedUser`
   - `UserTag`
   - `ServiceTicket`
4. The driver validates stream presence against descriptor input/output types.
5. Per-request `TClientOptions` are assembled:
   - password requirements for auth commands
   - token
   - service ticket auth
   - multiproxy target cluster
6. A user-scoped `IClient` is obtained from `TClientCache`.
7. A `TCommandContext` is created.
8. The command callback is scheduled via `Connection_->GetInvoker()`.
9. `DoExecute` wraps the command in tracing, logging, exception-to-`TError` conversion, and request cleanup.
10. `TCommandContext::Finish()` resets request-owned resources.

## `TCommandContext`

`TCommandContext` is the bridge between generic driver requests and typed command implementations.

It provides:

- `GetConfig()`
- `GetClient()`
- `GetRootClient()`
- `GetInternalClientOrThrow()`
- `GetDriver()`
- `Request()`
- `GetInputFormat()`
- `GetOutputFormat()`
- `ConsumeInputValue()`
- `ProduceOutputValue()`

### Format handling

Structured, tabular, and binary I/O are mediated through `NFormats`.

`ConsumeInputValue()`:

- reads from `InputStream`
- picks a producer based on `input_format` and the command descriptor `InputType`
- converts the incoming data into `TYsonString`

`ProduceOutputValue()`:

- takes a YSON value
- picks a consumer based on `output_format` and descriptor `OutputType`
- serializes to the output stream

### Important invariant

If a command descriptor says `InputType != Null`, the request must supply `InputStream`.
If a command descriptor says `OutputType != Null`, the request must supply `OutputStream`.

## Command implementation model

### `ICommand`

Each command implements:

- `void Execute(ICommandContextPtr context)`

### `TCommandBase`

Most commands derive from `TCommandBase` or `TTypedCommand<TOptions>`.

`TCommandBase::Execute` does the common work:

- adds request logging tags
- deserializes `request.Parameters` into the command object
- initializes empty response parameters for commands that do not provide them explicitly
- calls `DoExecute(context)`

### `TTypedCommand<TOptions>`

This is the main composition point.

It combines multiple mixin bases that auto-register common parameters when `TOptions` inherits specific option families.

Those mixins are defined in `command.h` and `command-inl.h`.

## Common option mixins

These mixins expose parameter names automatically when the command option type supports them.

### Transactional options

Exposed when `TOptions` is convertible to `NApi::TTransactionalOptions`:

- `transaction_id`
- `ping`
- `ping_ancestor_transactions`
- `suppress_transaction_coordinator_sync`
- `suppress_upstream_sync`
- `suppress_strongly_ordered_transaction_barrier`

### Mutating options

Exposed when `TOptions` is convertible to `NApi::TMutatingOptions`:

- `mutation_id`
- `retry`

### Master read options

Exposed when `TOptions` is convertible to `NApi::TMasterReadOptions`:

- `read_from`
- `disable_per_user_cache`
- `expire_after_successful_update_time`
- `expire_after_failed_update_time`
- `success_staleness_bound`
- `cache_sticky_group_size`

### Tablet read options

Exposed when `TOptions` is convertible to `NApi::TTabletReadOptions`:

- `read_from`
- `rpc_hedging_delay`
- `timestamp`
- `retention_timestamp`

### Suppressable access-tracking options

Exposed when `TOptions` is convertible to `NApi::TSuppressableAccessTrackingOptions`:

- `suppress_access_tracking`
- `suppress_modification_tracking`
- `suppress_expiration_timeout_renewal`

### Prerequisite options

Exposed when `TOptions` is convertible to `NApi::TPrerequisiteOptions`:

- `prerequisite_transaction_ids`
- `prerequisite_revisions`

### Timeout options

Exposed when `TOptions` is convertible to `NApi::TTimeoutOptions`:

- `timeout`

### Tablet transaction helpers

Tablet read/write helpers expose:

- `transaction_id`
- `atomicity`
- `durability`

These are separate from generic master transaction attachment and are used by row-oriented tablet commands.

### Select-rows options

Exposed when `TOptions` supports `NApi::TSelectRowsOptionsBase`:

- `range_expansion_limit`
- `max_subqueries`
- `min_row_count_per_subquery`
- `udf_registry_path`
- `verbose_logging`
- `new_range_inference`
- `syntax_version`
- `expression_builder_version`

## Transaction handling

There are two distinct transaction patterns.

### Master transaction attachment

`TTransactionalCommandBase::AttachTransaction`:

- requires or optionally uses `transaction_id`
- uses the driver's sticky transaction pool
- reuses cached transactions when possible
- renews leases when retrieving existing transactions
- falls back to `client->AttachTransaction(...)` for master transactions not present in the pool

### Tablet transaction handling

`TTabletWriteCommandBase`:

- reuses an existing tablet transaction when `transaction_id` is provided
- otherwise starts a new tablet transaction automatically
- commits only when the transaction was created implicitly by the command

### Important invariant

When changing row-level commands, be careful not to break the distinction between:

- user-supplied transaction lifetime
- driver-created per-command tablet transactions

## Output conventions and API versioning

The driver currently supports API versions `3` and `4`.

Configured via `TDriverConfig::ApiVersion`.

### Empty output

- v3: nothing is written
- v4: an empty map is written

### Single-value output

Helper functions change shape by API version:

- v3: bare value
- v4: `{name = value}` map

Examples:

- `get_version`
  - v3 returns a bare value
  - v4 returns `{version = ...}`
- `parse_ypath`
  - v3 returns bare parsed YPath representation
  - v4 returns `{path = ...}`

### Response parameters side-channel

Some streaming commands emit structured metadata through `ResponseParametersConsumer` before writing the main body stream.

Examples include:

- `read_file`
  - emits `id`, `revision`
- `read_table`
  - emits row-count and omission metadata
- `select_rows`
  - can emit query statistics
- `lookup_rows`
  - can emit unavailable-key metadata
- `pull_rows`
  - emits replication progress metadata
- `get_job_stderr`
  - emits size/offset metadata

This side-channel is essential for HTTP proxy style consumers that need headers/metadata before starting a streamed body.

## Batch execution

`execute_batch` is implemented in `etc_commands.cpp`.

It executes a vector of subrequests concurrently with bounded concurrency.

### Batch request shape

Each subrequest contains:

- `command`
- `parameters`
- `input`

### Batch restrictions

A command can participate only if:

- input type is `Null`, `Structured`, or `Tabular`
- output type is `Null` or `Structured`

Commands with binary output or binary input are intentionally excluded.

### Batch mutation handling

For volatile commands inside a batch:

- the batch command generates a base mutation id
- each subrequest gets a derived mutation id
- the `retry` flag is propagated

### Structured batch input caveat

For structured commands, batch execution may inject `input_format = yson` and serialize the `input` node itself, depending on `expect_structured_input_in_structured_batch_commands`.

That compatibility behavior matters when changing structured-input commands.

## Authentication model

Per-request authentication is derived from request fields and converted into `TClientOptions`.

Supported request-level auth inputs:

- effective user name
- user token
- service ticket
- user tag
- user remote address

The driver config can require passwords for authentication-related commands.

This is surfaced both in behavior and in `get_supported_features`.

## Caching and auxiliary facilities

### Client cache

The driver maintains a `TClientCache` keyed by authentication identity and client options.

Purpose:

- avoid recreating clients for repeated requests
- preserve subsystem connection reuse

### Sticky transaction pool

The sticky transaction pool stores attached transactions and renews their leases.

It is critical for:

- transaction commands
- tablet read/write operations with explicit transaction ids
- correct transaction reuse across requests

### Proxy discovery cache

`discover_proxies` uses `IProxyDiscoveryCache`, keyed by:

- proxy kind
- role
- address type
- network name
- ignore-balancers flag

### Trace/log integration

The driver creates a top-level `Driver` trace context and then a child span `Driver:<command_name>` for each command.

It annotates the trace context with:

- `user`
- `request_id`

## Driver config

`TDriverConfig` controls runtime behavior.

Key fields:

- reader/writer configs for file, table, journal, fetcher, chunk fragments
- `api_version`
- `read_buffer_row_count`
- `read_buffer_size`
- `write_buffer_size`
- `client_cache`
- `token`
- `multiproxy_target_cluster`
- `proxy_discovery_cache`
- `default_rpc_proxy_address_type`
- `enable_internal_commands`
- `expect_structured_input_in_structured_batch_commands`
- `require_password_in_authentication_commands`

Defaults are registered in `config.cpp`.

## Internal commands

Some commands are gated by `EnableInternalCommands`.

They are not always exposed and should be treated as privileged/internal surface.

Current internal-only group includes commands such as:

- `read_hunks`
- `write_hunks`
- `lock_hunk_store`
- `unlock_hunk_store`
- `get_connection_config`
- lease-management commands
- connection orchid accessors

## How command families are organized

Command implementations are split primarily by subsystem:

- `transaction_commands.*`
- `cypress_commands.*`
- `file_commands.*`
- `table_commands.*`
- `journal_commands.*`
- `scheduler_commands.*`
- `etc_commands.*`
- `admin_commands.*`
- `authentication_commands.*`
- `queue_commands.*`
- `query_commands.*`
- `flow_commands.*`
- `shuffle_commands.*`
- `distributed_table_commands.*`
- `distributed_file_commands.*`
- `ban_commands.*`
- `bundle_controller_commands.*`
- `chaos_commands.*`
- `internal_commands.*`

The grouping is logical, not architectural isolation: all families still register centrally in `driver.cpp`.

## Safe modification guide for AI agents

### To add a new command

1. Add command class declaration to the correct `*_commands.h` file.
2. Implement `Register` and `DoExecute` in the matching `.cpp` file.
3. Register it in `driver.cpp` with correct:
   - command string
   - input/output data types
   - volatility
   - heaviness
   - API version gating
4. Decide whether it needs:
   - response parameters
   - structured input/output formatting
   - transaction mixins
   - timeout/mutating/prerequisite mixins
5. Ensure the request parameter names are stable and explicit.

### To change an existing command

Always check all of the following:

- command descriptor in `driver.cpp`
- request parameter registration in `Register`
- response shape differences between v3 and v4
- whether the command participates in `execute_batch`
- whether the command is streamed and uses response parameters
- whether it relies on sticky transaction pool semantics

### High-risk areas

- changing `InputType` or `OutputType`
- changing command names
- changing API-v3/v4 output shape
- breaking response-parameter timing for streamed commands
- changing transaction attach/reuse behavior
- making batch-incompatible changes to structured commands

## Architectural summary

The driver is a thin but critical adaptation layer:

- it is thin because most business logic delegates to `NApi::IClient`
- it is critical because it defines the external command contract, request parsing rules, compatibility behavior, and streaming semantics

For development, the most authoritative files are:

- `driver.cpp` for what is exposed
- `command.h` and `command-inl.h` for common request semantics
- the relevant `*_commands.*` pair for command-specific behavior
