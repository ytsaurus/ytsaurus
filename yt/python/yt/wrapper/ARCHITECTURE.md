# Python SDK Architecture: `yt.wrapper`

## Purpose

`yt/python/yt/wrapper` is the high-level Python SDK for YTsaurus.
It exposes a Pythonic interface over YT commands, transport backends, table/file I/O,
transactions, operations, schemas, and Python job packaging.

This document is written for developers and AI agents modifying the SDK.
It focuses on:

- public entry points
- module responsibilities
- request and data flow
- coupling between subsystems
- invariants that should be preserved during refactoring

## High-level architecture

The package is layered.

1. **Public API layer**
   - `__init__.py`
   - `client_api.py`
   - `client.py`
   - generated `client_impl.py`
2. **Client state and configuration**
   - `config.py`
   - `default_config.py`
   - `client_state.py`
   - `client_helpers.py`
3. **Command layer**
   - `cypress_commands.py`
   - `table_commands.py`
   - `file_commands.py`
   - `dynamic_table_commands.py`
   - `operation_commands.py`
   - `run_operation_commands.py`
   - other domain modules imported by `client_api.py`
4. **Transport / driver layer**
   - `driver.py`
   - `http_driver.py`
   - `native_driver.py`
5. **Streaming, retries, and heavy I/O**
   - `heavy_commands.py`
   - `retries.py`
   - parallel read/write helpers
6. **Operations and Python job execution**
   - `spec_builders.py`
   - `prepare_operation.py`
   - `run_operation_commands.py`
   - `py_wrapper.py`
   - `operations_tracker.py`
7. **Core data abstractions**
   - `ypath.py`
   - `format.py`
   - `schema/`
8. **Batch and transaction support**
   - `batch_client.py`
   - `batch_execution.py`
   - `batch_helpers.py`
   - `transaction.py`

A typical user call flows like this:

`public API -> command module -> driver -> http/native backend -> response decoding`

For heavy reads/writes and operations, additional layers are involved:

`public API -> command module -> spec/format/path preparation -> retrier/streaming helper -> driver/backend`

## Public API surface

### `__init__.py`

This is the package aggregation layer. It re-exports:

- `YtClient`
- command functions from `client_api.py`
- path and format classes
- schema helpers
- spec builders
- Python job helpers and decorators
- exception classes

When a symbol appears to be part of the public SDK but its implementation is unclear,
start from `__init__.py`, then jump to `client_api.py` or the imported module.

### `client_api.py`

`client_api.py` is the canonical registry of public function-style APIs.
It imports commands from many modules and exposes them under one namespace.

Practical impact:

- adding a new public command usually requires adding it here
- this file defines the surface mirrored by the object-oriented client
- it is the fastest map of user-visible capabilities in the SDK

### `client.py` and `client_impl.py`

`client.py` exposes `YtClient` and validates compatibility between the function API
and the method API.

`client_impl.py` contains the concrete client methods and is generated.
Each method delegates to the corresponding function in `client_api.py` with
`client=self` injected.

Important invariant:

- **Do not hand-edit `client_impl.py`.**
- If signatures change in public command functions, regenerate the client implementation.

If method and function signatures diverge, import-time checks in `client.py`
are expected to fail.

### `client_helpers.py`

This module contains glue used by the client layer:

- client initialization
- signature comparison helpers
- function-to-method wrapping via `create_class_method`

It is the main bridge between standalone command functions and `YtClient` methods.

## Configuration and runtime state

### `config.py`

`config.py` is both configuration access layer and global SDK runtime holder.
It behaves like a module-level singleton and also carries client-like state.

Responsibilities include:

- loading default config
- exposing `get_config`, `get_option`, `set_option`
- applying environment-derived overrides
- backend detection
- remote patch support

A key behavior is backend selection:

- explicit backend wins
- otherwise proxy-oriented setup implies `http`
- driver config / driver config path implies `native`
- otherwise request execution cannot proceed

### `default_config.py`

This is the source of truth for config structure and default values.
It defines defaults for:

- proxy and transport behavior
- retries and backoff
- read/write settings
- heavy request handling
- transaction settings
- operation tracking
- Python pickling/job packaging behavior
- compatibility toggles such as YAMR-related behavior

When behavior seems surprising, check defaults here before changing command logic.

### `client_state.py`

`ClientState` stores mutable per-client runtime state that should not be mixed into
stateless command logic.

Examples of stored state:

- config object
- command parameters
- cached session and driver objects
- API version / command cache
- token/user cache
- transaction stack
- heavy proxy provider state
- telemetry

Important invariant:

- a client is not just configuration; it also owns transport/runtime caches
- deep-copying clients must preserve semantics while avoiding invalid sharing of live objects

## Command layer

Command modules implement user-facing behavior for each domain.
They usually do four things:

- normalize Python arguments
- validate mutually-dependent options
- translate high-level inputs into request parameters
- call `driver.make_request` or `driver.make_formatted_request`

### `cypress_commands.py`

Implements Cypress tree operations such as:

- `get`, `set`, `list`, `exists`
- `create`, `remove`, `copy`, `move`, `link`
- attribute access and search-like flows

This module is representative of metadata-oriented commands:
light request assembly, moderate path handling, direct dispatch to the driver.

### `table_commands.py`

This is one of the most central and complex modules.
It handles:

- table reads and writes
- schema-aware operations
- raw vs structured formats
- parallel read/write support
- retries for heavy operations
- blob-table and range-oriented flows

This module heavily depends on:

- `ypath.py` for table path normalization
- `format.py` for serialization and parsing
- `heavy_commands.py` for streaming
- retry configuration from config

If you change read/write semantics, inspect this module first and then check all helper layers it uses.

### `operation_commands.py`

Contains lower-level operation lifecycle commands such as:

- abort
- suspend / resume
- complete
- get / list
- state-related helpers

This module is focused on already-created operations, not on building new ones.

### `run_operation_commands.py`

Contains the high-level operation entry points:

- `run_map`
- `run_reduce`
- `run_map_reduce`
- `run_sort`
- `run_merge`
- `run_remote_copy`
- other operation-launching helpers

This module coordinates multiple subsystems:

- spec builders
- path and format preparation
- Python job wrapping
- retries around submission and state polling
- operation tracking

This is the main entry point for understanding how Python callables become executable YT jobs.

## Driver and backend dispatch

### `driver.py`

`driver.py` is the central transport-agnostic request dispatcher.
Core responsibilities:

- select backend type
- prepare common request parameters
- route to HTTP or native driver
- support batch mode
- apply logging / request metadata
- post-process responses

Two functions matter most:

- `make_request`
- `make_formatted_request`

`make_formatted_request` is used when command semantics include a `Format` object or
formatted response handling.

Important invariant:

- command modules should stay mostly backend-agnostic
- backend branching should remain centralized here or in backend-specific drivers

### `http_driver.py`

Implements transport over the HTTP proxy.
It is responsible for:

- HTTP request construction
- session usage
- proxy-specific behavior
- streaming response handling for proxy-based flows

### `native_driver.py`

Implements integration with native/RPC drivers.
Responsibilities include:

- creating/reusing native driver instances
- converting SDK-level parameters into driver requests
- binding to native or RPC behavior through driver configuration
- response conversion and error propagation

This layer is more stateful than plain HTTP transport because it manages driver objects.

## Data model abstractions

### `ypath.py`

`YPath` and related classes represent YT paths together with embedded attributes.
Specializations include `TablePath` and `FilePath`-style usage patterns.

Capabilities include:

- parsing YT path syntax
- storing inline attributes
- path joining and normalization
- handling table ranges / columns / append and similar table-specific options

Important invariant:

- path objects are not just strings
- many command behaviors depend on attributes carried by the path object itself

When debugging path-related bugs, inspect both explicit function arguments and path attributes.

### `format.py`

`format.py` defines the serialization/deserialization abstraction used across table and file commands.
It includes multiple concrete formats such as YSON, JSON, DSV, and Skiff-related variants.

Key responsibilities:

- parse/serialize row streams
- distinguish raw vs structured modes
- control encoding/decoding behavior
- bridge Python objects and wire formats

This module is on the critical path for all data I/O.
Changes here can silently affect many commands.

### `schema/`

The schema package provides typed helpers such as:

- dataclass integration
- table schema representation
- runtime schema context
- Python-type to YT-type conversion utilities

It is the structured-data layer above plain format parsing.

## Heavy I/O, streaming, and retries

### `heavy_commands.py`

This module supports large or long-running transfer flows.
It adds logic that is not appropriate in simple metadata commands:

- progress reporting
- streaming request/response handling
- transaction/lock-aware reads
- write retry support
- interaction with heavy proxies and chunked transfer behavior

If a bug appears only for large tables/files, this module is a likely source.

### `retries.py`

Defines the generic retry framework used in multiple parts of the SDK.
It provides retrier classes with configurable backoff and exception filtering.

Important properties:

- retry behavior is policy-driven from config
- retry users supply operation-specific exception classification
- the framework is reused by both metadata and heavy flows

When changing retry semantics, check all consumers instead of patching one command locally.

## Batch support

### `batch_client.py`

Defines `BatchClient`, a specialized client for accumulating requests.
It intentionally disables some methods that are unsafe or unsupported in batch mode.

### `batch_execution.py`

Defines `BatchExecutor`, which stores tasks, controls batch size/concurrency, and commits batched requests.

### `batch_helpers.py`

Contains convenience functions for creating and using batch execution.

Important invariant:

- not every command is valid in batch mode
- commands participating in batching must cooperate with deferred execution and response objects

## Transactions

### `transaction.py`

Implements transaction lifecycle management for the Python SDK.
Features include:

- context manager usage
- nested transaction handling
- background pinging
- abort/commit logic
- interaction with command parameters and current client state

The transaction object is both control flow and implicit request context.
Many commands rely on current transaction state being propagated through `client` command params.

Important invariant:

- transaction context must remain synchronized with request submission
- background pingers and cleanup logic should not leak when exceptions happen

## Operation preparation and tracking

### `spec_builders.py`

Contains helpers and builder classes for creating operation specs.
It is where many user-friendly arguments are normalized into the final YT operation spec.

Examples of responsibilities:

- reduce/sort/join key preparation
- ACL/spec helper construction
- finalization helpers
- spec assembly conventions

### `py_wrapper.py`

This module is specific to Python job execution.
It handles packaging Python code so it can run inside YT operations.

Responsibilities include:

- wrapping Python callables
- pickling functions and associated state
- module/archive preparation
- command-line construction for job launch
- job decorators such as `raw`, `aggregator`, and context-aware execution helpers

This is one of the highest-risk modules to change because it sits at the boundary between
local Python process semantics and remote execution semantics.

### `operations_tracker.py`

Tracks submitted operations asynchronously.
It provides polling, waiting, and management for multiple operations at once.

This module is important for long-running workflows and orchestration tools.
It sits above lower-level operation commands and below high-level user ergonomics.

## Main dependency structure

A simplified dependency picture:

- `__init__.py` -> `client_api.py`, `client.py`, data/helper modules
- `client.py` -> generated `client_impl.py`, `client_helpers.py`, `client_api.py`
- `client_impl.py` -> `client_api.py`
- command modules -> `driver.py`, `ypath.py`, `format.py`, config helpers, domain helpers
- `driver.py` -> `http_driver.py` or `native_driver.py`
- operation-launch modules -> `spec_builders.py`, `py_wrapper.py`, `operation_commands.py`, trackers
- heavy table/file flows -> `heavy_commands.py`, retry helpers, transport layer
- batch/transaction layers cross-cut many command modules through `client` state and command params

## End-to-end flows

### Metadata command flow

Example: `yt.get("//path")`

1. Public symbol is exported via `__init__.py` / `client_api.py`
2. `cypress_commands.get` normalizes path and options
3. Request is sent through `driver.make_request`
4. Driver chooses HTTP or native backend
5. Backend response is decoded and returned to caller

### Table read flow

Example: `yt.read_table(path, format=...)`

1. `table_commands.py` normalizes `TablePath`, ranges, and format
2. Heavy/streaming helper path may be chosen
3. `driver.make_formatted_request` or heavy read helper dispatches request
4. Backend returns a stream or payload
5. `Format` implementation converts wire data to Python rows/items

### Python operation launch flow

Example: `yt.run_map(func, input_table, output_table, ...)`

1. `run_operation_commands.py` validates arguments
2. `spec_builders.py` assembles the operation spec
3. `py_wrapper.py` packages the Python callable and dependencies
4. Operation is submitted through the driver/command path
5. Resulting operation object may be tracked via `operations_tracker.py`

## Editing guidelines for AI agents

### Safest extension points

These changes are usually local and predictable:

- adding a small command option inside one command module
- extending spec normalization in `spec_builders.py`
- adding support for a new format option in `format.py`
- adding client-facing re-exports in `client_api.py` and `__init__.py`

### High-risk change zones

Be extra careful in these files:

- `client_impl.py` because it is generated
- `driver.py` because backend dispatch is centralized
- `format.py` because many read/write paths depend on it
- `table_commands.py` because it combines many subsystems
- `transaction.py` because context propagation and background pinging are subtle
- `py_wrapper.py` because remote Python job semantics are fragile
- `native_driver.py` because driver lifecycle and bindings are stateful

### Invariants to preserve

- **Public function API and client method API must stay aligned.**
- **Command modules should remain mostly backend-agnostic.**
- **Path objects may carry semantics in attributes, not just strings.**
- **Format objects participate in both request construction and response parsing.**
- **Client state owns runtime caches and current command context.**
- **Batch mode and transaction mode alter execution semantics across many commands.**
- **Operation launch is a pipeline, not a single request.**

### Practical checklist before modifying code

When changing a public command:

- update the implementation module
- update `client_api.py` if the symbol is public
- ensure `YtClient` method compatibility remains valid
- check batch, transaction, and formatted-request behavior

When changing transport behavior:

- inspect `driver.py`
- inspect both `http_driver.py` and `native_driver.py`
- verify effects on retries and heavy commands

When changing table or file I/O:

- inspect `table_commands.py` or `file_commands.py`
- inspect `format.py`
- inspect `ypath.py`
- inspect `heavy_commands.py`
- inspect retry configuration assumptions

When changing operation execution:

- inspect `run_operation_commands.py`
- inspect `spec_builders.py`
- inspect `py_wrapper.py`
- inspect `operation_commands.py` and `operations_tracker.py`

## Suggested reading order for future agents

If you are new to this SDK, read in this order:

1. `client_api.py`
2. `client.py`
3. `client_state.py`
4. `config.py` and `default_config.py`
5. `driver.py`
6. `ypath.py`
7. `format.py`
8. `cypress_commands.py`
9. `table_commands.py`
10. `operation_commands.py`
11. `run_operation_commands.py`
12. `spec_builders.py`
13. `transaction.py`
14. `heavy_commands.py`
15. `py_wrapper.py`
16. `operations_tracker.py`

That order moves from public interface to transport, then into the two most complex verticals:
data I/O and operation execution.

## Summary

`yt.wrapper` is a layered SDK with a broad public API, centralized transport dispatch,
and a set of rich helper abstractions for paths, formats, transactions, batch execution,
and operation launch.

The most important architectural idea is that most command modules are thin orchestration layers,
while the real shared behavior is concentrated in:

- client/config state
- driver dispatch
- path/format abstractions
- retry/streaming helpers
- operation spec and Python job packaging

If you preserve those boundaries, changes remain local and understandable.
If you break them, seemingly small edits can affect large parts of the SDK.
