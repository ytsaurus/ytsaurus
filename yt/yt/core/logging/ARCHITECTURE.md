# YTsaurus Core Logging Architecture

This document describes the architecture of `yt/yt/core/logging`.

It is intended as a practical guide for developers and AI agents that need to read, debug, extend, or safely modify the logging subsystem.

`yt/yt/core/logging` is not a standalone logging library from scratch. It is the YTsaurus integration layer built on top of the lower-level shared logger types from `library/cpp/yt/logging`.

In practice:

- `library/cpp/yt/logging` defines the core logging model:
  - `TLogger`
  - `TLogEvent`
  - `TLoggingCategory`
  - `TLoggingAnchor`
  - log macros such as `YT_LOG_INFO`
- `yt/yt/core/logging` adds the production runtime:
  - configuration
  - log manager singleton
  - asynchronous delivery
  - writers
  - file rotation and compression
  - structured formatting
  - dynamic reconfiguration
  - profiling and suppression features

## Purpose

The subsystem is responsible for turning call-site log macros into durable output streams.

It provides all of the following:

- category lookup and category-level enablement
- anchor registration for per-message suppression and level overrides
- collection of thread/fiber/trace context
- asynchronous event transport from arbitrary threads into a dedicated logging thread
- routing of events to one or more configured writers
- formatting into plain text, JSON, or YSON
- file writer lifecycle management
- disk-space checks, reopening, rotation, and optional compression
- per-writer and per-category rate limiting
- request-scoped suppression
- runtime reconfiguration from static or dynamic configs
- profiling counters for backlog, drops, bytes, and anchor activity

## Main entry points

### Public logging API used by the rest of the codebase

The main include is:

- `log.h`

This re-exports the shared logger API from `library/cpp/yt/logging/logger.h` and injects YT-specific inline helpers from `log-inl.h`.

Common logging call sites use macros such as:

- `YT_LOG_TRACE`
- `YT_LOG_DEBUG`
- `YT_LOG_INFO`
- `YT_LOG_WARNING`
- `YT_LOG_ERROR`
- `YT_LOG_ALERT`
- `YT_LOG_FATAL`
- `YT_LOG_EVENT_WITH_DYNAMIC_ANCHOR`

Structured logging uses:

- `structured_log.h`
- `fluent_log.h`

### Runtime manager

The central runtime object is:

- `TLogManager` in `log_manager.h` / `log_manager.cpp`

`TLogManager` is the concrete `ILogManager` implementation and is exposed as a leaky singleton via `TLogManager::Get()`.

### Configuration

The logging singleton is configurable via:

- `TLogManagerConfig`
- `TLogManagerDynamicConfig`
- `configure_log_manager.cpp`

This connects logging into the YTsaurus reconfigurable singleton infrastructure under the name `logging`.

## File map

### Core runtime

- `log_manager.h` / `log_manager.cpp`
  - Main manager implementation.
  - Queueing, logging thread, config updates, routing, suppression, profiling.

### Public API adapters

- `log.h`
  - Main public include for plain-text logging.
- `log-inl.h`
  - Helper for building messages that include `TError` payloads.
- `log.cpp`
  - Builds `TLoggingContext` from thread, fiber, and trace state.
- `public.h`
  - Public enums and forward declarations for the core logging layer.

### Configuration

- `config.h` / `config.cpp`
  - Writer config schema.
  - rule config schema.
  - manager config schema.
  - config constructors such as default/stderr/file/server presets.

### Formatting

- `formatter.h` / `formatter.cpp`
  - `ILogFormatter`
  - plain-text formatter
  - structured formatter for JSON and YSON

### Writers

- `log_writer.h`
  - writer interfaces
- `log_writer_factory.h`
  - writer factory interface and host interface
- `log_writer_detail.h` / `log_writer_detail.cpp`
  - rate-limiting writer base
  - stream-based writer base
- `stream_log_writer.h` / `stream_log_writer.cpp`
  - simple stream/stderr writer implementation
- `file_log_writer.h` / `file_log_writer.cpp`
  - file writer implementation
  - reopen/rotation/compression/space-check logic
- `stream_output.h` / `stream_output.cpp`
  - buffered stream wrapper used by file output

### Compression and file helpers

- `appendable_compressed_file.*`
- `random_access_gzip.*`
- `zstd_log_codec.*`
- `log_codec.h`

These implement appendable compressed log writing and codec selection for compressed file logs.

### System log events

- `system_log_event_provider.h` / `.cpp`
  - startup messages
  - skipped-message summaries emitted by rate limiting

### Structured logging helpers

- `structured_log.h` / `structured_log-inl.h`
  - macro API for structured key/value events
- `fluent_log.h` / `fluent_log.cpp`
  - fluent builder API and structured log batching

### Logger wrappers

- `serializable_logger.*`
- `logger_owner.*`

These help embed loggers in persistent or Phoenix-serialized objects.

## Architectural layering

The subsystem has three distinct layers.

### Layer 1: shared logging model from `library/cpp/yt/logging`

This lower layer defines the basic data model.

Important types:

- `TLogger`
  - lightweight handle containing a pointer to `ILogManager`, a category, static logger options, and copy-on-write tags/validators
- `TLogEvent`
  - immutable event payload sent to the manager
- `TLoggingCategory`
  - per-category cached enablement state
- `TLoggingAnchor`
  - per-call-site state used for suppression and per-message overrides
- `ILogManager`
  - minimal interface used by `TLogger`

This layer is where the macros live and where the call-site behavior is decided.

### Layer 2: core logging runtime in `yt/yt/core/logging`

This layer implements `ILogManager` via `TLogManager` and owns:

- configuration
- queues
- logging thread
- routing rules
- writers
- formatters
- reconfiguration

### Layer 3: sink implementations

Concrete writers receive fully materialized `TLogEvent`s and decide how to emit them.

The built-in sink types are:

- stderr writer
- file writer

New writer types can be plugged in through `ILogWriterFactory` registration.

## Core data model

## `TLogEvent`

`TLogEvent` is the unit that moves through the runtime.

Important fields include:

- `Category`
- `Level`
- `Family`
  - `PlainText` or `Structured`
- `Essential`
  - bypasses backlog-based dropping
- `MessageKind`
  - `Unstructured` or `Structured`
- `MessageRef`
  - string payload or structured YSON fragment
- `Instant`
- `ThreadId`
- `ThreadName`
- `FiberId`
- `TraceId`
- `RequestId`
- `SourceFile`
- `SourceLine`
- `Anchor`

The `MessageRef` payload is intentionally already built before the event reaches the manager. The manager does not do printf-style formatting.

## `TLoggingCategory`

A category is a cached runtime object keyed by category name.

Important fields:

- `Name`
- `MinPlainTextLevel`
- `CurrentVersion`
- `ActualVersion`
- `StructuredValidationSamplingRate`

The important optimization is `MinPlainTextLevel`.
Plain-text call sites can cheaply check whether a message is definitely disabled before constructing expensive output. Structured events do not rely on the same optimization because their volume is expected to be much lower.

## `TLoggingAnchor`

Anchors are per-call-site state objects.

They exist so the runtime can attach policy to a specific message site rather than just a category.

Important fields:

- `SourceLocation`
- `AnchorMessage`
- `Suppressed`
- `LevelOverride`
- `CurrentVersion`
- `MessageCounter`
- `ByteCounter`

Anchors are the mechanism behind:

- prefix-based message suppression
- prefix-based per-message level overrides
- anchor profiling

Static anchors are typically one leaky anchor object per macro expansion. Dynamic anchors are registered explicitly and updated via `YT_LOG_EVENT_WITH_DYNAMIC_ANCHOR`.

## Log call flow

The end-to-end path for a typical plain-text log call is:

1. A macro such as `YT_LOG_INFO(...)` expands from `library/cpp/yt/logging/logger.h`.
2. The macro obtains the logger instance and the call-site `TLoggingAnchor`.
3. If the anchor is stale, it is registered or refreshed through the manager.
4. The effective level is computed:
   - base level from the macro
   - optional per-anchor level override
5. `logger.IsLevelEnabled(effectiveLevel)` checks:
   - logger-local min level
   - category cached min level
6. If enabled, `GetLoggingContext()` gathers:
   - cpu instant
   - thread id/name
   - fiber id
   - trace id
   - request id
   - trace logging tag
7. The message text is fully formatted at the call site.
8. `LogEventImpl` creates `TLogEvent` and calls `TLogger::Write`.
9. `TLogger::Write` forwards the event to `ILogManager::Enqueue`.
10. `TLogManager` places the event into a per-thread queue or global fallback queue.
11. The dedicated logging thread dequeues events, applies suppression/routing, and writes them via the selected writers.
12. Writers format and flush to their concrete output streams.

## Structured log flow

Structured logging uses the same manager and writer pipeline but a different event-construction path.

The main APIs are:

- `YT_SLOG_EVENT(...)`
- `LogStructuredEventFluently(...)`
- `TStructuredLogBatcher`

In this path:

1. The message payload is built as a YSON map fragment.
2. The event family is `Structured`.
3. The message kind is `Structured`.
4. The selected formatter decides whether the final output is YSON or JSON.

The key invariant is that the structured message payload inside `TLogEvent::MessageRef` is not a complete object. It is usually a map fragment that the structured formatter merges into the final output object.

## Categories, rules, and routing

Routing is config-driven.

## Rule model

`TRuleConfig` defines a mapping from event properties to writer names.

A rule can restrict by:

- included categories
- excluded categories
- minimum level
- maximum level
- message family
  - plain-text vs structured

Each matching rule contributes one or more writer names.
The final writer set for an event is the union of all matching rules.

`TLogManager::GetWriters` caches the resolved writer vector by:

- category name
- level
- family

The cache key is `TLogWriterCacheKey`.

## Category updates

When a category is first requested or when config changes, `DoUpdateCategory` recomputes:

- minimum enabled plain-text level across all applicable rules
- structured validation sampling rate
- current version

This is why category lookup is not just a name registry; it is also a configuration cache.

## Anchor updates

When an anchor is registered or refreshed, `DoUpdateAnchor` applies two config-driven policies based on anchor-message prefix matching:

- `SuppressedMessages`
  - if any prefix matches, the anchor becomes suppressed
- `MessageLevelOverrides`
  - if a prefix matches, the anchor gains a replacement log level

The anchor message is typically derived from the static log format string up to the first `(`, or from file/line if no message string exists.

This means suppression/override policy is intentionally attached to the stable primary message text, not to dynamic arguments.

## Configuration model

## Writer config hierarchy

`TLogWriterConfig` is the common base for all writers.

Important fields:

- `Type`
- `Format`
  - `PlainText`, `Json`, or `Yson`
- `RateLimit`
- `EnableSystemMessages`
- `SystemMessageFamily`
- `EnableSourceLocation`
- `EnableSystemFields`
- `EnableHostField`
- `CommonFields`
- `JsonFormat`

Two built-in derived configs exist:

- `TFileLogWriterConfig`
- `TStderrLogWriterConfig`

## File writer config

`TFileLogWriterConfig` adds:

- `FileName`
- `UseTimestampSuffix`
- `UseLogrotateCompatibleTimestampSuffix`
- `EnableCompression`
- `EnableNoReuse`
- `CompressionMethod`
  - `Gzip` or `Zstd`
- `CompressionLevel`
- `RotationPolicy`

`TRotationPolicyConfig` defines:

- `MaxTotalSizeToKeep`
- `MaxSegmentCountToKeep`
- `MaxSegmentSize`
- `RotationPeriod`

## Manager config

`TLogManagerConfig` controls the runtime.

Important fields:

- `FlushPeriod`
- `WatchPeriod`
- `CheckSpacePeriod`
- `RotationCheckPeriod`
- `MinDiskSpace`
- `HighBacklogWatermark`
- `LowBacklogWatermark`
- `ShutdownGraceTimeout`
- `ShutdownBusyTimeout`
- `Rules`
- `Writers`
- `CategoryRateLimits`
- `SuppressedMessages`
- `MessageLevelOverrides`
- `RequestSuppressionTimeout`
- `EnableAnchorProfiling`
- `MinLoggedMessageRateToProfile`
- `AbortOnAlert`
- `StructuredValidationSamplingRate`
- `CompressionThreadCount`

## Dynamic config

`TLogManagerDynamicConfig` can override a subset of manager settings at runtime.

`ApplyDynamic` merges dynamic values into a cloned static config and returns a full `TLogManagerConfig`.

Dynamic config can change:

- backlog limits
- routing rules
- category rate limits
- suppression settings
- request suppression timeout
- anchor profiling flags
- abort-on-alert
- structured validation sampling
- compression thread count

Writer definitions themselves are not dynamically rebuilt through `TLogManagerDynamicConfig`; they come from the base `TLogManagerConfig`.

## Built-in config constructors

`config.cpp` provides several convenience constructors, including:

- `CreateStderrLogger`
- `CreateLogFile`
- `CreateDefault`
- `CreateQuiet`
- `CreateSilent`
- `CreateYTServer`
- `CreateFromFile`
- `CreateFromNode`
- `TryCreateFromEnv`

`CreateYTServer` is especially important in server code. It generates multiple file writers like:

- main info log
- debug log
- error log
- optional structured logs bound to specific categories

`TryCreateFromEnv` supports emergency stderr logging via environment variables such as log level and include/exclude category filters.

## `TLogManager` internals

`TLogManager` is a small wrapper around `TLogManager::TImpl`.

`TImpl` owns all mutable runtime state.

## Initialization

On initialization, the manager:

1. registers built-in writer factories for `file` and `stderr`
2. tries to configure itself from environment variables
3. otherwise applies `CreateDefault()`
4. creates the special `Logging` system category

The `Logging` category is reserved for messages generated by the logging subsystem itself.
These messages always go to a dedicated stderr system writer rather than through normal rule routing.

## Logging thread model

The runtime is asynchronous.

`TImpl` owns:

- a dedicated logging thread
- an event queue invoker queue used by periodic executors
- periodic executors for:
  - dequeue
  - flush
  - file watch
  - disk-space checks
  - file rotation
  - disk profiling
  - anchor profiling
- a compression thread pool used by compressed file writers

The logging thread is started lazily on first use.

## Event transport model

A key design point is low-overhead logging from many producer threads.

### Per-thread queues

Each producer thread lazily allocates a thread-local SPSC queue:

- producer side: the calling thread
- consumer side: the logging thread

These queues are registered with the manager through `RegisteredLocalQueues_`.
When a thread exits, a thread-local reclaimer marks its queue as unregistered and the manager eventually deletes it when empty.

### Global fallback queue

If a thread-local queue is already destroyed, events are pushed into a global MPSC stack instead.
This avoids use-after-free during late thread teardown.

### Time ordering

The logging thread periodically calls `OnDequeue()`.
It:

1. collects newly registered local queues
2. merges front events from all local queues by event instant
3. drains eligible items into `TimeOrderedBuffer_`
4. drains the global queue
5. processes the buffer in time order

This design attempts approximate temporal ordering across threads, but it is not a strict total order.
The code explicitly documents races around backlog checks and global queue ordering.

### Config updates as queue items

Reconfiguration is serialized with log events by enqueuing `TConfigEvent` objects into the same pipeline.

This is a very important invariant:

- config updates and log writes are processed in one ordered stream on the logging thread
- writer rebuilds happen on the logging thread
- no external thread mutates writer state directly

## Backlog protection and dropping behavior

The manager tracks:

- `EnqueuedEvents_`
- `WrittenEvents_`
- `FlushedEvents_`
- `DroppedEvents_`
- backlog fill fraction

Two watermarks control behavior:

- `LowBacklogWatermark`
- `HighBacklogWatermark`

Behavior:

- when backlog reaches the low watermark, an out-of-band dequeue is scheduled
- when backlog reaches the high watermark, logging becomes suspended
- while suspended, non-system and non-essential events are dropped
- once backlog falls below the low watermark, logging resumes

Important exception paths:

- events in the system category are always allowed through
- `Essential` events bypass backlog-based dropping
- fatal events follow a dedicated crash path and do not return normally

## Fatal logging behavior

`TLogManager::Enqueue` treats `ELogLevel::Fatal` specially.

For fatal events it:

1. truncates the message to a bounded size for crash handling
2. enqueues the event for logging
3. invokes `AssertTrapImpl`
4. relies on crash handling and later shutdown behavior

This is intentionally not a normal asynchronous code path.
Fatal logging is meant to terminate the process.

## Request suppression

The manager supports suppression by request id.

Mechanism:

- callers may call `SuppressRequest(requestId)`
- request ids are enqueued into `SuppressedRequestIdQueue_`
- the logging thread moves them into `SuppressedRequestIdSet_` with TTL
- `ProcessTimeOrderedBuffer()` drops events whose `RequestId` is currently suppressed

The timeout is controlled by `RequestSuppressionTimeout`.

A subtle but important detail is that processing waits until events are older than the suppression deadline before deciding whether to suppress them. This avoids races where the suppression request arrives slightly after the events themselves.

## Writers and formatters

## Formatter interface

`ILogFormatter` has two methods:

- `WriteFormatted`
- `WriteLogReopenSeparator`

Built-in implementations:

- `TPlainTextLogFormatter`
- `TStructuredLogFormatter`

## Plain-text formatting

`TPlainTextLogFormatter` delegates event rendering to the shared plain-text event formatter from `library/cpp/yt/logging/plain_text_formatter/formatter.h`.

This formatter writes a single rendered line and inserts an empty line separator on reopen.

## Structured formatting

`TStructuredLogFormatter` outputs either JSON or YSON.

It constructs a final map containing:

- `CommonFields` from writer config
- either:
  - structured payload items directly, or
  - `message` for unstructured input
- optional system fields:
  - `instant`
  - `level`
  - `category`
- optional `host`
- for plain-text-originated events:
  - `fiber_id`
  - `trace_id`
  - optional `source_file`

For YSON output it appends a trailing semicolon so each line is a valid list fragment item.

## Writer interface

`ILogWriter` exposes:

- `Write`
- `Flush`
- `Reload`
- `SetRateLimit`
- `SetCategoryRateLimits`

`IFileLogWriter` adds:

- `GetFileName`
- `CheckSpace`
- `MaybeRotate`

## Rate-limiting writer base

`TRateLimitingLogWriterBase` implements writer-side throttling.

It maintains:

- an optional global per-writer byte rate limit
- optional per-category byte rate limits
- counters for bytes written and skipped events
- current segment size profiling

Behavior:

- limits are evaluated in one-second windows
- when a new interval begins, the writer may emit a synthetic system log event reporting skipped records in the previous second
- system messages for skip summaries are created by `ISystemLogEventProvider`

This means rate limiting is visible in logs rather than silently dropping records without explanation.

## Stream writer base

`TStreamLogWriterBase` is a formatter-backed writer that writes to an `IOutputStream`.

It provides:

- `WriteImpl` via formatter
- `Flush`
- default `Reload` no-op
- default exception handling that aborts the process dramatically

The stderr writer is just a `TStreamLogWriterBase` bound to `Cerr`.

## File writer behavior

`TFileLogWriter` is the main durable sink.

### Open and reload

On `Open()` it:

1. ensures the target directory exists
2. computes file open flags depending on compression mode
3. chooses the final file name, optionally adding a timestamp suffix
4. opens the file
5. creates the output stream:
   - raw buffered file output
   - appendable zstd compressed file
   - random-access gzip file
6. writes reopen separator if the file is non-empty
7. optionally emits a startup system message
8. resets segment-size tracking

`Reload()` simply closes and reopens the file.

### Space checks

`CheckSpace(minSpace)` disables the writer if the directory's available disk space falls below the configured threshold.
When space recovers, the writer reopens itself.

### Rotation

`MaybeRotate()` rotates if either condition holds:

- current file exceeds `MaxSegmentSize`
- `RotationPeriod` elapsed

Rotation then:

- lists matching files in the directory
- keeps only as many as allowed by count and total-size limits
- deletes older excess files
- renames existing segments according to the chosen suffix policy

Supported naming styles:

- numeric suffixes
- timestamp suffix on all files
- logrotate-compatible timestamp suffix on archived files only

### Compression

Compressed file output supports:

- `Zstd`
- `Gzip`

Zstd uses an appendable compressed-file abstraction plus a background compression invoker from the log manager host.
This is why `ILogWriterFactory::CreateWriter` receives an `ILogWriterHost*`.

## System log events

System log events are generated internally by writers, not by normal application code.

They are used for:

- `Logging started`
- `Events skipped`

The event provider chooses plain-text or structured system messages based on writer config.

A useful design detail is that system-message family can differ from the family of normal routed messages. For example, a writer could accept plain-text-originated events but serialize them with a structured formatter.

## Reconfiguration model

Reconfiguration is performed by enqueuing a `TConfigEvent` and processing it on the logging thread.

`DoUpdateConfig` performs the actual rebuild.

It:

1. compares old and new config nodes and returns early if unchanged
2. resolves writer factories by writer `Type`
3. validates each writer config with its factory
4. updates category structured-validation sampling rate
5. clears writer caches and notification watches
6. recreates all writers from config
7. applies writer-level and category-level rate limits
8. recreates inotify watches for file writers where possible
9. updates runtime fields such as:
   - backlog watermarks
   - request suppression enablement
   - abort-on-alert
   - compression thread count
   - periodic executor periods
10. stores the new config and increments version

### Important invariants

- writers are replaced wholesale on config update
- rule-to-writer cache is invalidated on each update
- categories and anchors use versioning so call sites can lazily refresh cached state
- config equality is checked structurally via YSON nodes

## File watching and reopen behavior

The runtime supports two mechanisms for log-file reopen.

### Explicit reopen

`TLogManager::Reopen()` sets a flag.
On the next event write, the logging thread reloads all writers.

### SIGHUP reopen

`EnableReopenOnSighup()` installs a `SIGHUP` handler on Unix that triggers `Reopen()`.

### Inotify-based watching

On Linux, file writers can be watched for self-delete or self-move events.
This supports external rotation tools and similar file replacement patterns.

If a watch fires, the manager reloads that writer and attempts to recreate the watch.

## Profiling and observability

The manager exports sensors for:

- enqueued events
- backlog size
- dropped events
- suppressed events
- message buffer memory

It also tracks per-category/per-level written event counters and writer-local counters such as:

- bytes written
- events skipped by global limit
- events skipped by category limit
- current segment size

Two periodic profiling tasks are especially notable.

### Disk profiling

`OnDiskProfiling()` computes the minimum available and free space across directories used by file writers.

### Anchor profiling

The manager periodically scans registered anchors and can export only those with a sufficient logged message rate when anchor profiling is enabled.

For hot call sites this gives visibility into where logging volume comes from.

## Structured logging helpers

## `YT_SLOG_EVENT`

This macro enforces a compile-time constant primary message.
That is intentional.

The design assumption is that structured logs are aggregated by primary message, while variable data should be moved into explicit structured fields.

Another important guardrail is that the structured key `message` is reserved and cannot be passed as a user tag key.

## Fluent structured logging

`LogStructuredEventFluently(logger, level)` returns a temporary fluent map builder.
When the builder object is destroyed, it emits the completed structured event.

This enables ergonomic code like:

- build map fields fluently
- rely on RAII to flush the event at scope end

The fluent builder also automatically includes structured tags already attached to the logger.

## Structured batching

`TStructuredLogBatcher` accumulates YSON list fragments and flushes them as a structured event with a `batch` field.

This is useful when many similar structured items should be emitted as one log record.

## Shutdown semantics

`Shutdown()` marks shutdown requested and then:

- if called from the logging thread, flushes writers immediately
- otherwise waits until previously enqueued events are flushed or the grace timeout expires
- optionally busy-sleeps for `ShutdownBusyTimeout` to accommodate asynchronous writer flushes
- shuts down the event queue

This is deliberately pragmatic rather than perfectly elegant. The code explicitly notes that asynchronous writer operations are not yet modeled cleanly.

## Boundary with `TLogger`

For modifications, it is important to understand what belongs to `TLogger` versus `TLogManager`.

### `TLogger` responsibilities

- own logger-local min level
- own logger-local essential flag
- own logger-local plain tags and structured tags
- perform cheap enablement checks
- maintain category pointer
- manage static and dynamic anchors via the manager
- construct events and hand them off

### `TLogManager` responsibilities

- own runtime configuration
- own categories and anchors registry
- own queues and logging thread
- own writers and their lifecycle
- enforce suppression, routing, and request suppression
- perform actual delivery and flushing

If a change affects event creation semantics, start with `library/cpp/yt/logging/logger.h` and related inline code.
If a change affects runtime delivery semantics, start with `yt/yt/core/logging/log_manager.cpp`.

## Extension points

## Adding a new writer type

To add a new writer:

1. Define a config type derived from `TLogWriterConfig`.
2. Implement a concrete `ILogWriter`.
3. Implement `ILogWriterFactory` with:
   - `ValidateConfig`
   - `CreateWriter`
4. Register the factory via `TLogManager::RegisterWriterFactory(typeName, factory)`.
5. Ensure config nodes for the new writer type deserialize correctly.
6. Consider whether the writer needs:
   - file-like reload/rotation hooks
   - rate limiting
   - background compression invoker
   - system startup/skipped-message events

If the writer should participate in manager-side file watching or disk checks, it must also implement `IFileLogWriter`.

## Adding a new formatter

A formatter change is lower risk than a new writer type.

To add a new output encoding, you would need to:

- extend `ELogFormat`
- update formatter creation in `TLogManager::CreateFormatter`
- define config semantics for the new format
- decide how system fields and source locations should appear

This is a cross-cutting change because config schema and formatter creation must stay aligned.

## Safe modification guide for AI agents

## Authoritative files to inspect first

When changing behavior, the most authoritative files are:

- `library/cpp/yt/logging/logger.h`
  - macro behavior, event model, categories, anchors
- `yt/yt/core/logging/log_manager.cpp`
  - queueing, routing, suppression, reconfiguration, shutdown
- `yt/yt/core/logging/config.h` / `config.cpp`
  - config schema and defaults
- `yt/yt/core/logging/file_log_writer.cpp`
  - file sink semantics
- `yt/yt/core/logging/formatter.cpp`
  - output shape

## High-risk areas

The following changes are easy to get wrong:

- modifying `TLogEvent` shape
- changing anchor update logic
- changing category min-level calculation
- altering backlog drop rules
- changing config update sequencing
- changing writer exception behavior
- altering request suppression timing
- changing structured formatter field names or semantics
- changing file naming/rotation rules
- changing fatal logging behavior

## Common pitfalls

### Do not confuse family with format

- `ELogFamily` describes semantic origin:
  - plain-text or structured event
- `ELogFormat` describes serialized sink format:
  - plain text, JSON, or YSON

A plain-text event may still be serialized by a structured formatter.

### Do not mutate writers from arbitrary threads

All writer rebuild and most writer lifecycle operations are serialized through the logging thread.
Bypass that and you will likely introduce races.

### Remember that config updates are queued

A call to `Configure(..., sync = false)` does not apply config immediately. It only enqueues it.

### Remember that system logging is special

Messages emitted by the logging subsystem itself use the reserved `Logging` category and a dedicated stderr writer. They do not follow normal rule routing.

### Ordering is approximate, not perfect

The runtime tries to preserve event time order across producer threads, but the code explicitly documents cases where the ordering is not total.

## Practical mental model

A good working model is:

- the macros and `TLogger` decide whether a message is worth constructing
- the manager owns all runtime policy
- events cross an asynchronous queue boundary
- rules map events to writers
- writers apply rate limits and sink-specific lifecycle logic
- formatters decide the final serialized representation

If you keep those boundaries in mind, most changes become straightforward to localize.

## Architectural summary

`yt/yt/core/logging` is a production-grade asynchronous logging runtime layered on top of the shared YT logger model.

Its core responsibilities are:

- turn cheap macro-level logging decisions into durable asynchronous delivery
- centralize routing and suppression policy
- manage sink lifecycle and file-system interactions
- support multiple output formats and structured logging
- remain reconfigurable at runtime without forcing producer threads to coordinate directly with sink state

For almost any debugging or feature work, start by deciding which boundary your change belongs to:

- call-site and event construction: shared `TLogger` layer
- runtime transport and policy: `TLogManager`
- serialization: formatters
- durability and file-system behavior: writers
