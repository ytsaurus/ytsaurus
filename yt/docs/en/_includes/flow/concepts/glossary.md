# Glossary of {{product-name}} Flow

## Introduction {#introduction}

This article collects all the key concepts of {{product-name}} Flow: from the basic data processing model to the architecture and mechanisms for ensuring correctness. The material is structured from simple to complex: first, stream processing is covered, followed by parallelism, external connections, system design, and pipeline management.

When you’re getting familiar with the system, read the article from start to finish, following the links for details. Later, this page will serve as a reference: other documentation sections often link here to remind you of term definitions.

## Basic model {#basic-model}

To understand Flow as a stream processing system, you can use an analogy with a factory assembly line:

- [Pipeline](#pipeline) — the entire factory that solves a specific business task.
- [Stream](#stream) — the conveyor belt that carries data.
- [Message](#message) — an individual item on the belt.
- [Computation](#computation) — a workstation that takes items from one or several belts, processes them, and places the results on other belts.

### Pipeline {#pipeline}

This is a specific business task that is being executed. It can contain many different data grouping and processing nodes.

From the {{product-name}} perspective, a pipeline is represented as a Cypress object of type `pipeline`. For details about the object structure, creation methods, and management, see the [Pipeline Object](../../../flow/concepts/pipeline-object.md) section.

{% if audience == "internal" %}

{% cut "Difference from BigRT" %}

This highlights the main difference from the [BigRT](https://docs.yandex-team.ru/big_rt/) framework. In BigRT, the largest entity is `ConsumingSystem`, which describes just a single data transformation. So, `ConsumingSystem` in BigRT is analogous to a single [computation](#computation) in Flow. A pipeline, however, is a higher-level entity and can contain dozens of different [computations](#computation) along with all the streams between them.

{% endcut %}

{% endif %}

### Stream {#stream} {#stream-and-computation}

A stream is a schematized, named data flow that connects [computations](#computation) in the pipeline graph. Each stream has a fixed schema (a set of typed fields) and is identified by `stream_id`. A stream consists of [messages](#message). Each stream has exactly one writer — a [computation](#computation) that outputs data to it — and an arbitrary number of readers.

The order of messages in a stream is not guaranteed in general. However, for derived messages with matching [keys](#key) along the entire [lineage](#lineage) chain, there are guarantees of relative order. For more details, see the [Message Processing Order](../../../flow/concepts/ordering.md) section.

### Computation {#computation}

A computation is a node in the pipeline graph that performs a specific data transformation. It reads [messages](#message) from input [streams](#stream), processes them, and writes the results to output [streams](#stream). It can have multiple inputs and multiple outputs. The input stream is split into [partitions](#partition) for parallel processing.

For more details, see the [Computation](../../../flow/concepts/computation.md) section.

### Passthrough Computation {#passthrough}

This is a type of [computation](#computation) that doesn’t contain any user business logic: incoming messages are converted to the output [stream](#stream) schema and passed on unchanged. It’s used for simple schema alignment between streams.

For more details, see [Computation](../../../flow/concepts/computation.md#passthrough).

### Swift {#swift}

This is a data processing principle in Flow where the result of a [computation](#computation) isn’t stored in {{product-name}}. Instead, the transformation function must be strictly deterministic — if needed, the result is recomputed. This reduces the load on {{product-name}} while maintaining [exactly-once](#exactly-once) guarantees.

For more details, see the [Swift](../../../flow/concepts/swift.md) section.

### Message {#message}

A single message within a stream.

In the context of a single [computation](#computation), there are several types of messages:
- `input` — messages from other [computations](#computation); essentially, they’re the `output` of another [computation](#computation), but with a grouped schema.
- `source` — messages from internal [sources](#source).
- `timer` — special internal [timer](#timer) messages.
- `output` — the result of a [computation](#computation)’s work, which becomes public and can go to an internal [sink](#sink) and to other [computations](#computation).

### Key {#key}

A set of [message](#message) field values defined via `group_by_schema` in the [spec](../../../flow/concepts/spec.md) of a [computation](#computation). All messages with the same key are routed to a single [partition](#partition) — each partition is assigned a key range `[LowerKey; UpperKey)`. The key is the isolation unit for [state](#state), [timers](#timer), and [ordering guarantees](../../../flow/concepts/ordering.md#ordering-guarantees).

The concept of a key depends on the message type:
- For `input` messages, the key is defined via `group_by_schema`.
- For `source` messages, the key is the source’s partition key.
- For `output` messages, there is no key.

### Lineage {#lineage}

The collection of input [messages](#message) and [timers](#timer) from which specific output results of a [computation](#computation) were derived. The framework uses lineage to compute system output messages and to ensure [ordering guarantees](../../../flow/concepts/ordering.md#ordering-guarantees) for derived messages.

For more details, see the [Lineage](../../../flow/concepts/lineage.md) section.

## Timer {#timer}

A delayed-call mechanism tied to a specific [grouping key](#key) within `TTransformComputation`. A timer lets a [computation](#computation) say, “Wake me up when time X arrives.”

Each timer contains two timestamp fields:
- `TriggerTimestamp` — the trigger moment. When the [EventWatermark](../../../flow/concepts/glossary.md#timestamps-and-watermarks) (or another configured watermark) exceeds this value, the timer is passed to the computation for processing.
- `EventTimestamp` — the business time of the original event, “held” in the timer.

Timers are reliably stored in {{product-name}}. The result of timer processing is recorded in the same transaction in which the timer is removed — this ensures [exactly-once](#exactly-once) guarantees.

{% note warning %}

In the current implementation, all active timers are additionally stored in the process memory. With a large number of timers, this can lead to `Out of Memory` errors when jobs start.

{% endnote %}

For more details, see the [Timers](../../../flow/concepts/timers.md) section.

## Resource {#resource}

Resources exist to describe data shared across multiple [jobs](#job). They can be clients to {{product-name}} and other systems, caches for calls to external systems, machine learning models, and so on.

Currently, resources can only be static (that is, unchangeable without a restart).

## Distributed Throttler {#distributed-throttler}

In Flow, you can create named distributed throttlers that are shared across all [jobs](#job) in a pipeline. They’re used to limit the load on external APIs, balance throughput between [partitions](#partition) of a single [computation](#computation), or slow down reading from sources. They’re implemented as a [token bucket](https://en.wikipedia.org/wiki/Token_bucket) on the controller; jobs request a quota before processing messages or manually from user code. For more details, see the [Distributed Throttler](../../../flow/concepts/distributed_throttler.md) section.

## State {#state}

Persistent data associated with a specific [grouping key](#key) within a [computation](#computation). It’s stored in {{product-name}} dynamic tables and updated atomically when an [epoch](#epoch) is committed. An empty state value corresponds to the absence of a row in the table.

For more details, see the [Stateful Processing](../../../flow/concepts/stateful.md) section.

## Parallelism and execution {#parallelism-and-execution}

To ensure scalability of the data processing workflow, Flow splits each [computation](#computation) into multiple [partitions](#partition), which are processed in parallel within [jobs](#job).

### Partition {#partition}

The input stream to a specific [computation](#computation) can be quite large, so for parallel data processing, the [computation](#computation) is split into multiple partitions. Approximate stream limits per partition: no more than 1 MB/s and no more than 1000 messages per second.

Thanks to the `group_by` option, the input stream to a [computation](#computation) is grouped, and data with the same [key](#key) will always go to a specific partition. For this, each partition is assigned a key range `[LowerKey; UpperKey)`.

### Job {#job}

A specific run of a handler for a specific [partition](#partition) on a [worker](#worker) selected by the [controller](#controller).

### Epoch {#epoch}

A job processes data in epochs to regularly and reliably save the processing progress. The epoch mechanism helps maintain [exactly-once](#exactly-once) guarantees with a reasonable load on {{product-name}}, and also to minimize downtime when recovering from failures.

Some epochs may not contain commits to {{product-name}}. Some [computation](#computation) implementations may not make any commits at all.

### Layout {#layout}

A list of all [jobs](#job) and [partitions](#partition) in the system. In other words, it’s a description of what’s running where right now.

## External connections {#external-connections}

Flow interacts with external systems (queues, tables, etc.) via [connectors](#connector). Each connector provides a [source](#source) for reading and/or a [sink](#sink) for writing.

### Connector {#connector}

A component that connects the pipeline to an external system by reading messages from the connector’s [source](#source) and/or writing messages to the connector’s [sink](#sink). A connector can include both a [source](#source) and a [sink](#sink) (for example, in the case of [QYT](../../../flow/connectors/queue.md)) or only a [source](#source) (for example, in the case of [static table](../../../flow/connectors/static-table.md)).

In code and specs, sources and sinks are described directly. The connector itself doesn’t exist as an entity; it’s more of a logical grouping element for sources and sinks by the external system that’s being interacted with.

For more details about connectors, see the [Connectors Documentation](../../../flow/connectors/about.md).

### Source {#source}

A component that lets a [computation](#computation) read messages from an external system. It’s implemented as a separate class and configured in the [computation](#computation) spec.

### Sink {#sink}

A component that lets a [computation](#computation) write messages to an external system. It’s implemented as a separate class and configured in the [computation](#computation) spec.

## System architecture {#system-architecture}

A running pipeline consists of a set of processes with two roles — [controller](#controller) and [worker](#worker). The controller manages work distribution, and the workers directly process the data.

### Controller {#controller}

Manages the entire pipeline. It assigns [jobs](#job) to [workers](#worker), provides an API for pipeline management, is responsible for synchronizing all settings, monitors the status of all [workers](#worker), and collects their statuses. It can be run in multiple instances (recommended: 2–3); then, one instance will be the leader, and the others will wait in stand-by mode to ensure fault tolerance.

Specs for execution are provided to the controller via a special API.

#### Computation Controller {#computation-controller}

The controller for a specific [computation](#computation): it’s responsible for creating the required number of [partitions](#partition) with the necessary settings. Don’t confuse it with the plain [controller](#controller). Computation Controller is a class, while the controller is a program that manages the cluster operation and creates the necessary Computation Controllers.

#### JobManager {#job-manager}

A module responsible for creating and distributing [jobs](#job) across [workers](#worker).

#### LeaseManager {#lease-manager}

A module responsible for creating and pinging `Lease` — master transactions that jobs use during commits as `prerequisite_transaction_ids`. They help ensure [exactly-once](#exactly-once) guarantees.

### Worker {#worker}

One of the two main types of processes in the system, performing the actual computations within the [jobs](#job) assigned to it. It regularly sends [heartbeats](#heartbeat) to the [controller](#controller), reporting the statuses of all jobs. In return, it receives the current system settings, in particular the [Layout](#layout).

#### Heartbeat {#heartbeat}

A periodic message a [worker](#worker) sends to the [controller](#controller) reporting the statuses of all its [jobs](#job). The controller's response carries the current system settings, in particular the [Layout](#layout); a worker that stops sending heartbeats is treated as unavailable.

#### Message Distributor {#message-distributor}

A [worker](#worker) module responsible for distributing messages between different [jobs](#job). It runs based on the system’s current [layout](#layout). It sends a message until it receives confirmation from the recipient about processing, including the fact of a reliable commit of the processing result (referred to in the codebase as `MarkPersisted`).

#### Resource Manager {#resource-manager}

A [worker](#worker) component that creates [resources](#resource) for shared use. For example, several [computations](#computation) can use the same binary database or the same {{product-name}} client.

#### BufferStateManager {#buffer-state-manager}

A component responsible for creating and maintaining the current sizes of all (or almost all) buffers in the system. Each job has buffers for incoming and outgoing messages. The task of `BufferStateManager` is to ensure the buffer sizes necessary for uninterrupted operation.

### Companion {#companion}

A separate process launched on the same host as the [worker](#worker) and executing user code in [Python](../../../flow/python/getting-started.md), [Java or Kotlin](../../../flow/java/getting-started.md). It interacts with the [worker](#worker) via gRPC. It lets you implement [computations](#computation) in languages other than C++.

For more details, see the [Companion](../../../flow/concepts/companion.md) section.

## Timestamps and watermarks {#timestamps-and-watermarks}

Correct time-based event processing is one of the key tasks of a streaming system. Flow tracks three types of timestamps for each message and maintains a watermark mechanism to determine processing progress.

### EventTimestamp, SystemTimestamp, AlignmentTimestamp, StabilizedEventTimestamp, and Watermarks

For each message in Flow, the following timestamps can be used:
- [EventTimestamp](../../../flow/concepts/watermarks#eventtimestamp) — the true time of the event associated with the message.
- [SystemTimestamp](../../../flow/concepts/watermarks.md#systemtimestamp) — the time when the specific message was created. For messages generated inside Flow, `SystemTimestamp` takes the “{{product-name}} time”. For `source` streams, it’s the time when the message appeared in the remote system.
- [AlignmentTimestamp](../../../flow/concepts/ordering.md#alignment-timestamp) — a timestamp used to align the processing progress of partitions. It’s computed automatically.
- [StabilizedEventTimestamp](../../../flow/concepts/ordering.md#stabilized-event-timestamp) — a timestamp computed based on `AlignmentTimestamp`; it’s used as a non-decreasing approximation of `EventTimestamp` across the message stream (with a matching set of [keys](#key) in the [lineage](#lineage)).

The first three timestamps are stored in the message itself, while `StabilizedEventTimestamp` is computed on the fly when needed.

[Watermark](../../../flow/concepts/watermarks.md) is a timestamp indicating that the system no longer expects older events to arrive. Each stream in the system maintains current `SystemWatermark` and `EventWatermark` values.

{% note info %}

Unfortunately, typical data-writing protocols for queues can’t guarantee that a yesterday’s event won’t be written to the queue. Therefore, for `SystemWatermark` on internal streams, you can say it’s absolute, while `EventWatermark` is only heuristic and approximate.

{% endnote %}

## Ensuring Exactly Once {#exactly-once}

By default, Flow ensures exactly-once semantics for event processing. The mechanism is based on barrier Lease transactions, deduplication of input messages by `message_id` (for internal streams) and by offsets (for sources), and the atomicity of output data via [epoch](../../../flow/concepts/glossary.md#epoch) transactions. If needed, you can relax the semantics to at-least-once or at-most-once.

For more details, see the [Processing Guarantees](../../../flow/concepts/guarantees.md) section.

## Message deduplication and message processing order {#deduplication}

{% if audience == "internal" %}Unlike BigRT, the main{% else %}The main{% endif %} way to deduplicate messages is by `message_id`, not by offset. The offset-based mechanism is used only when working with queues. Also, Flow doesn’t provide any guarantee about the order of event processing.

## Manage the pipeline {#manage-pipeline}

This section describes the pipeline lifecycle operations: from configuring it via specs to release, migration, and running in different environments.

### Spec and DynamicSpec {#spec-and-dynamic-spec}

Any pipeline running on Flow is a combination of two YSON configs called `Spec` and `DynamicSpec`.

- `Spec` — defines the pipeline’s static properties, its topology, node properties, connections between them, object types, etc. You can change the static spec only if the pipeline is stopped.
- `DynamicSpec` — defines the pipeline’s dynamic properties.

Accordingly, the specs contain the properties of all system components, both from the user layer (for example, the list of [computations](#computation) and the [stream](#stream) schemas between them) and from the system-level settings (compression codecs in system tables, buffer sizes, etc.).

### Pipeline release {#release-pipeline}

Updating the pipeline, which includes changing its executable files.

### Update pipeline specs {#update-pipeline-specs}

Update the [static](*static_spec_upd) and/or dynamic [spec](../../../flow/concepts/spec.md) of the pipeline.

### Start, stop, and pause the pipeline {#start-stop-pause-pipeline}

Change the pipeline’s target state and transition the pipeline to that state. The existing states are:

- `unknown` — the pipeline hasn’t started yet.
- `working` — the pipeline is running, and messages are being processed.
- `stopped` — the pipeline is stopped, and all messages have been fully processed.
- `paused` — the pipeline is paused (jobs are stopped, and intermediate messages might not be fully processed).
- `draining` — a transitional state: the pipeline is in the process of stopping.
- `pausing` — a transitional state: the pipeline is in the process of pausing.
- `completed` — a final state: all pipeline sources were [finite](../../../flow/python/testing.md) (`finite = true`), and all messages from them have been processed. You can’t exit this state — you’ll need to recreate the pipeline. This state most often appears in [integration tests](../../../flow/python/testing.md), and it can also occur in production pipelines if the deployment steps are executed in the wrong order (for example, if a source was mistakenly marked as finite) or if an incorrect spec is set.

### Internal pipeline tables {#inner-pipeline-tables}

Service tables that are placed in the pipeline directory in {{product-name}}. They are necessary for the pipeline to run (for example, to deduplicate incoming messages, store partition information, etc.).

The full list of internal tables and their purposes are provided in the section [Pipeline Object → Internal tables](../../../flow/concepts/pipeline-object.md#internal_tables).

### User tables {#user-tables}

Tables that users work with. Typically, their content has a product-related meaning, for example:

- Ordered tables — queues from which the user pipeline reads input events or writes output events.
- Sorted tables with entity states (banner/offer/user profiles, etc.) that the user builds while the pipeline is running.

### Migration {#migration}

The process of changing the structure of objects in {{product-name}} — for example, adding new tables, changing the schema of existing tables, or deleting them.

The main reasons to run migrations are:

- Deploying a new pipeline version that isn’t compatible with the current state of objects in {{product-name}}. Migration lets you update the objects to a state that the new pipeline version can work with, without losing data.
- Changing the table type (replicated/standalone/chaos) for performance or availability reasons.

Examples of changes:

#|
|| **Require migration**

- Changing multiple tables (adding new ones or deleting old ones).
- Changing table schemas.
- Changing table types (replicated/standalone/chaos).

| **Do not require migration**

- Changing most table attributes (for example, changing compaction settings, which only affect performance).
- Changing the list of replicas.

||
|#

{% if audience == "internal" %}Most migrations are run by the user by launching the [ensure_heavy scenario]({{yt-sync-docs}}/getting_started#ensure-heavy-scenario) in YtSync.{% endif %}

### Environment {#environment}

The installation where the pipeline runs — testing, pre-production, or production.

It’s a synonym for the term “stage”{% if audience == "internal" %} — this is the name of the similar entity in the [YtSync]({{yt-sync-docs}}/) configuration{% endif %}.


<style>

.yfm table:nth-of-type(2) {
    border: none;
}

</style>


## See also

- [Computation](../../../flow/concepts/computation.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)
- [Watermarks and Timers](../../../flow/concepts/watermarks.md)
- [Stateful processing](../../../flow/concepts/stateful.md)
- [Connectors](../../../flow/connectors/about.md)

[*static_spec_upd]: You can change it only if the pipeline is stopped.
