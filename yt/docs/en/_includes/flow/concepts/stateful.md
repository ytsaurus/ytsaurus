# Stateful processing in {{product-name}} Flow

Use stateful processing to handle events with read-modify-write operations on states stored in {{product-name}}. For example, you can count statistics for incoming events by key: you load the old value, update it, and write it back.

## State access model {#model}

You access state inside a [computation](../../../flow/concepts/glossary.md#stream-and-computation) through three components:

- **State** — user data tied to a [key](../../../flow/concepts/glossary.md#key) and stored in a {{product-name}} dynamic table.
- **State client** — a type-specific object (`Client<TState>`) that the computation creates one for each named state and initializes in `DoInit`. You access the state by key through the client. It can be **read-write** (works with both [internal](#internal-state) and [external](#external-state) states) or **read-only** ([joiner](#external-state-joiner) for external state).
- **State accessor** — what the client returns for a specific key (based on an input message, timer, or explicit key): a representation of the state of the same type `TState`. The accessor behaves like a smart pointer to the state: a read-write accessor lets you read, modify, and clear the state; a read-only accessor lets you only read it.

{% note warning %}

The accessor is valid only within the current [epoch](../../../flow/concepts/glossary.md#epoch). Don't store it in computation fields or reuse it across epochs — you must get the state through the client again in each epoch.

{% endnote %}

## State types {#state-types}

### Internal State {#internal-state}

This is the simplest way to work with state: you don't need to create tables — Flow manages them automatically. Data loads at the start of the [epoch](../../../flow/concepts/glossary.md#epoch) and writes on commit. You get read-write access through the same client you use for external state. The state type can be arbitrary; the only requirement is that it's serializable to YSON (to persist between epochs).

### External State {#external-state}

This is state in a user-managed dynamic table. You create and manage the tables{% if audience == "internal" %} (for example, via [YtSync]({{yt-sync-docs}})){% endif %}. You get read-write access through the same client you use for internal state, but the backend is a **state manager** (`TSimpleExternalStateManager`{% if audience == "internal" %}, `NBigRTExtensions::TProfileManager`{% endif %}). You declare it in the computation spec in the top-level `external_state_managers` section; the implementation resolves via `external_state_manager_class_name`. It supports caching.

### External State Joiner {#external-state-joiner}

You get read-only access to external states via a key-based join — through a **joiner** (`TSimpleExternalStateJoiner`{% if audience == "internal" %}, `NBigRTExtensions::TProfileJoiner`{% endif %}). You declare it in the computation spec in the top-level `external_state_joiners` section (at the same level as `external_state_managers`); the implementation resolves via `external_state_joiner_class_name`. It supports TTL-based caching: loaded states live in the shared [StateCache](#state-cache) and reload from YT only after the TTL expires or the state is evicted from the cache.

{% if audience == "internal" %}

{% note info %}

For frequently updated protobuf profiles, External State and External State Joiner have a specialized extension — [Serializable Profile](../../../yandex-specific/flow/extensions/serializable-profile.md). It provides a pair of `NBigRTExtensions::TProfileManager` (read-write) and `NBigRTExtensions::TProfileJoiner` (read-only), parameterized by the user's profile type, with delta encoding of changes and compression on top of a regular state table. This saves space and traffic compared to writing the full profile on every update.

{% endnote %}

{% endif %}

{% note warning %}

Tables accessed by a read-write state manager must be modified only through it (or when the [pipeline](../../../flow/concepts/glossary.md#pipeline) is [stopped](../../../flow/concepts/glossary.md#start-stop-pause-pipeline)), because it may use caches.

{% endnote %}

{% note warning "One table, one writer" %}

Only one computation should write to an external state table: writes from different [partitions](../../../flow/concepts/glossary.md#partition) and transactions break state consistency. The state manager owns its table for writing: `TSimpleExternalStateManager`{% if audience == "internal" %}, like `NBigRTExtensions::TProfileManager`,{% endif %} declares it as its own. To get read-only access to another computation's state, use an [external state joiner](#external-state-joiner) (`TSimpleExternalStateJoiner`{% if audience == "internal" %}, `NBigRTExtensions::TProfileJoiner`{% endif %}) or send messages to the writer computation — joiners don't lock the table. The spec validation checks write ownership: each state table must have exactly one owner writer, and a pipeline where two managers lock the same table for writing is rejected with the error `State table <path> is claimed for writing by both ...`. Read-only consumers (joiners) don't lock the table for writing, so they can share it with the owner writer.

{% endnote %}

{% note info %}

For any state, an empty value corresponds to the absence of a row in the table. If the state is empty after modification, the corresponding row is deleted. By default, emptiness and state clearing are determined automatically (by comparing to the default value); the state type can override this behavior.

{% endnote %}

## State storage {#storage}

States are stored in {{product-name}} dynamic tables. Here's a simple schema example:

#|
|| **name** | **type** | **sort_order** | **expression** ||
|| `hash` | `uint64` | `ascending` | `farm_hash(my_key)` ||
|| `my_key` | `string` | `ascending` | ||
|| `my_value_1` | `string` | | ||
|| `my_value_2` | `string` | | ||
|#

### group_by_schema consistency {#group-by-schema}

For correctness and performance, we strongly recommend that you use, as the [group_by_schema](../../../flow/concepts/spec.md#computation) for a [computation](../../../flow/concepts/glossary.md#stream-and-computation), the schema of the first key columns of the dynamic table with states (strictly a prefix of the key columns). This ensures that:

- Only one [partition](../../../flow/concepts/glossary.md#partition) handles events for a single key (correctness).
- One partition handles a limited number of tablets (performance).

Here's an example of a `group_by_schema` consistent with the state table schema from the example above:

#|
|| **name** | **type** | **expression** ||
|| `hash` | `uint64` | `farm_hash(my_key)` ||
|| `my_key` | `string` | ||
|#

## StateCache {#state-cache}

Flow provides a shared two-level (uncompressed + compressed) LRU cache for states. Configure it at `/dynamic_spec/job_tracker/state_cache`.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStateCacheSpec.md) %}

## Implementation in different languages

- **C++**: the client `TMutableStateKeyClient<TState>` (read-write) or `TJoinedStateKeyClient<TState>` (read-only) returns the accessor `TStateAccessor<TState>` / `TConstStateAccessor<TState>`; the same key client works with both internal and external states. [Learn more →](../../../flow/cpp/state.md)
- **Java**: YsonStateAccessor, ProtoStateAccessor, ExternalStateAccessor. [Learn more →](../../../flow/java/state.md)
- **Python**: ctx.state(), ctx.external_state(), ctx.proto_state(). [Learn more →](../../../flow/python/state.md)

## See also

- [Working with states (C++)](../../../flow/cpp/state.md)
- [Working with states (Java)](../../../flow/java/state.md)
- [Working with states (Python)](../../../flow/python/state.md)