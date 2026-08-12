# Working with states in {{product-name}} Flow (C++)

{% note info %}

This page describes the specifics of working with states in C++. For a language-agnostic description of the concept, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

{% endnote %}

## Internal State {#internal-state}

This is the simplest way to store a state inside a `Computation`. Data is automatically loaded at the start of the [epoch](../../../flow/concepts/glossary.md#epoch) and written on commit. You don’t need to create tables yourself — Flow manages them automatically.

### Usage

To work with Internal State, you need to:

You can use **any type** as a state type, as long as it has serialization and deserialization functions defined. By default, all types with standard `Serialize`/`Deserialize` functions defined in YT are supported (including `TYsonStruct` subclasses); for a custom type, you just need to provide overloads for these functions.

By default, a state is considered empty if it equals the default value (`TMyState{}`), and clearing it recreates it. To override this behavior, the state type can inherit optional mixins from `NYT::NFlow`:

- `ICustomStateOps` — custom logic for `Clear()` and `IsEmpty()` (specified as a pair);
- `ICustomYsonView` — custom representation for `ToYsonView()` for read-state introspection.

You need to:

1. Declare a `TMutableStateKeyClient<TMyState> MyStateClient_` field in your `TComputation`.
2. Override `DoInit(IJobInitContextPtr initContext)` and call the initialization `initContext->InitClient<TMyState>(MyStateClient_, "my_state")` in it. Use a string that’s unique within the `Computation` as the name.
3. To get the state by key, use `MyStateClient_.GetState(message->Key)`. The returned accessor `TStateAccessor<TMyState>` behaves like a smart pointer to `TMyState` (`state->...`, `*state`) and is valid only within the current epoch — you can’t store it in fields.
4. To clear the state (delete the row from the table), call `state.Clear()` on the accessor.

If the state is empty, the corresponding row will be deleted.

Example:

```cpp
struct TMyState
    : public NYTree::TYsonStruct
{
    std::optional<ui64> SomeValue;

    REGISTER_YSON_STRUCT(TMyState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("some_value", &TThis::SomeValue)
            .Default();
    }
};

class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(MyStateClient_, "my_state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr output) override
    {
        auto state = MyStateClient_.GetState(message->Key);
        state->SomeValue = 42;
        // ...
    }

    void DoProcessTimer(
        const TTimer& timer,
        IOutputCollectorPtr output) override
    {
        auto state = MyStateClient_.GetState(timer.Key);
        // Clear the state (delete the row from the table):
        state.Clear();
    }

private:
    TMutableStateKeyClient<TMyState> MyStateClient_;
};
```

### Compression

Internal State supports data compression. You configure it in [DynamicSpec](../../../flow/concepts/spec.md) for each state separately — in the `state_manager` section of the computation, in `overrides/<state_name>/format`:

- `compress` — enable compression (default `false`);
- `recode_probability` — probability to recode the state to the specified format during the next processing; this ensures gradual migration of states after changing the format (default `0.1`).

## Yson State Reader

{% note warning %}

This functionality is not yet implemented.

{% endnote %}

## External State {#external-state}

External State is a state stored in a user-defined dynamic table in {{product-name}}. Unlike Internal State, the tables are created and managed by the user{% if audience == "internal" %} (for example, via [YtSync]({{yt-sync-docs}})){% endif %}.

The external state manager is declared in the `Computation` spec under a unique name in the `external_state_managers` section and connected to the `Computation` via a typed client `TMutableStateKeyClient<TState>`. The names in the spec must start with `/` (for example, `"/state"`) and match the name passed to `InitExternalStateClient`. The implementation is looked up in the registry by `external_state_manager_class_name`.

### Usage

To work with External State, you need to:

1. Declare a `TMutableStateKeyClient<TState> StateClient_` field in your `TComputation`, where `TState` is the state type returned by the corresponding external state manager (see below for specific implementations).
2. Override `DoInit(IJobInitContextPtr initContext)` and call `initContext->InitExternalStateClient(StateClient_, "/state")` in it. Use a string that’s unique within the `Computation` as the name — this same name must appear in the spec.
3. To get the state by key, use `StateClient_.GetState(message->Key)`. The returned accessor `TStateAccessor<TState>` behaves like a smart pointer to `TState` and is valid only within the current epoch.

Example:

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};
```

`Computation` spec with `TSimpleExternalStateManager` connected:

```yson
"external_state_managers" = {
    "/state" = {
        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
        "parameters" = {
            "path" = "//path/to/state/table";
        };
    };
};
```

### TSimpleExternalStateManager

`TSimpleExternalStateManager` is the standard implementation of an external state manager. It works with a single dynamic table whose keys match the `group_by_schema`. `GetState` returns an accessor over `TSimpleExternalState` with `Payload` and `Schema` fields; columns are retrieved and written via `GetColumn[Value]<T>` / `TPayloadBuilder` by name or index. State caching happens automatically via the shared [StateCache](#state-cache).

You don’t need to register `TSimpleExternalStateManager` in `register.cpp` — it’s already registered in the Flow library itself.

Spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSimpleExternalStateManagerSpec.md) %}

Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSimpleExternalStateManagerSpec.md) %}

For a complete example of using `TSimpleExternalStateManager`, see the breakdown of the [word_count](../../../flow/cpp/examples/word_count.md) example.

{% if audience == "internal" %}

## NBigRTExtensions::TProfileManager {#profile-manager}

`TProfileManager<TProfile>` is an external state manager compatible with BigRT profiles. It lets you reuse existing BigRT profiles in Flow [pipelines](../../../flow/concepts/glossary.md#pipeline).

Unlike `TSimpleExternalStateManager`, `TProfileManager` is parameterized by a user-defined profile type and therefore must be registered by the user. In the client, it’s convenient to use the helper state type `TProfileManagerState<TMyProfile>`, which lets you specify only the profile itself (without the manager alias). For brevity, there’s a client alias next to the state type: `NBigRTExtensions::TProfileMutableStateKeyClient<TMyProfile>` — it’s fully equivalent to `TMutableStateKeyClient<NBigRTExtensions::TProfileManagerState<TMyProfile>>`. Example:

```cpp
// header: connect the client with the state type bound to the profile.
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    // ...

private:
    // Equivalent to TMutableStateKeyClient<NBigRTExtensions::TProfileManagerState<TMyProfile>>.
    NBigRTExtensions::TProfileMutableStateKeyClient<TMyProfile> StateClient_;
};
```

```cpp
// register.cpp: register the computation and the manager itself.
YT_FLOW_DEFINE_COMPUTATION(TMyComputation);
YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(NYT::NFlow::NBigRTExtensions::TProfileManager<TMyProfile>);
```

Spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TProfileManagerSpec.md) %}

Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TDynamicProfileManagerSpec.md) %}

In the `Computation` spec, the class name is set via `external_state_manager_class_name` and must be the fully qualified name of the type that was passed to `YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER` (for example, `NYT::NFlow::NBigRTExtensions::TProfileManager<NMyNamespace::TMyProfile>`).

For more details on this extension, see the [Serializable Profile](../../../yandex-specific/flow/extensions/serializable-profile.md) section. For unit testing of functions with a Serializable Profile state, see the [Testing](../../../flow/cpp/process-functions.md#profile-testing) section.

{% endif %}

## External State Joiner {#external-state-joiner}

Use the External State Joiner to get read-only access to external states via a key-based join. You cache the loaded states in the shared StateCache with a TTL. Before the TTL expires, repeated requests for the same key are served from the cache without contacting YT.

You declare an external state joiner in the `Computation` spec under a unique name in the `external_state_joiners` section. Connect it to the `Computation` using the typed client `TJoinedStateKeyClient<TState>`. The names in the spec must start with `/` (for example, `"/reference"`) and match the name you pass to `InitExternalStateClient`. The system looks up the implementation in the registry by `external_state_joiner_class_name`.

### Usage

This works similarly to the external state manager:

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateReaderClient_, "/reference");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto state = StateReaderClient_.GetState(message->Key);
        // state is a read-only accessor TConstStateAccessor<TSimpleExternalState> (valid within the epoch).
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> StateReaderClient_;
};
```

Here is a `Computation` spec with `TSimpleExternalStateJoiner` attached:

```yson
"external_state_joiners" = {
    "/reference" = {
        "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
        "parameters" = {
            "path" = "//path/to/reference/table";
        };
    };
};
```

### TSimpleExternalStateJoiner {#simple-external-state-joiner}

`TSimpleExternalStateJoiner` is the standard implementation of an external state joiner. Its interface is similar to `TSimpleExternalStateManager`: it reads rows from a single dynamic table whose keys match the `group_by_schema`. The `GetState` method returns a read-only accessor over `TSimpleExternalState`, which contains `TPayload` and the schema of the table’s value columns. You retrieve columns via `GetColumn[Value]<T>` by name or index.

You don’t need to register `TSimpleExternalStateJoiner` in `register.cpp`; it’s already registered in the Flow library.

Spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSimpleExternalStateJoinerSpec.md) %}

Dynamic spec (set the cache TTL via the `cache` section):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSimpleExternalStateJoinerSpec.md) %}

### TStaticTableKeyVisitorJoiner {#static-table-key-visitor-joiner}

`TStaticTableKeyVisitorJoiner` is an external state joiner over a **static** sorted table. It works only with the [key-visitor stream](../../../flow/concepts/key_visitor.md#static-table-joiner). Unlike `TSimpleExternalStateJoiner`, it doesn’t perform random reads. The background visitor scan reads the table sequentially using the same key ranges as the internal state, and the table row becomes available in `DoProcessVisit` as a read-only state for the visit key. If a key isn’t in the table, it returns an empty state (`IsEmpty() == true`).

You bind the joiner to a visit stream by listing its name in the `external_names` field of the `key_visitor_streams` section (see [static spec parameters](../../../flow/concepts/key_visitor.md#static-params)). You can bind one joiner to no more than one visit stream (the system checks this when you submit the spec). `join_on/key_schema_override` isn’t supported; the key is always the joiner’s own `group_by_schema`.

Requirements for the source table:

- It must be static; the system rejects a dynamic table when reading the schema.
- It must be sorted so that the prefix of its key columns matches the `group_by_schema` of the `Computation` by name and type. The table can reside on another cluster (`<cluster=...>` in the rich path).

{% note warning %}

The system doesn’t validate computed column expressions. The source’s partition column must be **materialized** with values that match the expression in `group_by_schema` (usually `farm_hash(key)`). The framework can’t distinguish a differently materialized table from a correct one; each visit key silently resolves as missing.
{% endnote %}

The canonical pattern is mirroring (reconciliation). The table’s keys participate in the scan alongside the keys of your own state. That means a visit arrives for a key that isn’t yet in the state (you need to create it) and for a key that’s no longer in the table (you need to delete it). A periodic scan thus aligns the `Computation`’s state with the external table:

```cpp
class TMirrorComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient<TMirrorState>(MirroredState_, "/mirror");
        initContext->InitExternalStateClient(SourceState_, "/source");
    }

    void DoProcessVisit(
        const TVisit& visit,
        IOutputCollectorPtr /*output*/) override
    {
        auto src = SourceState_.GetState(visit.Key);
        if (!src.IsInitialized()) {
            // The source range wasn’t read (mark_unreadable): the key’s state is unknown.
            return;
        }
        auto mirror = MirroredState_.GetState(visit.Key);
        if (src.IsEmpty()) {
            mirror.Clear();
        } else {
            mirror->Payload = src->GetColumnValue<std::optional<std::string>>("payload").value_or(std::string{});
        }
    }

private:
    TMutableStateKeyClient<TMirrorState> MirroredState_;
    TJoinedStateKeyClient<TSimpleExternalState> SourceState_;
};
```

Here is the `Computation` spec for this example:

```yson
"external_state_joiners" = {
    "/source" = {
        "external_state_joiner_class_name" = "NYT::NFlow::TStaticTableKeyVisitorJoiner";
        "parameters" = {
            "path" = "//path/to/source";
        };
    };
};
"key_visitor_streams" = {
    "visit_iter" = {
        "names" = ["/mirror"];
        "external_names" = ["/source"];
    };
};
```

The `unavailable_source_policy` defines the behavior when the source is unavailable. A read is considered unsuccessful if it exhausts the `read_attempts` limit:

- `retry` (default): the error fails the scan iteration, and the read retries. The scan doesn’t advance past the unread range.
- `mark_unreadable`: the error is ignored, and the scan continues. Keys in the unread range resolve to an **uninitialized** accessor: `IsInitialized() == false`, and dereferencing throws an exception. The `Computation` decides how to handle a key whose state is unknown in the source (usually you skip the visit, as shown in the example above).

After an unsuccessful read, the source is marked unavailable for `unavailable_source_backoff`. While this flag is active, the system doesn’t contact the source; reads immediately resolve according to the policy. The first read after the window expires tries the source again. You can see the joiner’s state in the worker sensors `static_table_key_visitor_joiner/{source_unavailable,failed_reads,reader_opens,listed_size}` with the tag `external_state_joiner=<name>`.

You don’t need to register `TStaticTableKeyVisitorJoiner` in `register.cpp`; it’s already registered in the Flow library.

Spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TStaticTableKeyVisitorJoinerSpec.md) %}

Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStaticTableKeyVisitorJoinerSpec.md) %}

{% if audience == "internal" %}

### NBigRTExtensions::TProfileJoiner {#profile-joiner}

`TProfileJoiner<TProfile>` is a read-only joiner compatible with BigRT profiles. You register it with the user via `YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER`:

```cpp
YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER(NYT::NFlow::NBigRTExtensions::TProfileJoiner<TMyProfile>);
```

In the `Computation`, you use the same helper state type `TProfileManagerState<TMyProfile>` as for the manager: `TJoinedStateKeyClient<NBigRTExtensions::TProfileManagerState<TMyProfile>>`. The shortened alias is `NBigRTExtensions::TProfileJoinedStateKeyClient<TMyProfile>`; it’s more convenient to use this in your user code.

Spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TProfileJoinerSpec.md) %}

Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TDynamicProfileJoinerSpec.md) %}

For more details about this extension, see the [Serializable Profile](../../../yandex-specific/flow/extensions/serializable-profile.md) section. For unit testing functions with a Serializable Profile state, see the [Testing](../../../flow/cpp/process-functions.md#profile-testing) section.

{% endif %}

## State Joiner {#state-joiner}

Use the State Joiner to get read-only access to the internal state of another `Computation`. One `Computation` can enrich its data with the state that another `Computation` accumulates, even when the join key doesn’t match its own `group_by_schema`.

You declare the Joiner in the `Computation` spec under a unique name in the `state_joiners` section and connect it via the typed client `TJoinedStateKeyClient<TState>`. Names in the spec must start with `/` and match the name you pass to `InitClient`. You don’t need a separate implementation or registration—this is a built-in Flow feature.

In the spec, you specify:

- `computation_id` — the `Computation` whose internal state is read.
- `state_name` — the state client name of the target `Computation` (the prefix it passed to `InitClient`; it must start with `/`).
- `join_on/key_schema_override` — the columns in the current row that define the key. If you don’t set this, the `Computation` uses its own `group_by_schema` (join on the same key).
- `join_on/key_provider_streams` — the streams from which keys are taken from messages and timers (`nullopt` means all input streams).
- `auto_preload` — if `true` (the default), the framework loads the keys before each `DoProcess`; otherwise, the `Computation` calls `PreloadKeyStates` itself.

{% note warning %}

The framework checks key types at startup: `key_schema_override` (or the `Computation`’s own `group_by_schema` if you don’t set an override) must match the target `Computation`’s `group_by_schema` in the number of columns and their types. The Joiner calculates the full key for the target state. Column names and expressions can differ.

You’re responsible for the following (the framework doesn’t check them):

- *Value mapping* — you must ensure each column maps correctly so the calculated key points to the right row in the other `Computation`’s state.
- `TState` must match the state type of the target `Computation`. The Joiner deserializes its rows the same way `TMutableStateKeyClient<TState>` does on the owner side.

{% endnote %}

### Usage

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(UpstreamClient_, "/upstream");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto state = UpstreamClient_.GetState(message);
        // state — read-only accessor TConstStateAccessor<TUpstreamState> (valid within the epoch).
    }

private:
    TJoinedStateKeyClient<TUpstreamState> UpstreamClient_;
};
```

Spec for a `Computation` with a Joiner connected using a key that differs from its own `group_by_schema`:

```yson
"state_joiners" = {
    "/upstream" = {
        "computation_id" = "accumulator";
        "state_name" = "/total";
        "join_on" = {
            "key_schema_override" = [
                {name = "Hash"; expression = "farm_hash(UserId)"; type = "uint64"; required = %true;};
                {name = "UserId"; type = "string"; required = %true;};
            ];
        };
    };
};
```

By default, the cache is disabled (`ttl = 0`). The Joiner reads the target `Computation`’s state at `SyncLastCommittedTimestamp` each epoch, so you always see the latest commits. You enable a cache with TTL in the dynamic spec via the `cache` section (loaded states stay valid until `ttl` expires; repeated reads for the same key are served without contacting YT). For `Swift` computations that require strict determinism, you must consider the cache and reading at `SyncLastCommittedTimestamp`.

Spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TStateJoinerSpec.md) %}

Dynamic spec (set cache TTL via the `cache` section):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStateJoinerSpec.md) %}

## StateCache {#state-cache}

Flow provides a shared two-level (uncompressed + compressed) LRU cache for states. You configure it via `/dynamic_spec/job_tracker/state_cache`. It’s primarily used for manager states; Joiners have the cache disabled by default, and data is loaded at `SyncLastCommittedTimestamp`.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStateCacheSpec.md) %}

For more details on `group_by_schema` consistency and general principles for working with states, see [Stateful processing](../../../flow/concepts/stateful.md).

## See also

- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Quick start (C++)](../../../flow/cpp/getting-started.md)
