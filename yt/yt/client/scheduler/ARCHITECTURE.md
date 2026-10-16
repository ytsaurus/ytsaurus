## `yt/yt/client/scheduler` — Scheduler Client Public Types

The narrow set of scheduler types that have to be visible in the **client** tier (`yt/yt/client/api`, RPC proxy client, driver, federated/hedging clients). Everything here is consumed by client code that talks to the scheduler over RPC; server-side types live in `yt/yt/ytlib/scheduler` and below.

Namespace: `NYT::NScheduler`.

**Components:**

**`public.h`** — Strong typedefs and shared enums. No code beyond enum/typedef declarations.
- `TAllocationId` — `YT_DEFINE_STRONG_TYPEDEF(TAllocationId, TGuid)`. A single scheduled unit on a node (one job slot). `NullAllocationId` is the zero-initialized default.
- `TOperationId` / `TJobId` — re-exported from `NJobTrackerClient`.
- `EOperationType` — `Map` / `Merge` / `Erase` / `Sort` / `Reduce` / `MapReduce` / `RemoteCopy` / `JoinReduce` / `Vanilla`.
- `EOperationState` — full lifecycle: `None → Starting → (Orphaned | WaitingForAgent) → Initializing → Preparing → Materializing → (ReviveInitializing → Reviving → RevivingJobs) → Pending → Running → (Completing → Completed | Aborting → Aborted | Failing → Failed)`.
- `EAbortReason` — large enum covering all allocation abort reasons, numerically partitioned: values `1–99` are non-scheduling, `100–199` are `Scheduling*` (with `SchedulingFirst=100` / `SchedulingLast=199` sentinels), `200+` are miscellaneous late additions. `DEFINE_ENUM_UNKNOWN_VALUE` points at `Unknown` for forward-compatibility. Extending it requires bumping the controller-agent tracker protocol version (see comment).
- `EInterruptionReason`, `EUnavailableChunkAction`, `ESchemaInferenceMode`, `EAutoMergeMode` — smaller enums used by operation specs and allocation interruption.
- `YT_DEFINE_ERROR_ENUM` — scheduler error codes in the `200–224` range (`NoSuchOperation`, `InvalidOperationState`, `NoSuchAllocation`, `AgentRevoked`, `PoolTreesAreUnspecified`, `OperationLaunchedInNonexistentPool`, …). These are the only error codes client code is expected to catch by code.

**`operation_id_or_alias.h`** — `TOperationIdOrAlias`.
- Tagged union (`std::variant<TOperationId, TString>`) used anywhere the public API accepts either form. Alias strings must start with `*` (the `OperationAliasPrefix`); `TOperationIdOrAlias::FromString(s)` parses the prefix character to decide which branch to take, `FormatValue` emits the alias verbatim or stringifies the id.
- Implements `operator==` and `operator size_t()` (hash), so it works as a map key directly — used e.g. as the `TOperationCache` key.
- `FromProto<TProto>` / `ToProto<TProto>` are templates over an **enclosing** proto message because the wire representation is a `oneof` of `operation_id` / `operation_alias` fields — not a standalone message. Unknown alias prefix or an unset oneof throw. Callers pass the parent message, not a sub-field.

**`operation_cache.h`** — `TOperationCache`.
- Async expiring cache (`TAsyncExpiringCache<TOperationIdOrAlias, TYsonString>`) sitting in front of `IClient::GetOperation`. Constructed with the set of attributes to fetch; each lookup issues `GetOperation(key, {.Attributes = attributes_, .IncludeRuntime = true})` and memoizes the YSON response. Runs on the RPC dispatcher's heavy invoker. Primary consumer: the HTTP proxy CHYT handler (`yt/yt/server/http_proxy/clickhouse/handler.cpp`).

**`spec_patch.h`** — `TSpecPatch`.
- `TYsonStruct` carrying a `(YPath, INodePtr value)` pair; a vector of these is passed through `IClient::PatchOperationSpec` to mutate an operation's spec at runtime. Has `ToProto`/`FromProto` for the RPC-proxy proto message. `TSpecPatchList = std::vector<TSpecPatchPtr>` is the typical argument type.

**`private.h`** — Internal.
- `SchedulerLogger` — global logger named `"Scheduler"`. Flagged as TODO-to-move-to-public.

**Notes:**
- The library is intentionally thin — it contains only the vocabulary needed by the public client API (`yt/yt/client/api/operation_client.h`). Anything heavier (RPC proto messages, spec structs, runtime parameters, fair-share config) lives in `yt/yt/ytlib/scheduler`, which is the server-side companion.
- `EAbortReason`'s numerical partitioning (`Scheduling*` ∈ `[100, 199]`) is used as a runtime predicate: "was this allocation aborted by scheduling logic?" Code filtering on that range assumes every scheduling abort reason falls in the band — keep new values in it.
- `TAllocationId` is a strong typedef, not an alias, so accidental `TGuid ↔ TAllocationId` conversions will not compile. Use `TAllocationId{guid}` explicitly when constructing from proto.
- Aliases (the `*`-prefixed strings) are a scheduler concept: any operation can register an alias that subsequent API calls accept in place of the id. The client-side handling is entirely in `TOperationIdOrAlias`; the scheduler resolves the alias to an id on the server.
