## `yt/yt/ytlib/scheduler` — Scheduler Server-Tier Shared Types

Server-tier counterpart to `yt/yt/client/scheduler`. Everything needed by code that talks to the scheduler with full privileges — operation spec schemas, pool configuration schemas, the operation-service RPC proxy, resource vocabulary, and the operations-archive table schemas — plus helpers used across the scheduler, controller agent, and CLI.

Namespace: `NYT::NScheduler` (with `NYT::NScheduler::NProto` for proto messages and `NYT::NScheduler::NRecords` for archive row types).

**Components:**

**`public.h`** — Vocabulary and forward declarations.
- Re-exports from `NVectorHdrf`: `TJobResources`, `TResourceVector`, `TResourceVolume`, `EJobResourceType`, `TCpuResource`, `EIntegralGuaranteeType`, `ESchedulingMode`, `ResourceCount`. Thanks to the re-export, downstream code does not need to include the `vector_hdrf` headers directly.
- Scheduler-side enums: `EJobFinalState` (`Failed`/`Aborted`/`Completed`), `EPreemptionMode` (`Normal`/`Graceful`), `EFifoSortParameter`, `ESchedulingSegment` (`Default`/`LargeGpu`), `EEnablePorto`, `EDelayType`, `EExtraEnvironment`.
- Constants: `RootPoolName = "<Root>"`, `PoolTreesRootCypressPath = "//sys/pool_trees"`, `MaxSchedulingTagRuleCount = 100`.
- Forward declarations for every proto message in `proto/` and every config/spec struct in `config.h` (around 60 `DECLARE_REFCOUNTED_STRUCT` lines). Include this header to reach for a refcounted pointer type without pulling in the full spec definitions.

**`config.h`** — The big one: YSON-struct definitions for every user-facing configuration surface.
- **Pool config:** `TPoolConfig` (weight, guarantees, FIFO mode, limits, integral guarantees, preemption overrides, metering tags, priority adjustments, …), `TPoolPresetConfig`, `TEphemeralSubpoolConfig`, `TPoolIntegralGuaranteesConfig`, `TPoolPreemptionConfig`, `TCommonPreemptionConfig`, `TSchedulableConfig`, `TExtendedSchedulableConfig`.
- **Pool name:** `TPoolName` — `(pool, optional parent)` pair with `ToString` / `FromString` round-trip. Used by API calls that accept a `pool:parent` form for ephemeral pools.
- **Operation specs:** `TOperationSpecBase` (root of the hierarchy — holds ACL/ACO, pool trees, scheduling tag filter, default job spec, alerts config, secure vault, etc.) and all operation-type-specific subclasses: `TMapOperationSpec`, `TReduceOperationSpec`, `TMapReduceOperationSpec`, `TMergeOperationSpec` and variants (`TUnorderedMergeOperationSpec`, `TOrderedMergeOperationSpec`, `TSortedMergeOperationSpec`), `TSortOperationSpec`, `TEraseOperationSpec`, `TRemoteCopyOperationSpec`, `TVanillaOperationSpec`. Intermediate abstract bases: `TSimpleOperationSpecBase`, `TUnorderedOperationSpecBase`, `TSortOperationSpecBase`, `TOperationWithInputSpec`, `TOperationWithUserJobSpec`, `TInputlyQueryableSpec`.
- **Job specs:** `TUserJobSpec` (with `TMandatoryUserJobSpec` / `TOptionalUserJobSpec` variants), `TVanillaTaskSpec`, `TGangOptions`, `TCollectiveOptions`, `TSidecarJobSpec`, `TGracefulShutdownSpec`.
- **Strategy:** `TStrategyOperationSpec` (per-operation pool-tree-related knobs), `TStrategyPackingConfig`, `TTentativeTreeEligibilityConfig`.
- **Runtime parameters** (mutable subset persisted to Cypress between updates): `TOperationRuntimeParameters`, per-tree `TOperationPoolTreeRuntimeParameters`, job-shell `TOperationJobShellRuntimeParameters`, and their `*Update` counterparts. `UpdatePoolTreeRuntimeParameters(origin, update)` applies an update onto the existing value.
- **Job I/O & resources:** `TJobIOConfig`, `TJobResourcesConfig` (inherits `NVectorHdrf::TJobResourcesConfig` and adds YSON registration), `TJobResourcesWithDiskConfig`, `TSamplingConfig`, `TJobSplitterConfig`, `TAutoMergeConfig`, `TInputQueryFilterOptions` / `TInputQueryOptions`.
- **Environment & storage:** `TTmpfsVolumeConfig`, `TNbdDiskConfig`, `TDiskRequestConfig` (+ `TLocalDiskRequest`, `TNbdDiskRequest`, `TTmpfsStorageRequest`, `TDeprecatedDiskRequestConfig`, `TStorageRequestBase`), `TVolume`, `TVolumeMount`, `TLayer`, `TCudaProfilerEnvironment`, `TJobProfilerSpec`.
- **Experiments & testing:** `TJobExperimentConfig`, `TExperimentOperationSpec`, `TTestingOperationOptions`, `TDelayConfig`, `TPatchSpecProtocolTestingOptions`.
- **Connection config:** `TSchedulerConnectionConfig` (inherits `TRetryingChannelConfig`) — RPC timeout + ack timeout.
- A few non-YSON testing/compat enums: `EPackingMetricType`, `EMergeMode`, `EDelayInsideOperationCommitStage`, `EControllerFailureType`, `ESingleChunkTeleportStrategy`, …

**`helpers.h`** — Shared utilities.
- **ID conversion:** `AllocationIdFromJobId(jobId)` derives the containing allocation's id from a job id.
- **Cypress paths:** `GetOperationPath`, `GetJobsPath` / `GetJobPath`, `GetStderrPath`, `GetFailContextPath`, `GetSnapshotPath`, `GetSecureVaultPath`, `GetSchedulerOrchidOperationPath`, `GetSchedulerOrchidAliasPath`, `GetControllerAgentOrchidOperationPath`, `FindControllerAgentAddressFromCypress`, `GetPoolTreesLockPath`, `GetUserToDefaultPoolMapPath`, the `GetOperationsArchive*Path` family (operation id / ordered-by-id / ordered-by-start-time / aliases / jobs / job_specs / stderrs / profiles / fail_contexts / trace_events / traces / events / version), `GetOperationsAcoPrincipalPath`.
- **State predicates:** `IsOperationFinished(state)`, `IsOperationFinishing(state)`, `IsOperationInProgress(state)`; `IsSchedulingReason(reason)` / `IsNonSchedulingReason` / `IsSentinelReason` classify `EAbortReason` by the `Scheduling*` numerical band.
- **Access control:** `TAccessControlRule` — a variant of a `TSerializableAccessControlList` or an ACO name; `GetOrLookupAcl` resolves to an ACL via the client when the rule is an ACO. `ValidateOperationAccess` / `CheckOperationAccess{ByAcl,ByAco}` perform permission checks (the `Validate*` family throws, `Check*` returns `TError`). `GetAclFromAcoName`, `TryGetAccessControlRuleFromOperation`, `GetAcrFromAllocationBriefInfo` are support helpers.
- **Archive lookup:** `LookupOperationsInArchive(client, ids, columnFilter, timeout?)` reads operation rows from the archive.
- **Pool name validation:** `CheckPoolName(name, regex)` / `ValidatePoolName(name, regex)`.
- **Allocation brief info:** `TAllocationBriefInfo` (`AllocationId`, `OperationId`, ACL/ACO, controller-agent descriptor, node descriptor) and the companion `TAllocationInfoToRequest` (field selector). Used by `GetAllocationBriefInfo` RPC.
- **Job shell:** `TJobShellInfo` wraps a `TJobShell` config + runtime parameters; `ApplyJobShellOptionsUpdate` merges an update map into the existing map.
- **Transaction error builders:** `GetSchedulerTransactionsAbortedError`, `GetUserTransactionAbortedError`.
- **Proto conversions:** `ToProto` / `FromProto` for the round-trip types `TAllocationBriefInfo`, `TAllocationInfoToRequest`, `TGracefulShutdownSpec`, `TSidecarJobSpec`, `TVolumeMount`. The write-only builders of the execution spec the controller agent ships to exec node live in `yt/yt/server/controller_agent/helpers.h`, their only user.

**`scheduler_channel.h`** — `CreateSchedulerChannel(config, channelFactory, masterChannel, networks)` returns an `IChannelPtr` that discovers the active scheduler via the master and wraps the discovered channel in retry logic per `TSchedulerConnectionConfig`. Consumers: the native client and the scheduler CLI.

**`scheduler_service_proxy.h`** — `TOperationServiceProxy` (`SchedulerService`, protocol version 4). Client-facing RPCs: `StartOperation`, `AbortOperation`, `SuspendOperation`, `ResumeOperation`, `CompleteOperation`, `UpdateOperationParameters`, `PatchOperationSpec`, `GetAllocationBriefInfo`. Used directly by native clients that cannot go through the RPC proxy layer.

**`job_resources.h`** — One-line forwarder to `yt/yt/library/vector_hdrf/job_resources.h`.

**`job_resources_with_quota.h`** — Resource vector extended with disk.
- `TDiskQuota` — `{medium → bytes}` map plus an optional "without medium" bucket. Supports arithmetic (+/-), `Max`, `Dominates`, and explicit `operator bool()` (non-empty check). `CreateDiskQuota(mediumIndex, bytes)` / `CreateDiskQuotaWithoutMedium(bytes)` are the common factories.
- `TJobResourcesWithQuota : TJobResources` — resource vector plus a `TDiskQuota`. `TJobResourcesWithQuotaList` is a `TCompactVector<…, 8>` used on heartbeat hot paths.
- `EJobResourceWithDiskQuotaType` — extension of `EJobResourceType` that adds `DiskQuota` as resource index 5.
- `CanSatisfyDiskQuotaRequest(resources, quota, considerUsage?)` / `CanSatisfyDiskQuotaRequests(resources, quotaList, ...)` — feasibility checks the policy uses before placing an allocation. The vector-input version also has a free-standing testing overload that takes raw per-location sizes.

**`job_resources_helpers.h`** — Resource formatting, profiling, serialization.
- `ToJobResources` / `ToNodeResources` — `TJobResources ↔ TNodeResources` (node-tracker proto).
- `ToJobResources(config, defaults)` — fill a `TJobResources` from a `TJobResourcesConfigPtr`, substituting defaults for unset fields.
- `SerializeDiskQuota` / `SerializeJobResourcesWithQuota` — YSON serialization that knows about the medium directory.
- `FormatResourceUsage(usage, limits)` / `FormatResources(resourcesWithQuota)` / `FormatResourcesConfig(config)` — string formatters for logs and errors.
- `TJobResourcesProfiler` — holds a `TBufferedProducer` and emits per-resource gauges or counters. Call `Init` → `Start` → `Update(resources, tags?)` → `Stop`. `ProfileResources(writer, resources, prefix, metricType)` is the one-shot equivalent.
- `PHOENIX_DECLARE_EXTERNAL_TYPE(TJobResources, ...)` — native-stream persistence, declared externally since the fields are protected and the type lives in `library/vector_hdrf`.
- `NProto::ToProto` / `FromProto` for `TDiskQuota`, `TJobResources`, `TJobResourcesWithQuota`.

**`disk_resources.h`** — `TDiskResources` carries per-location `(usage, limit, mediumIndex)` plus a default medium index; converted to/from the node-tracker `TDiskResources` proto on every heartbeat. `ToString(resources, mediumDirectory)` pretty-prints it; `Serialize` emits YSON.

**`cluster_name.h`** — `TClusterName` — strong typedef for `std::optional<std::string>`; `std::nullopt` is `LocalClusterName`. `IsLocal(name)` is the inverse. Used mainly by remote-copy operations.

**`input_query.h`** — `TInputQuerySpec` — query + external codegen info + filter options, with phoenix-style serialization via protobuf so it round-trips through snapshots. `CanCreateGranuleFilter()` checks whether the query can be used to prune chunks at read time.

**`pool_ypath_proxy.h`** — `TSchedulerPoolYPathProxy` — YPath proxy for `master`-side `SchedulerPool` Cypress objects, with a `TransferPoolResources` mutating method.

**`proto/`** — The wire schemas:
- `scheduler_service.proto` — operation service RPCs (start/abort/suspend/resume/complete/update-parameters/patch-spec/allocation-brief-info).
- `resources.proto` — `TJobResources`, `TDiskQuota`, `TDiskLocationQuota`, disk-request messages (`TDiskRequest`, `TLocalDiskRequest`, `TNbdDiskRequest`, `TTmpfsStorageRequest`, `TDeprecatedDiskRequest`, `TStorageRequestCommonParameters`), `TJobResourcesWithQuota`, `TAllocationAttributes`, `TChunkNbdDisk`.
- `allocation.proto` / `input_query.proto` / `rich_transaction_id.proto` / `pool_ypath.proto` — narrower message groups.

**`records/`** — YAML record schemas (in `NYT::NScheduler::NRecords` namespace) for the operations archive tables the scheduler writes and clients read:
- `operation_id.yaml`, `ordered_by_id.yaml`, `ordered_by_start_time.yaml`, `operation_alias.yaml`, `operation_events.yaml` — operation-level tables.
- `job.yaml`, `job_spec.yaml`, `job_stderr.yaml`, `job_profile.yaml`, `job_fail_context.yaml`, `job_trace_event.yaml`, `job_trace.yaml` — job-level tables.

These are compiled into structured `T*`/`T*Partial`/`T*Key` row types via the records codegen, so archive clients get typed row access rather than hand-written column lookups.

**Notes:**
- Many `T*Config` structs inherit multiple YSON bases via virtual inheritance (e.g. `TPoolConfig : TSchedulableConfig, TPoolPresetConfig` where both ultimately derive from `TYsonStruct`). This diamond is deliberate — it lets pool presets mirror the full pool surface. When adding a new field to a `*Config`, think about whether it belongs on the preset or the leaf.
- Spec types in `config.h` are **construction time** — the scheduler parses user YSON into them once per operation (or once per patch). Mutations happen via `TOperationRuntimeParametersUpdate`, never by mutating a parsed spec in place.
- The operations archive tables (`records/`) are schemaless from the scheduler's point of view — the scheduler writes whatever columns the current binary supports; older readers silently ignore unknown ones. Evolving a schema is mostly additive.
- `TAccessControlRule` carries **either** an inline ACL **or** an ACO name. Most code paths convert the ACO to an ACL via `GetOrLookupAcl` before permission checks, but some diagnostics keep the raw name (e.g. for display). The `IsAcoName()` / `IsAcl()` discriminator is the right way to branch.
- `TOperationServiceProxy`'s protocol version (`4`) is checked at handshake. Bumping it is required when adding/removing RPCs or changing message semantics; see the comment on `EAbortReason` in `client/scheduler/public.h` for a similar compatibility constraint on enum values.
- Phoenix metadata is the persistence format for `TJobResources` in native snapshots (not YSON, not proto). Fields are saved positionally, so reordering them breaks binary compatibility with existing snapshots — add new fields at the end only.

**See also:** `yt/yt/client/scheduler` (public vocabulary), `yt/yt/library/vector_hdrf` (the resource-vector / job-resources types this library re-exports), `yt/yt/server/lib/scheduler` (strictly server-only types, built on top of everything here), `yt/yt/server/scheduler` (the scheduler daemon).
