#pragma once

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! YT's underlying ``ModifyRows`` bound is ~100k; we batch well below it for paginated
//! reads and deletes across all state sections (key_states, partition_states, external).
inline constexpr i64 StateAccessBatchSize = 10'000;

////////////////////////////////////////////////////////////////////////////////

//! Which table(s) ``read-states`` / ``delete-states`` should access.
DEFINE_ENUM(EFlowStateTarget,
    ((All)               (0))
    ((KeyState)          (1))
    ((PartitionState)    (2))
    ((ExternalKeyState)  (3))
);

//! Shared args for read-states and delete-states. Modes:
//!   1. ComputationId only       → all tables for every partition of the computation.
//!   2. ComputationId + Key      → key_states only.
//!   3. PartitionId              → partition_states + key_states under the partition's SourceKey.
struct TStateAccessArgs
    : public NYTree::TYsonStructLite
{
    std::optional<TComputationId> ComputationId;
    std::optional<TPartitionId> PartitionId;
    std::optional<NYson::TYsonString> KeyAsYson;
    std::optional<std::string> Name;
    EFlowStateTarget Target{};

    REGISTER_YSON_STRUCT_LITE(TStateAccessArgs);

    static void Register(TRegistrar registrar);
};

struct TReadStatesArg
    : public TStateAccessArgs
{
    i64 Limit{};

    REGISTER_YSON_STRUCT_LITE(TReadStatesArg);

    static void Register(TRegistrar registrar);
};

//! Dry-run by default: without ``commit``, matching rows are counted but not erased.
struct TDeleteStatesArg
    : public TStateAccessArgs
{
    bool Force{};
    bool Commit{};

    REGISTER_YSON_STRUCT_LITE(TDeleteStatesArg);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! One row in the ``key_states`` (or ``external_key_states``) section of read-states output.
struct TKeyStateRow
    : public NYTree::TYsonStructLite
{
    TComputationId ComputationId;
    TKey Key;
    THashMap<std::string, NYson::TYsonString> States;

    REGISTER_YSON_STRUCT_LITE(TKeyStateRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! One row in the ``partition_states`` section of read-states output.
struct TPartitionStateRow
    : public NYTree::TYsonStructLite
{
    std::optional<TComputationId> ComputationId;
    TPartitionId PartitionId;
    THashMap<std::string, NYson::TYsonString> States;

    REGISTER_YSON_STRUCT_LITE(TPartitionStateRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TReadStatesResponse
    : public NYTree::TYsonStructLite
{
    std::vector<TKeyStateRow> KeyStates;
    std::vector<TPartitionStateRow> PartitionStates;
    //! External states owned by managers (mutable, deletable).
    std::vector<TKeyStateRow> ExternalKeyStates;
    //! External states observed through readers (read-only, joined-in).
    std::vector<TKeyStateRow> JoinedExternalKeyStates;
    std::vector<std::string> Errors;

    REGISTER_YSON_STRUCT_LITE(TReadStatesResponse);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Per-source breakdown of erased rows for delete-states output.
struct TMatchedStatesBucket
    : public NYTree::TYsonStructLite
{
    i64 Total{};
    THashMap<TComputationId, THashMap<std::string, i64>> Details;

    REGISTER_YSON_STRUCT_LITE(TMatchedStatesBucket);

    static void Register(TRegistrar registrar);
};

struct TMatchedStates
    : public NYTree::TYsonStructLite
{
    TMatchedStatesBucket KeyStates;
    TMatchedStatesBucket PartitionStates;
    TMatchedStatesBucket ExternalKeyStates;

    REGISTER_YSON_STRUCT_LITE(TMatchedStates);

    static void Register(TRegistrar registrar);
};

struct TDeleteStatesResponse
    : public NYTree::TYsonStructLite
{
    bool Committed{};
    TMatchedStates MatchedStates;
    std::vector<std::string> Errors;

    REGISTER_YSON_STRUCT_LITE(TDeleteStatesResponse);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TControllerExternalStateManagerEntry
{
    IExternalStateManagerPtr Manager;
};

struct TControllerExternalStateJoinerEntry
{
    NTableClient::TTableSchemaPtr KeySchema;
    IExternalStateJoinerPtr Joiner;
};

//! One-shot bundle of external state managers/joiners instantiated on the controller for
//! a single read-states / delete-states call. Owns the throwaway StateCache and resource
//! manager — destroying the bundle tears everything down.
struct TControllerExternalStateBundle
{
    NTableClient::TTableSchemaPtr ComputationKeySchema;
    THashMap<std::string, TControllerExternalStateManagerEntry> Managers;
    THashMap<std::string, TControllerExternalStateJoinerEntry> Joiners;

    // Anchors: keep the underlying caches and resources alive while entries are in use.
    TStateCachePtr StateCache;
    IResourceManagerPtr ResourceManager;
};

//! Instantiates external state managers and joiners for |computationId| and loads its
//! controller-side static resources. Throws on any failure (unknown computation, missing
//! spec, failed resource load).
TControllerExternalStateBundle BuildControllerExternalStateBundle(
    const TFlowViewPtr& flowView,
    const TComputationId& computationId,
    const NClient::NCache::IClientsCachePtr& clientsCache,
    const NYPath::TRichYPath& pipelinePath,
    const IPipelineAuthenticatorPtr& authenticator,
    const IStatusProfilerPtr& statusProfiler,
    const IPayloadConverterCachePtr& converterCache,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Per-external filter for a single ``read-states`` / ``delete-states`` request.
//!   * ``ExactKey`` set → Mode 1/2 exact-key Lookup (adapted to the external's key schema).
//!   * ``ExactKey`` unset → Mode 3 unbounded range scan.
//! When ``Error`` is set, the entry was skipped due to a per-X failure (e.g., schema
//! conversion). Callers propagate it to ``response.errors`` rather than throwing.
struct TExternalStateFilter
{
    std::string Name;
    std::optional<TKey> ExactKey;
    std::optional<std::string> Error;
};

//! Builds per-external filters for |argument| against |bundle|. Per-X failures are captured
//! in ``Error`` rather than thrown; the entry without ``Error`` and without ``ExactKey`` is a
//! Mode-3 unbounded scan. Mode 2 (PartitionId) without ``SourceKey`` is silently skipped (matches
//! the existing key_states behaviour). When ``argument.Name`` is set, only matching externals
//! are returned.
std::vector<TExternalStateFilter> BuildExternalStateFilters(
    const TStateAccessArgs& argument,
    const TControllerExternalStateBundle& bundle,
    const TFlowViewPtr& flowView,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache,
    const IPayloadConverterCachePtr& converterCache,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct TReadExternalStatesResult
{
    //! Rendered rows from manager-backed external states.
    std::vector<TKeyStateRow> ManagerRows;
    //! Rendered rows from reader-backed (joined) external states.
    std::vector<TKeyStateRow> JoinerRows;
    std::vector<std::string> Errors;
};

//! Executes |filters| against |bundle| and renders matching external states. Mode-1/2
//! filters trigger a single-key Preload + render. Mode-3 filters are split per section:
//! managers share |limit| as ``max(1, limit / managerRangeCount)``, joiners share |limit|
//! independently as ``max(1, limit / joinerRangeCount)``. Each range filter issues one ``List``
//! of that size, then ``Preload`` + render. Empty states are dropped. Per-X failures are
//! captured in ``Errors``.
TReadExternalStatesResult ReadExternalStates(
    const TComputationId& computationId,
    const TControllerExternalStateBundle& bundle,
    const std::vector<TExternalStateFilter>& filters,
    i64 limit,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct TDeleteExternalStatesResult
{
    TMatchedStatesBucket Matched;
    std::vector<std::string> Errors;
};

//! Counts and (if |commit| is set) erases external state rows matching |filters| via
//! ``Manager::PreloadKeyStates`` + ``state->Clear()`` + ``Manager::Sync``. Joiners are skipped.
//! Mode-1/2 issues a single per-key cycle; Mode-3 paginates ``List`` in ``StateAccessBatchSize``
//! batches and commits per batch. Per-X failures (e.g., List throws, Sync throws) are captured
//! in ``Errors``; the rest of the filters still process.
TDeleteExternalStatesResult DeleteExternalStates(
    const TComputationId& computationId,
    const TControllerExternalStateBundle& bundle,
    const std::vector<TExternalStateFilter>& filters,
    const NApi::IClientPtr& client,
    bool commit,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
