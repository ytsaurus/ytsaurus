#include "state_access.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/guid.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TStateAccessArgs::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_id", &TThis::ComputationId)
        .Default();
    registrar.Parameter("partition_id", &TThis::PartitionId)
        .Default();
    registrar.Parameter("key", &TThis::KeyAsYson)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
    registrar.Parameter("target", &TThis::Target)
        .Default(EFlowStateTarget::All);

    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Throw);

    registrar.Postprocessor([] (TThis* arg) {
        THROW_ERROR_EXCEPTION_UNLESS(arg->ComputationId || arg->PartitionId,
            "Exactly one of computation_id or partition_id must be specified");
        THROW_ERROR_EXCEPTION_IF(arg->ComputationId && arg->PartitionId,
            "computation_id and partition_id are mutually exclusive");
        THROW_ERROR_EXCEPTION_IF(arg->KeyAsYson && !arg->ComputationId,
            "key requires computation_id");
        THROW_ERROR_EXCEPTION_IF(arg->KeyAsYson && arg->Target == EFlowStateTarget::PartitionState,
            "target=partition_state cannot be combined with key (a key only addresses key_states)");
        if (arg->Name) {
            ValidateStateName(*arg->Name);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TReadStatesArg::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .GreaterThan(0)
        .Default(10);
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteStatesArg::Register(TRegistrar registrar)
{
    registrar.Parameter("force", &TThis::Force)
        .Default(false);
    registrar.Parameter("commit", &TThis::Commit)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TKeyStateRow::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_id", &TThis::ComputationId);
    registrar.Parameter("key", &TThis::Key);
    registrar.Parameter("states", &TThis::States);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionStateRow::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_id", &TThis::ComputationId)
        .Default();
    registrar.Parameter("partition_id", &TThis::PartitionId);
    registrar.Parameter("states", &TThis::States);
}

////////////////////////////////////////////////////////////////////////////////

void TReadStatesResponse::Register(TRegistrar registrar)
{
    registrar.Parameter("key_states", &TThis::KeyStates)
        .Default();
    registrar.Parameter("partition_states", &TThis::PartitionStates)
        .Default();
    registrar.Parameter("external_key_states", &TThis::ExternalKeyStates)
        .Default();
    registrar.Parameter("joined_external_key_states", &TThis::JoinedExternalKeyStates)
        .Default();
    registrar.Parameter("errors", &TThis::Errors)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMatchedStatesBucket::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total)
        .Default();
    registrar.Parameter("details", &TThis::Details)
        .Default();
}

void TMatchedStates::Register(TRegistrar registrar)
{
    registrar.Parameter("key_states", &TThis::KeyStates);
    registrar.Parameter("partition_states", &TThis::PartitionStates);
    registrar.Parameter("external_key_states", &TThis::ExternalKeyStates);
}

void TDeleteStatesResponse::Register(TRegistrar registrar)
{
    registrar.Parameter("committed", &TThis::Committed);
    registrar.Parameter("matched_states", &TThis::MatchedStates);
    registrar.Parameter("errors", &TThis::Errors)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TControllerExternalStateBundle BuildControllerExternalStateBundle(
    const TFlowViewPtr& flowView,
    const TComputationId& computationId,
    const NClient::NCache::IClientsCachePtr& clientsCache,
    const NYPath::TRichYPath& pipelinePath,
    const IPipelineAuthenticatorPtr& authenticator,
    const IStatusProfilerPtr& statusProfiler,
    const IPayloadConverterCachePtr& converterCache,
    const NLogging::TLogger& logger)
{
    const auto& pipelineSpec = flowView->CurrentSpec->GetValue();
    auto computationIt = pipelineSpec->Computations.find(computationId);
    THROW_ERROR_EXCEPTION_IF(
        computationIt == pipelineSpec->Computations.end(),
        "Unknown computation %Qv",
        computationId);
    const auto& computationSpec = computationIt->second;

    const auto& dynamicPipelineSpec = flowView->CurrentDynamicSpec->GetValue();
    const auto& computationDynamicSpec = GetOrCrash(dynamicPipelineSpec->Computations, computationId);

    TControllerExternalStateBundle bundle;
    bundle.ComputationKeySchema = computationSpec->GroupBySchema;

    auto perRequestInvoker = CreateSerializedInvoker(GetCurrentInvoker());

    bundle.StateCache = New<TStateCache>(New<TDynamicStateCacheSpec>(), NProfiling::TProfiler{});
    auto jobStateCache = bundle.StateCache->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{});

    auto resourceManagerContext = New<TResourceManagerContext>();
    resourceManagerContext->PipelineAuthenticator = authenticator;
    resourceManagerContext->Logger = logger;
    resourceManagerContext->Profiler = NProfiling::TProfiler{};
    resourceManagerContext->StatusProfiler = statusProfiler;
    resourceManagerContext->Invoker = perRequestInvoker;
    bundle.ResourceManager = CreateResourceManager(
        std::move(resourceManagerContext),
        pipelineSpec->Resources,
        dynamicPipelineSpec->Resources);

    THashSet<TResourceId> requiredResourceIds;
    THashMap<TResourceId, IResourcePtr> staticResources;
    for (const auto& [resourceId, resourceDescription] : computationSpec->RequiredResourceIds) {
        if (!resourceDescription->Controller) {
            continue;
        }
        requiredResourceIds.insert(resourceId);
        auto aliasResourceId = resourceDescription->Alias ? *resourceDescription->Alias : resourceId;
        EmplaceOrCrash(staticResources, aliasResourceId, bundle.ResourceManager->Get(resourceId));
    }
    // LoadRequiredResources also awaits the always-on resources (loaded eagerly, outside
    // RequiredResourceIds), so it is called even when requiredResourceIds is empty.
    WaitFor(bundle.ResourceManager->LoadRequiredResources(requiredResourceIds))
        .ThrowOnError();

    for (const auto& [name, spec] : computationSpec->ExternalStateManagers) {
        auto context = New<TExternalStateManagerContext>();
        context->ExternalStateManagerSpec = spec;
        context->StateCache = jobStateCache->WithName(name);
        context->KeySchema = bundle.ComputationKeySchema;
        context->ClientsCache = clientsCache;
        context->PipelinePath = pipelinePath;
        context->SerializedInvoker = perRequestInvoker;
        context->StatusProfiler = statusProfiler;
        context->Logger = logger.WithTag("ExternalStateManager: %v", name);

        auto dynamicContext = New<TDynamicExternalStateManagerContext>();
        auto specIt = computationDynamicSpec->ExternalStateManagers.find(name);
        dynamicContext->DynamicExternalStateManagerSpec =
            (specIt != computationDynamicSpec->ExternalStateManagers.end() && specIt->second)
            ? specIt->second
            : New<TDynamicExternalStateManagerSpec>();

        EmplaceOrCrash(bundle.Managers, name, TControllerExternalStateManagerEntry{
                .Manager = TRegistry::Get()->CreateExternalStateManager(context, dynamicContext),
                                              });
    }
    for (const auto& [name, spec] : computationSpec->ExternalStateJoiners) {
        const auto& joinOnOverride = spec->JoinOn ? spec->JoinOn->KeySchemaOverride : nullptr;
        auto context = New<TExternalStateJoinerContext>();
        context->ExternalStateJoinerSpec = spec;
        context->StateCache = jobStateCache->WithName(name);
        context->KeySchema = joinOnOverride ? joinOnOverride : bundle.ComputationKeySchema;
        context->ConverterCache = converterCache;
        context->ClientsCache = clientsCache;
        context->StaticResources = staticResources;
        context->PipelinePath = pipelinePath;
        context->SerializedInvoker = perRequestInvoker;
        context->StatusProfiler = statusProfiler;
        context->Logger = logger.WithTag("ExternalStateJoiner: %v", name);

        auto dynamicContext = New<TDynamicExternalStateJoinerContext>();
        auto specIt = computationDynamicSpec->ExternalStateJoiners.find(name);
        dynamicContext->DynamicExternalStateJoinerSpec =
            (specIt != computationDynamicSpec->ExternalStateJoiners.end() && specIt->second)
            ? specIt->second
            : New<TDynamicExternalStateJoinerSpec>();

        EmplaceOrCrash(bundle.Joiners, name, TControllerExternalStateJoinerEntry{
                .KeySchema = context->KeySchema,
                .Joiner = TRegistry::Get()->CreateExternalStateJoiner(context, dynamicContext),
                                             });
    }

    return bundle;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// Adapts a Mode-1/2 key to |targetSchema|. Returns:
//   * TError on a hard failure (bad yson, unknown partition, conversion failure);
//   * ``std::nullopt`` to silently skip the entry (Mode 2 with a known partition that has
//     no SourceKey — mirrors the existing key_states behaviour);
//   * The key otherwise.
TErrorOr<std::optional<TKey>> ResolveExactKey(
    const TStateAccessArgs& argument,
    const TFlowViewPtr& flowView,
    const NTableClient::TTableSchemaPtr& computationKeySchema,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache,
    const IPayloadConverterCachePtr& converterCache,
    const NLogging::TLogger& logger)
{
    if (argument.KeyAsYson) {
        try {
            return std::optional<TKey>(BuildKeyFromYson(
                *argument.KeyAsYson,
                targetSchema,
                evaluatorCache,
                logger,
                /*ignoreUnknownColumns*/ true));
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }
    if (argument.PartitionId) {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        auto it = layout->Partitions.find(*argument.PartitionId);
        if (it == layout->Partitions.end()) {
            return TError("Unknown partition %Qv", *argument.PartitionId);
        }
        if (!it->second->SourceKey) {
            return std::optional<TKey>{};
        }
        try {
            return std::optional<TKey>(ConvertKeyToSchema(
                *it->second->SourceKey,
                computationKeySchema,
                targetSchema,
                converterCache));
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }
    return std::optional<TKey>{};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<TExternalStateFilter> BuildExternalStateFilters(
    const TStateAccessArgs& argument,
    const TControllerExternalStateBundle& bundle,
    const TFlowViewPtr& flowView,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache,
    const IPayloadConverterCachePtr& converterCache,
    const NLogging::TLogger& logger)
{
    std::vector<TExternalStateFilter> filters;
    const bool hasExactKeyRequest = argument.KeyAsYson || argument.PartitionId;

    auto addFilter = [&] (const std::string& name, const NTableClient::TTableSchemaPtr& keySchema) {
        if (argument.Name && *argument.Name != name) {
            return;
        }
        TExternalStateFilter filter;
        filter.Name = name;
        if (hasExactKeyRequest) {
            auto keyOrError = ResolveExactKey(
                argument,
                flowView,
                bundle.ComputationKeySchema,
                keySchema,
                evaluatorCache,
                converterCache,
                logger);
            if (!keyOrError.IsOK()) {
                filter.Error = Format("External state %Qv: %v", name, keyOrError);
            } else if (auto& key = keyOrError.Value(); key) {
                filter.ExactKey = std::move(*key);
            } else {
                // Mode 2 with a known partition but no SourceKey — silent skip.
                return;
            }
        }
        filters.push_back(std::move(filter));
    };

    for (const auto& [name, entry] : bundle.Managers) {
        addFilter(name, bundle.ComputationKeySchema);
    }
    for (const auto& [name, entry] : bundle.Joiners) {
        addFilter(name, entry.KeySchema);
    }
    return filters;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// Preloads |keys| into |external| and renders non-empty states into |grouped|. Used by
// both Mode-1/2 (single-key wrap) and Mode-3 (list of keys returned by List).
template <class TExternal>
void FetchExternalKeys(
    const TExternal& external,
    const std::string& name,
    const std::vector<TKey>& keys,
    std::map<TKey, THashMap<std::string, NYson::TYsonString>>& grouped,
    std::vector<std::string>& errors)
{
    if (keys.empty()) {
        return;
    }
    try {
        THashSet<TKey> keySet(keys.begin(), keys.end());
        WaitFor(external->PreloadKeyStates(keySet))
            .ThrowOnError();
        for (const auto& key : keys) {
            auto state = external->GetState(key);
            if (state && !state->IsEmpty()) {
                grouped[key].emplace(name, state->ToYsonView());
            }
        }
    } catch (const std::exception& ex) {
        errors.push_back(Format("External state %Qv read failed: %v", name, TError(ex)));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TReadExternalStatesResult ReadExternalStates(
    const TComputationId& computationId,
    const TControllerExternalStateBundle& bundle,
    const std::vector<TExternalStateFilter>& filters,
    i64 limit,
    const NLogging::TLogger& /*logger*/)
{
    TReadExternalStatesResult result;

    // Group rendered states by realized TKey; multiple externals with the same key share one row.
    std::map<TKey, THashMap<std::string, NYson::TYsonString>> managerGrouped;
    std::map<TKey, THashMap<std::string, NYson::TYsonString>> joinerGrouped;

    // Mode-3 limit fairness: managers and joiners each get their own |limit| budget,
    // split across the range filters within their own section.
    int managerRangeCount = 0;
    int joinerRangeCount = 0;
    for (const auto& filter : filters) {
        if (filter.Error || filter.ExactKey) {
            continue;
        }
        if (bundle.Managers.contains(filter.Name)) {
            ++managerRangeCount;
        } else if (bundle.Joiners.contains(filter.Name)) {
            ++joinerRangeCount;
        }
    }
    const i64 managerPerFilterLimit = managerRangeCount > 0 ? std::max<i64>(1, limit / managerRangeCount) : limit;
    const i64 joinerPerFilterLimit = joinerRangeCount > 0 ? std::max<i64>(1, limit / joinerRangeCount) : limit;

    auto handle = [&] (
        const auto& external,
        const TExternalStateFilter& filter,
        std::map<TKey, THashMap<std::string, NYson::TYsonString>>& grouped,
        i64 perFilterLimit) {
        using TExternalType = std::decay_t<decltype(*external)>;
        using TExternalFilter = typename TExternalType::TFilter;

        std::vector<TKey> keys;
        if (filter.ExactKey) {
            keys.push_back(*filter.ExactKey);
        } else {
            try {
                auto listed = WaitFor(external->List(TExternalFilter{}, perFilterLimit, std::nullopt))
                    .ValueOrThrow();
                keys = std::move(listed.Keys);
            } catch (const std::exception& ex) {
                result.Errors.push_back(Format("External state %Qv list failed: %v", filter.Name, TError(ex)));
                return;
            }
        }
        FetchExternalKeys(external, filter.Name, keys, grouped, result.Errors);
    };

    for (const auto& filter : filters) {
        if (filter.Error) {
            result.Errors.push_back(*filter.Error);
            continue;
        }
        if (auto it = bundle.Managers.find(filter.Name); it != bundle.Managers.end()) {
            handle(it->second.Manager, filter, managerGrouped, managerPerFilterLimit);
        } else if (auto it = bundle.Joiners.find(filter.Name); it != bundle.Joiners.end()) {
            handle(it->second.Joiner, filter, joinerGrouped, joinerPerFilterLimit);
        }
    }

    auto rowify = [&] (auto& grouped, std::vector<TKeyStateRow>& rows) {
        rows.reserve(grouped.size());
        for (auto& [key, states] : grouped) {
            TKeyStateRow row;
            row.ComputationId = computationId;
            row.Key = key;
            row.States = std::move(states);
            rows.push_back(std::move(row));
        }
    };
    rowify(managerGrouped, result.ManagerRows);
    rowify(joinerGrouped, result.JoinerRows);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void RunExternalDeleteSync(
    const IExternalStateManagerPtr& manager,
    const NApi::IClientPtr& client,
    bool commit,
    bool anyMatched)
{
    auto retryableTx = CreateRetryableTransaction();
    manager->Sync(retryableTx);
    if (!commit || !anyMatched) {
        return;
    }
    auto ytTx = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();
    retryableTx->DoAttempt(ytTx);
    WaitFor(ytTx->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDeleteExternalStatesResult DeleteExternalStates(
    const TComputationId& computationId,
    const TControllerExternalStateBundle& bundle,
    const std::vector<TExternalStateFilter>& filters,
    const NApi::IClientPtr& client,
    bool commit,
    const NLogging::TLogger& /*logger*/)
{
    TDeleteExternalStatesResult result;

    auto countMatch = [&] (const std::string& name) {
        result.Matched.Total += 1;
        result.Matched.Details[computationId][name] += 1;
    };

    for (const auto& filter : filters) {
        if (filter.Error) {
            result.Errors.push_back(*filter.Error);
            continue;
        }
        auto it = bundle.Managers.find(filter.Name);
        if (it == bundle.Managers.end()) {
            // Filter is for a joiner — delete is manager-only, silently skip.
            continue;
        }
        const auto& manager = it->second.Manager;

        try {
            if (filter.ExactKey) {
                WaitFor(manager->PreloadKeyStates({*filter.ExactKey}))
                    .ThrowOnError();
                auto state = manager->GetState(*filter.ExactKey);
                bool matched = state && !state->IsEmpty();
                if (matched) {
                    state->Clear();
                }
                RunExternalDeleteSync(manager, client, commit, matched);
                if (matched) {
                    countMatch(filter.Name);
                }
            } else {
                std::optional<TKey> continuation;
                while (true) {
                    auto listed = WaitFor(manager->List(IExternalStateManager::TFilter{}, StateAccessBatchSize, continuation))
                        .ValueOrThrow();
                    if (listed.Keys.empty()) {
                        break;
                    }
                    THashSet<TKey> keys(listed.Keys.begin(), listed.Keys.end());
                    WaitFor(manager->PreloadKeyStates(keys))
                        .ThrowOnError();
                    i64 matchedHere = 0;
                    for (const auto& key : listed.Keys) {
                        auto state = manager->GetState(key);
                        if (state && !state->IsEmpty()) {
                            state->Clear();
                            ++matchedHere;
                        }
                    }
                    RunExternalDeleteSync(manager, client, commit, matchedHere > 0);
                    for (i64 i = 0; i < matchedHere; ++i) {
                        countMatch(filter.Name);
                    }
                    if (!listed.OffsetExclusive) {
                        break;
                    }
                    continuation = std::move(listed.OffsetExclusive);
                }
            }
        } catch (const std::exception& ex) {
            result.Errors.push_back(Format("External state %Qv delete failed: %v", filter.Name, TError(ex)));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
