#include "state_manager.h"

#include "state_providers.h"

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/tables/key_states.h>
#include <yt/yt/flow/library/cpp/tables/partition_states.h>

#include <yt/yt/flow/lib/serializer/state.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TJobStateManager::TJobStateManager(
    TJobStateManagerContextPtr context,
    TDynamicJobStateManagerContextPtr dynamicContext)
    : Context_(std::move(context))
    , DynamicSpec_(dynamicContext ? dynamicContext->StateManager : New<TDynamicStateManagerSpec>())
    , ExternalStateManagers_(CreateExternalStateManagers(Context_, dynamicContext))
    , ExternalStateJoiners_(CreateExternalStateJoiners(Context_, dynamicContext))
{
    YT_VERIFY(Context_->KeyStates);
    YT_VERIFY(Context_->PartitionStates);
    if (dynamicContext) {
        DynamicStateJoiners_ = dynamicContext->StateJoiners;
    }
}

THashMap<std::string, IExternalStateManagerPtr> TJobStateManager::CreateExternalStateManagers(
    const TJobStateManagerContextPtr& context,
    const TDynamicJobStateManagerContextPtr& dynamicContext)
{
    THashMap<std::string, IExternalStateManagerPtr> result;
    for (const auto& [name, externalStateSpec] : context->ExternalStateManagers) {
        auto perManagerContext = CreateExternalStateManagerContext(context, name, externalStateSpec);
        auto perManagerDynamicContext = New<TDynamicExternalStateManagerContext>();
        perManagerDynamicContext->DynamicExternalStateManagerSpec = dynamicContext
            ? GetOrDefault(dynamicContext->ExternalStateManagers, name, TDynamicExternalStateManagerSpecPtr{})
            : TDynamicExternalStateManagerSpecPtr{};
        if (!perManagerDynamicContext->DynamicExternalStateManagerSpec) {
            perManagerDynamicContext->DynamicExternalStateManagerSpec = New<TDynamicExternalStateManagerSpec>();
        }
        EmplaceOrCrash(
            result,
            name,
            TRegistry::Get()->CreateExternalStateManager(perManagerContext, perManagerDynamicContext));
    }
    return result;
}

THashMap<std::string, IExternalStateJoinerPtr> TJobStateManager::CreateExternalStateJoiners(
    const TJobStateManagerContextPtr& context,
    const TDynamicJobStateManagerContextPtr& dynamicContext)
{
    THashMap<std::string, IExternalStateJoinerPtr> result;
    for (const auto& [name, externalStateSpec] : context->ExternalStateJoiners) {
        auto perJoinerContext = CreateExternalStateJoinerContext(context, name, externalStateSpec);
        auto perJoinerDynamicContext = New<TDynamicExternalStateJoinerContext>();
        perJoinerDynamicContext->DynamicExternalStateJoinerSpec = dynamicContext
            ? GetOrDefault(dynamicContext->ExternalStateJoiners, name, TDynamicExternalStateJoinerSpecPtr{})
            : TDynamicExternalStateJoinerSpecPtr{};
        if (!perJoinerDynamicContext->DynamicExternalStateJoinerSpec) {
            perJoinerDynamicContext->DynamicExternalStateJoinerSpec = New<TDynamicExternalStateJoinerSpec>();
        }
        EmplaceOrCrash(
            result,
            name,
            TRegistry::Get()->CreateExternalStateJoiner(perJoinerContext, perJoinerDynamicContext));
    }
    return result;
}

void TJobStateManager::Reconfigure(TDynamicJobStateManagerContextPtr dynamicContext)
{
    DynamicSpec_ = dynamicContext->StateManager;
    auto doReconfigure = [&] (auto& providers, auto getPath) {
        for (auto iter = providers.begin(); iter != providers.end();) {
            if (auto strongProvider = iter->second.Lock()) {
                strongProvider->Reconfigure(GetDynamicSpec(getPath(iter->first)));
                ++iter;
            } else {
                providers.erase(iter++);
            }
        }
    };
    auto identity = [] (const std::string& s) -> const std::string& {
        return s;
    };
    auto pairSecond = [] (const std::pair<TKey, std::string>& p) -> const std::string& {
        return p.second;
    };
    doReconfigure(PartitionMutableStateProviders_, identity);
    doReconfigure(KeyMutableStateProviders_, pairSecond);
    doReconfigure(MutableStateKeyProviders_, identity);

    for (const auto& [name, manager] : ExternalStateManagers_) {
        auto perManagerDynamicContext = New<TDynamicExternalStateManagerContext>();
        auto it = dynamicContext->ExternalStateManagers.find(name);
        perManagerDynamicContext->DynamicExternalStateManagerSpec = it != dynamicContext->ExternalStateManagers.end() && it->second
            ? it->second
            : New<TDynamicExternalStateManagerSpec>();
        manager->Reconfigure(perManagerDynamicContext);
    }
    for (const auto& [name, joiner] : ExternalStateJoiners_) {
        auto perJoinerDynamicContext = New<TDynamicExternalStateJoinerContext>();
        auto it = dynamicContext->ExternalStateJoiners.find(name);
        perJoinerDynamicContext->DynamicExternalStateJoinerSpec = it != dynamicContext->ExternalStateJoiners.end() && it->second
            ? it->second
            : New<TDynamicExternalStateJoinerSpec>();
        joiner->Reconfigure(perJoinerDynamicContext);
    }

    DynamicStateJoiners_ = dynamicContext->StateJoiners;
    for (auto iter = JoinedStateKeyProviders_.begin(); iter != JoinedStateKeyProviders_.end();) {
        if (auto provider = iter->second.Lock()) {
            auto dynamicSpec = GetOrDefault(DynamicStateJoiners_, iter->first);
            if (!dynamicSpec) {
                dynamicSpec = New<TDynamicStateJoinerSpec>();
            }
            provider->Reconfigure(dynamicSpec->Cache);
            ++iter;
        } else {
            JoinedStateKeyProviders_.erase(iter++);
        }
    }
}

bool TJobStateManager::HasPreloadCallbacks() const
{
    if (!MutableStateKeyProviders_.empty() || !ExternalStateManagers_.empty()) {
        return true;
    }
    for (const auto& [name, _] : ExternalStateJoiners_) {
        if (GetOrCrash(Context_->ExternalStateJoiners, name)->AutoPreload) {
            return true;
        }
    }
    for (const auto& [name, weakProvider] : JoinedStateKeyProviders_) {
        if (weakProvider.Lock() && GetOrCrash(Context_->StateJoiners, name)->AutoPreload) {
            return true;
        }
    }
    return false;
}

TFuture<void> TJobStateManager::PreloadKeyStates(const IInputContextPtr& inputContext)
{
    std::vector<TFuture<void>> futures;
    auto defaultKeys = ExtractKeys(inputContext);
    for (const auto& [name, provider] : MutableStateKeyProviders_) {
        if (auto strongProvider = provider.Lock()) {
            futures.push_back(strongProvider->PreloadKeyStates(defaultKeys));
        }
    }
    for (const auto& manager : GetValues(ExternalStateManagers_)) {
        futures.push_back(manager->PreloadKeyStates(defaultKeys));
    }
    for (const auto& [name, joiner] : ExternalStateJoiners_) {
        // Visitor-driven joiners are preloaded with visit keys via #PreloadVisitorDrivenJoiners.
        if (joiner->IsVisitorDriven()) {
            continue;
        }
        const auto& spec = GetOrCrash(Context_->ExternalStateJoiners, name);
        if (!spec->AutoPreload) {
            continue;
        }
        if (!spec->JoinOn->KeySchemaOverride && !spec->JoinOn->KeyProviderStreams) {
            futures.push_back(joiner->PreloadKeyStates(defaultKeys));
            continue;
        }
        auto keys = ExtractKeys(
            inputContext,
            spec->JoinOn->KeySchemaOverride,
            spec->JoinOn->KeyProviderStreams,
            Context_->ConverterCache);
        futures.push_back(joiner->PreloadKeyStates(keys));
    }
    for (const auto& [name, weakProvider] : JoinedStateKeyProviders_) {
        auto provider = weakProvider.Lock();
        if (!provider) {
            continue;
        }
        const auto& spec = GetOrCrash(Context_->StateJoiners, name);
        if (!spec->AutoPreload) {
            continue;
        }
        if (!spec->JoinOn->KeySchemaOverride && !spec->JoinOn->KeyProviderStreams) {
            futures.push_back(provider->PreloadKeyStates(defaultKeys));
            continue;
        }
        auto keys = ExtractKeys(
            inputContext,
            spec->JoinOn->KeySchemaOverride,
            spec->JoinOn->KeyProviderStreams,
            Context_->ConverterCache);
        futures.push_back(provider->PreloadKeyStates(keys));
    }
    return AllSucceeded(futures);
}

void TJobStateManager::Sync(IRetryableTransactionPtr transaction)
{
    auto doSync = [&] (auto& providers) {
        for (auto iter = providers.begin(); iter != providers.end();) {
            if (auto strongProvider = iter->second.Lock()) {
                strongProvider->Sync(transaction);
                ++iter;
            } else {
                providers.erase(iter++);
            }
        }
    };
    doSync(PartitionMutableStateProviders_);
    doSync(KeyMutableStateProviders_);
    doSync(MutableStateKeyProviders_);

    for (const auto& manager : GetValues(ExternalStateManagers_)) {
        manager->Sync(transaction);
    }
    for (const auto& joiner : GetValues(ExternalStateJoiners_)) {
        joiner->Reset();
    }
    for (auto iter = JoinedStateKeyProviders_.begin(); iter != JoinedStateKeyProviders_.end();) {
        if (auto provider = iter->second.Lock()) {
            provider->Reset();
            ++iter;
        } else {
            JoinedStateKeyProviders_.erase(iter++);
        }
    }
}

IJobInitContextPtr TJobStateManager::CreateContext(std::string prefix)
{
    if (!prefix.empty()) {
        prefix = ExtendStateNamePrefix({}, prefix);
    }
    return New<TJobInitContext>(MakeStrong(this), std::move(prefix));
}

IExternalStateManagerPtr TJobStateManager::GetExternalStateManagerOrThrow(const std::string& name) const
{
    auto it = ExternalStateManagers_.find(name);
    if (it == ExternalStateManagers_.end()) {
        THROW_ERROR_EXCEPTION("External state manager %Qv is not declared in computation spec", name);
    }
    return it->second;
}

IExternalStateJoinerPtr TJobStateManager::GetExternalStateJoinerOrThrow(const std::string& name) const
{
    auto it = ExternalStateJoiners_.find(name);
    if (it == ExternalStateJoiners_.end()) {
        THROW_ERROR_EXCEPTION("External state joiner %Qv is not declared in computation spec", name);
    }
    return it->second;
}

const THashMap<std::string, IExternalStateManagerPtr>& TJobStateManager::GetExternalStateManagers() const
{
    return ExternalStateManagers_;
}

const THashMap<std::string, IExternalStateJoinerPtr>& TJobStateManager::GetExternalStateJoiners() const
{
    return ExternalStateJoiners_;
}

TFuture<void> TJobStateManager::PreloadVisitorDrivenJoiners(const THashMap<std::string, THashSet<TKey>>& joinerKeys)
{
    std::vector<TFuture<void>> futures;
    for (const auto& [name, keys] : joinerKeys) {
        const auto& joiner = GetOrCrash(ExternalStateJoiners_, name);
        YT_VERIFY(joiner->IsVisitorDriven());
        futures.push_back(joiner->PreloadKeyStates(keys));
    }
    return AllSucceeded(std::move(futures));
}

TExternalStateManagerContextPtr TJobStateManager::CreateExternalStateManagerContext(
    const TJobStateManagerContextPtr& context,
    const std::string& name,
    const TExternalStateManagerSpecPtr& spec)
{
    auto result = New<TExternalStateManagerContext>();
    result->ExternalStateManagerSpec = spec;
    result->StateCache = context->StateCache ? context->StateCache->WithName(name) : nullptr;
    result->KeySchema = context->KeySchema;
    result->ClientsCache = context->ClientsCache;
    result->PipelinePath = context->PipelinePath;
    result->SerializedInvoker = context->SerializedInvoker;
    result->StatusProfiler = context->StatusProfiler;
    result->Logger = context->Logger.WithTag("ExternalStateManager: %v", name);
    return result;
}

TExternalStateJoinerContextPtr TJobStateManager::CreateExternalStateJoinerContext(
    const TJobStateManagerContextPtr& context,
    const std::string& name,
    const TExternalStateJoinerSpecPtr& spec)
{
    auto result = New<TExternalStateJoinerContext>();
    result->ExternalStateJoinerSpec = spec;
    result->StateCache = context->StateCache ? context->StateCache->WithName(name) : nullptr;
    // ``join_on/key_schema_override`` wins over the computation's group-by key schema —
    // used when the joiner's state table is keyed on columns different from the
    // computation's group-by key.
    result->KeySchema = spec->JoinOn->KeySchemaOverride ? spec->JoinOn->KeySchemaOverride : context->KeySchema;
    result->ConverterCache = context->ConverterCache;
    result->ClientsCache = context->ClientsCache;
    result->StaticResources = context->StaticResources;
    result->PipelinePath = context->PipelinePath;
    result->SerializedInvoker = context->SerializedInvoker;
    result->StatusProfiler = context->StatusProfiler->WithPrefix(Format("/external_state_joiner/%v", name));
    result->Profiler = context->Profiler.WithTag("external_state_joiner", name);
    result->Logger = context->Logger.WithTag("ExternalStateJoiner: %v", name);
    return result;
}

TFuture<IMutableStateProviderPtr> TJobStateManager::CreatePartitionMutableStateProvider(std::string path, std::function<IStateHolderPtr()> ctor)
{
    YT_VERIFY(!PartitionMutableStateProviders_.contains(path));
    auto provider = New<TPartitionMutableStateProvider>(
        CreateStateClientContext(path),
        Context_->PartitionId,
        path,
        std::move(ctor));
    provider->Reconfigure(GetDynamicSpec(path));
    PartitionMutableStateProviders_[path] = MakeWeak(provider);
    return provider->Init()
        .Apply(BIND([provider] (const TErrorOr<void>& result) -> IMutableStateProviderPtr {
            result.ThrowOnError();
            return provider;
        }));
}

TFuture<IMutableStateProviderPtr> TJobStateManager::CreateKeyMutableStateProvider(TKey key, std::string path, std::function<IStateHolderPtr()> ctor)
{
    // Forbid duplicate (key, name) pairs within KeyMutableStateProviders_.
    // Cross-type: same name may be used for partition and key clients (different tables).
    auto mapKey = std::pair{key, path};
    YT_VERIFY(!KeyMutableStateProviders_.contains(mapKey));
    auto provider = New<TKeyMutableStateProvider>(
        CreateStateClientContext(path),
        key,
        path,
        std::move(ctor));
    provider->Reconfigure(GetDynamicSpec(path));
    KeyMutableStateProviders_[mapKey] = MakeWeak(provider);
    return provider->Init()
        .Apply(BIND([provider] (const TErrorOr<void>& result) -> IMutableStateProviderPtr {
            result.ThrowOnError();
            return provider;
        }));
}

TFuture<IMutableStateKeyProviderPtr> TJobStateManager::CreateMutableStateKeyProvider(std::string path, std::function<IStateHolderPtr()> ctor)
{
    YT_VERIFY(!MutableStateKeyProviders_.contains(path));
    auto provider = New<TJobMutableStateKeyProvider>(
        CreateStateClientContext(path),
        path,
        std::move(ctor));
    provider->Reconfigure(GetDynamicSpec(path));
    MutableStateKeyProviders_[path] = MakeWeak(provider);
    return provider->Init()
        .Apply(BIND([provider] (const TErrorOr<void>& result) -> IMutableStateKeyProviderPtr {
            result.ThrowOnError();
            return provider;
        }));
}

TFuture<IJoinedStateKeyProviderPtr> TJobStateManager::CreateJoinedStateKeyProvider(std::string name, std::function<IStateHolderPtr()> ctor)
{
    auto it = Context_->StateJoiners.find(name);
    if (it == Context_->StateJoiners.end()) {
        THROW_ERROR_EXCEPTION("State joiner %Qv is not declared in computation spec", name);
    }
    const auto& spec = it->second;
    YT_VERIFY(!JoinedStateKeyProviders_.contains(name));
    auto dynamicSpec = GetOrDefault(DynamicStateJoiners_, name);
    if (!dynamicSpec) {
        dynamicSpec = New<TDynamicStateJoinerSpec>();
    }
    auto provider = New<TJobJoinedStateKeyProvider>(
        CreateStateJoinerClientContext(name, spec),
        spec->StateName,
        std::move(ctor),
        Context_->ConverterCache,
        spec->JoinOn->KeyProviderStreams,
        spec->JoinOn->KeySchemaOverride != nullptr,
        dynamicSpec->Cache);
    JoinedStateKeyProviders_[name] = MakeWeak(provider);
    return provider->Init()
        .Apply(BIND([provider] (const TErrorOr<void>& result) -> IJoinedStateKeyProviderPtr {
            result.ThrowOnError();
            return provider;
        }));
}

TJobStateClientContextPtr TJobStateManager::CreateStateJoinerClientContext(
    const std::string& name,
    const TStateJoinerSpecPtr& spec) const
{
    auto context = New<TJobStateClientContext>();
    context->ComputationId = spec->ComputationId;
    // The lookup key is the target computation's group-by key; ``key_schema_override`` names
    // which of our own columns carry it (defaulting to our own group-by key when omitted).
    context->KeySchema = spec->JoinOn->KeySchemaOverride ? spec->JoinOn->KeySchemaOverride : Context_->KeySchema;
    context->Logger = Context_->Logger.WithTag("StateJoiner: %v", name);
    context->Profiler = Context_->Profiler.WithTag("state_joiner", name);
    // The joiner's TTL cache is keyed by this client name, so it never collides with the
    // owning computation's own state cache.
    if (Context_->StateCache) {
        context->StateCache = Context_->StateCache->WithName(name);
    }
    auto tag = Format("%v:states:%v", spec->ComputationId, name);
    context->KeyStates = New<NTables::TTaggedKeyStates>(Context_->KeyStates, tag);
    context->PartitionStates = New<NTables::TTaggedPartitionStates>(Context_->PartitionStates, tag);
    return context;
}

TJobStateClientContextPtr TJobStateManager::CreateStateClientContext(const std::string& name) const
{
    auto context = New<TJobStateClientContext>();
    context->ComputationId = Context_->ComputationId;
    context->KeySchema = Context_->KeySchema;
    context->Logger = Context_->Logger.WithTag("State: %v", name);
    context->Profiler = Context_->Profiler.WithTag("state", name);
    if (Context_->StateCache) {
        context->StateCache = Context_->StateCache->WithName(name);
    }
    // Wrap the shared tables with per-client tags for throttling/profiling.
    auto tag = Format("%v:states:%v", Context_->ComputationId, name);
    context->KeyStates = New<NTables::TTaggedKeyStates>(Context_->KeyStates, tag);
    context->PartitionStates = New<NTables::TTaggedPartitionStates>(Context_->PartitionStates, tag);
    return context;
}

TDynamicStateSpecPtr TJobStateManager::GetDynamicSpec(const std::string& path) const
{
    auto it = DynamicSpec_->FormatOverrides.find(path);
    if (it == DynamicSpec_->FormatOverrides.end()) {
        return DynamicSpec_;
    }
    auto spec = New<TDynamicStateSpec>();
    spec->Format = it->second;
    spec->TableRequest = DynamicSpec_->TableRequest;
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

TJobInitContext::TJobInitContext(
    TJobStateManagerPtr stateManager,
    std::string prefix)
    : StateManager_(std::move(stateManager))
    , Prefix_(std::move(prefix))
{ }

TFuture<IMutableStateKeyProviderPtr> TJobInitContext::CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const
{
    return StateManager_->CreateMutableStateKeyProvider(GetPrefix(), std::move(ctor));
}

TFuture<IJoinedStateKeyProviderPtr> TJobInitContext::CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const
{
    return StateManager_->CreateJoinedStateKeyProvider(GetPrefix(), std::move(ctor));
}

IInitContextPtr TJobInitContext::AsPartition() const
{
    return New<TPartitionInitContext>(StateManager_, Prefix_);
}

IInitContextPtr TJobInitContext::AsKey(TKey key) const
{
    return New<TKeyInitContext>(StateManager_, Prefix_, std::move(key));
}

IJobInitContextPtr TJobInitContext::WithPrefix(TStringBuf prefix) const
{
    return New<TJobInitContext>(StateManager_, ExtendStateNamePrefix(Prefix_, prefix));
}

const std::string& TJobInitContext::GetPrefix() const
{
    return Prefix_;
}

IExternalStateManagerPtr TJobInitContext::GetExternalStateManagerOrThrow(const std::string& name) const
{
    return StateManager_->GetExternalStateManagerOrThrow(name);
}

IExternalStateJoinerPtr TJobInitContext::GetExternalStateJoinerOrThrow(const std::string& name) const
{
    return StateManager_->GetExternalStateJoinerOrThrow(name);
}

////////////////////////////////////////////////////////////////////////////////

TKeyInitContext::TKeyInitContext(
    TJobStateManagerPtr stateManager,
    std::string prefix,
    TKey key)
    : StateManager_(std::move(stateManager))
    , Prefix_(std::move(prefix))
    , Key_(std::move(key))
{ }

TFuture<IMutableStateProviderPtr> TKeyInitContext::CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const
{
    return StateManager_->CreateKeyMutableStateProvider(Key_, GetPrefix(), std::move(ctor));
}

IInitContextPtr TKeyInitContext::WithPrefix(TStringBuf prefix) const
{
    return New<TKeyInitContext>(StateManager_, ExtendStateNamePrefix(Prefix_, prefix), Key_);
}

const std::string& TKeyInitContext::GetPrefix() const
{
    return Prefix_;
}

////////////////////////////////////////////////////////////////////////////////

TPartitionInitContext::TPartitionInitContext(
    TJobStateManagerPtr stateManager,
    std::string prefix)
    : StateManager_(std::move(stateManager))
    , Prefix_(std::move(prefix))
{ }

TFuture<IMutableStateProviderPtr> TPartitionInitContext::CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const
{
    return StateManager_->CreatePartitionMutableStateProvider(GetPrefix(), std::move(ctor));
}

IInitContextPtr TPartitionInitContext::WithPrefix(TStringBuf prefix) const
{
    return New<TPartitionInitContext>(StateManager_, ExtendStateNamePrefix(Prefix_, prefix));
}

const std::string& TPartitionInitContext::GetPrefix() const
{
    return Prefix_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
