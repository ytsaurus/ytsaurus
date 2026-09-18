#include "test_state_environment.h"

#include <yt/yt/flow/library/cpp/process_function/host/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_states.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/partition_states.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Init context that delegates internal-state operations to a real TRuntimeInitContext but
//! resolves external state managers and joiners from in-memory maps shared with
//! TTestStateEnvironment.
class TExternalAwareInitContext
    : public IRuntimeInitContext
{
public:
    using TExternalManagerMap = THashMap<std::string, IExternalStateManagerPtr>;
    using TExternalJoinerMap = THashMap<std::string, IExternalStateJoinerPtr>;
    using TStaticResourceMap = THashMap<TResourceId, IResourcePtr>;

    TExternalAwareInitContext(
        IRuntimeInitContextPtr underlying,
        std::shared_ptr<TExternalManagerMap> externalManagers,
        std::shared_ptr<TExternalJoinerMap> externalJoiners,
        std::shared_ptr<TStaticResourceMap> staticResources)
        : Underlying_(std::move(underlying))
        , ExternalManagers_(std::move(externalManagers))
        , ExternalJoiners_(std::move(externalJoiners))
        , StaticResources_(std::move(staticResources))
    { }

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override
    {
        return Underlying_->CreateMutableStateKeyProvider(std::move(ctor));
    }

    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override
    {
        return Underlying_->CreateJoinedStateKeyProvider(std::move(ctor));
    }

    IInitContextPtr AsPartition() const override
    {
        return Underlying_->AsPartition();
    }

    IInitContextPtr AsKey(TKey key) const override
    {
        return Underlying_->AsKey(std::move(key));
    }

    IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const override
    {
        return New<TExternalAwareInitContext>(Underlying_->WithPrefix(prefix), ExternalManagers_, ExternalJoiners_, StaticResources_);
    }

    const std::string& GetPrefix() const override
    {
        return Underlying_->GetPrefix();
    }

    NYTree::IMapNodePtr GetParametersNode() const override
    {
        return Underlying_->GetParametersNode();
    }

    NYTree::TYsonStructPtr GetParametersObject() const override
    {
        return Underlying_->GetParametersObject();
    }

    IResourcePtr GetStaticResource(const TResourceId& resourceId) const override
    {
        auto iter = StaticResources_->find(resourceId);
        if (iter == StaticResources_->end()) {
            THROW_ERROR_EXCEPTION("Static resource %Qv is not registered in the test environment", resourceId);
        }
        return iter->second;
    }

    NProfiling::TProfiler GetProfiler() const override
    {
        return Underlying_->GetProfiler();
    }

    NHttp::IClientPtr GetHttpClient() const override
    {
        return Underlying_->GetHttpClient();
    }

    NHttp::IClientPtr GetHttpsClient() const override
    {
        return Underlying_->GetHttpsClient();
    }

    TPartitionId GetPartitionId() const override
    {
        return Underlying_->GetPartitionId();
    }

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const override
    {
        auto it = ExternalManagers_->find(name);
        if (it == ExternalManagers_->end()) {
            THROW_ERROR_EXCEPTION("External state manager %Qv is not registered in the test environment", name);
        }
        return it->second;
    }

    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const override
    {
        auto it = ExternalJoiners_->find(name);
        if (it == ExternalJoiners_->end()) {
            THROW_ERROR_EXCEPTION("External state joiner %Qv is not registered in the test environment", name);
        }
        return it->second;
    }

private:
    const IRuntimeInitContextPtr Underlying_;
    const std::shared_ptr<TExternalManagerMap> ExternalManagers_;
    const std::shared_ptr<TExternalJoinerMap> ExternalJoiners_;
    const std::shared_ptr<TStaticResourceMap> StaticResources_;
};

class TTestClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TTestClientsCache(NApi::IClientPtr client)
        : Client_(std::move(client))
    { }

    NApi::IClientPtr GetClient(TStringBuf /*cluster*/) override
    {
        return Client_;
    }

private:
    const NApi::IClientPtr Client_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTestStateEnvironment::TTestStateEnvironment(NTableClient::TTableSchemaPtr keySchema)
{
    ManagerContext_ = New<TJobStateManagerContext>();
    ManagerContext_->ComputationId = TComputationId("test-computation");
    ManagerContext_->PartitionId = TPartitionId(TGuid::Create());
    ManagerContext_->Logger = NLogging::TLogger("ProcessFunctionTest");
    ManagerContext_->Profiler = NProfiling::TProfiler();
    ManagerContext_->KeyStates = New<NTables::TInMemoryKeyStates>();
    ManagerContext_->PartitionStates = New<NTables::TInMemoryPartitionStates>();
    ManagerContext_->KeySchema = std::move(keySchema);
    // StateCache is null — cache disabled.

    auto dynamicContext = New<TDynamicJobStateManagerContext>();
    dynamicContext->StateManager = New<TDynamicStateManagerSpec>();

    StateManager_ = New<TJobStateManager>(ManagerContext_, std::move(dynamicContext));
    ExternalManagers_ = std::make_shared<TExternalManagerMap>();
    ExternalJoiners_ = std::make_shared<TExternalJoinerMap>();
    StaticResources_ = std::make_shared<TStaticResourceMap>();
    Logger_ = ManagerContext_->Logger;
    StatusProfiler_ = CreateSyncStatusProfiler(Logger_);
    ClientsCache_ = New<TTestClientsCache>(New<NApi::TMockClient>());
    Invoker_ = GetSyncInvoker();
    PrimaryRetryableClient_ = CreateRetryableClient(
        ClientsCache_->GetClient("primary"),
        Invoker_,
        StatusProfiler_->WithPrefix("/retryable_client"),
        Logger_)
        ->WithErrorComponent("/process_function/default");
    RebuildInitContext();
}

void TTestStateEnvironment::SetStaticParameters(const NYTree::TYsonStructPtr& parameters)
{
    EnsureProcessFunctionContextMutable();
    StaticParametersNode_ = NYTree::ConvertTo<NYTree::IMapNodePtr>(parameters);
    StaticParametersObject_ = parameters;
    RebuildInitContext();
}

void TTestStateEnvironment::SetProfiler(NProfiling::TProfiler profiler)
{
    EnsureProcessFunctionContextMutable();
    Profiler_ = std::move(profiler);
    RebuildInitContext();
}

void TTestStateEnvironment::SetHttpClient(NHttp::IClientPtr client)
{
    EnsureProcessFunctionContextMutable();
    HttpClient_ = std::move(client);
    RebuildInitContext();
}

void TTestStateEnvironment::SetHttpsClient(NHttp::IClientPtr client)
{
    EnsureProcessFunctionContextMutable();
    HttpsClient_ = std::move(client);
    RebuildInitContext();
}

void TTestStateEnvironment::SetLogger(NLogging::TLogger logger)
{
    EnsureProcessFunctionContextMutable();
    Logger_ = std::move(logger);
}

void TTestStateEnvironment::SetStatusProfiler(IStatusProfilerPtr statusProfiler)
{
    EnsureProcessFunctionContextMutable();
    StatusProfiler_ = std::move(statusProfiler);
}

void TTestStateEnvironment::SetClientsCache(NClient::NCache::IClientsCachePtr clientsCache)
{
    EnsureProcessFunctionContextMutable();
    ClientsCache_ = std::move(clientsCache);
}

void TTestStateEnvironment::SetInvoker(IInvokerPtr invoker)
{
    EnsureProcessFunctionContextMutable();
    Invoker_ = std::move(invoker);
}

void TTestStateEnvironment::SetPrimaryRetryableClient(IRetryableClientPtr client)
{
    EnsureProcessFunctionContextMutable();
    PrimaryRetryableClient_ = std::move(client);
}

TProcessFunctionContextPtr TTestStateEnvironment::CreateProcessFunctionContext()
{
    ProcessFunctionContextFrozen_ = true;

    auto context = New<TProcessFunctionContext>();
    context->InitContext = InitContext_;
    context->ClientsCache = ClientsCache_;
    context->Invoker = Invoker_;
    context->RetryableClient = PrimaryRetryableClient_;
    context->Logger = Logger_;
    context->StatusProfiler = StatusProfiler_;
    return context;
}

void TTestStateEnvironment::InitProcessFunction(const IProcessFunctionBasePtr& function)
{
    function->Init(InitContext_);
}

void TTestStateEnvironment::InitProcessFunction(
    const IProcessFunctionBasePtr& function,
    const TProcessFunctionContextPtr& context)
{
    function->Init(context->InitContext);
}

void TTestStateEnvironment::EnsureProcessFunctionContextMutable() const
{
    THROW_ERROR_EXCEPTION_IF(
        ProcessFunctionContextFrozen_,
        "Process-function context dependencies must be configured before Create<T>()");
}

void TTestStateEnvironment::RebuildInitContext()
{
    InitContext_ = New<TExternalAwareInitContext>(
        New<TRuntimeInitContext>(
            StateManager_->CreateContext(),
            StateManager_,
            ManagerContext_->PartitionId,
            StaticParametersNode_,
            StaticParametersObject_,
            /*staticResources*/ THashMap<TResourceId, IResourcePtr>{},
            Profiler_,
            HttpClient_,
            HttpsClient_),
        ExternalManagers_,
        ExternalJoiners_,
        StaticResources_);
}

IRuntimeInitContextPtr TTestStateEnvironment::MakeReloadedInitContext()
{
    Sync();

    auto dynamicContext = New<TDynamicJobStateManagerContext>();
    dynamicContext->StateManager = New<TDynamicStateManagerSpec>();
    auto manager = New<TJobStateManager>(ManagerContext_, std::move(dynamicContext));
    return New<TRuntimeInitContext>(manager->CreateContext(), manager, ManagerContext_->PartitionId);
}

const IRuntimeInitContextPtr& TTestStateEnvironment::GetInitContext() const
{
    return InitContext_;
}

const TJobStateManagerPtr& TTestStateEnvironment::GetStateManager() const
{
    return StateManager_;
}

TPartitionId TTestStateEnvironment::GetPartitionId() const
{
    return ManagerContext_->PartitionId;
}

void TTestStateEnvironment::Sync()
{
    StateManager_->Sync(/*transaction*/ nullptr);
}

void TTestStateEnvironment::PreloadKeyStates(const IInputContextPtr& inputContext)
{
    NConcurrency::WaitFor(StateManager_->PreloadKeyStates(inputContext))
        .ThrowOnError();
}

void TTestStateEnvironment::PreloadEpoch(const IInputContextPtr& input)
{
    PreloadKeyStates(input);

    auto keys = ExtractKeys(input);
    for (const auto& [name, manager] : *ExternalManagers_) {
        NConcurrency::WaitFor(manager->PreloadKeyStates(keys)).ThrowOnError();
    }
}

void TTestStateEnvironment::CommitEpoch(const std::function<void(const IRetryableTransactionPtr&)>& enrollSync)
{
    StateManager_->Sync(/*transaction*/ nullptr);

    if (EpochCommits_.empty()) {
        // No external store; still exercise the sync enrollment.
        if (enrollSync) {
            enrollSync(CreateRetryableTransaction());
        }
        return;
    }

    // Each store commits on its own client, so enroll the sync writes into the first store only —
    // enrolling into every store would double-commit them.
    bool syncEnrolled = false;
    for (const auto& commit : EpochCommits_) {
        auto transaction = CreateRetryableTransaction();
        if (enrollSync && !syncEnrolled) {
            enrollSync(transaction);
            syncEnrolled = true;
        }
        commit(transaction);
    }
}

void TTestStateEnvironment::RegisterEpochCommit(std::function<void(const IRetryableTransactionPtr&)> commit)
{
    EpochCommits_.push_back(std::move(commit));
}

void TTestStateEnvironment::RegisterStaticResource(const TResourceId& resourceId, IResourcePtr resource)
{
    EmplaceOrCrash(*StaticResources_, resourceId, std::move(resource));
}

void TTestStateEnvironment::RegisterExternalState(TStringBuf name, IExternalStateManagerPtr manager)
{
    // The init context resolves managers by GetPrefix(), which equals the prefix the
    // function reaches via WithPrefix(name) from the root (empty) prefix.
    auto prefix = ExtendStateNamePrefix(/*prefix*/ {}, name);
    EmplaceOrCrash(*ExternalManagers_, prefix, std::move(manager));
}

TInMemorySimpleExternalStateManagerPtr TTestStateEnvironment::RegisterExternalState(
    TStringBuf name,
    NTableClient::TTableSchemaPtr stateSchema,
    NTableClient::TTableSchemaPtr keySchema)
{
    auto manager = New<TInMemorySimpleExternalStateManager>(std::move(stateSchema), std::move(keySchema));
    RegisterExternalState(name, manager);
    // Ends the manager's epoch with the harness epoch, as the worker's Sync would.
    RegisterEpochCommit([manager] (const IRetryableTransactionPtr& transaction) {
        manager->Sync(transaction);
    });
    return manager;
}

void TTestStateEnvironment::RegisterExternalStateJoiner(TStringBuf name, IExternalStateJoinerPtr joiner)
{
    auto prefix = ExtendStateNamePrefix(/*prefix*/ {}, name);
    EmplaceOrCrash(*ExternalJoiners_, prefix, std::move(joiner));
}

TInMemorySimpleExternalStateJoinerPtr TTestStateEnvironment::RegisterExternalStateJoiner(
    TStringBuf name,
    NTableClient::TTableSchemaPtr stateSchema,
    NTableClient::TTableSchemaPtr keySchema)
{
    auto joiner = New<TInMemorySimpleExternalStateJoiner>(std::move(stateSchema), std::move(keySchema));
    RegisterExternalStateJoiner(name, joiner);
    return joiner;
}

TStateJoinerSpecPtr TTestStateEnvironment::RegisterStateJoiner(
    TStringBuf name,
    TStringBuf stateName,
    NTableClient::TTableSchemaPtr keySchemaOverride)
{
    auto spec = New<TStateJoinerSpec>();
    // Read this environment's own computation's state, so the producing function (run against
    // the same environment) writes exactly what the joiner reads back.
    spec->ComputationId = ManagerContext_->ComputationId;
    spec->StateName = std::string(stateName);
    spec->JoinOn = New<TStateJoinSpec>();
    spec->JoinOn->KeySchemaOverride = std::move(keySchemaOverride);
    spec->AutoPreload = false;
    EmplaceOrCrash(ManagerContext_->StateJoiners, ExtendStateNamePrefix(/*prefix*/ {}, name), spec);
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
