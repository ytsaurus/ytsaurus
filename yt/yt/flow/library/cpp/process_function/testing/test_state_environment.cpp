#include "test_state_environment.h"

#include <yt/yt/flow/library/cpp/process_function/host/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_states.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/partition_states.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

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

    TExternalAwareInitContext(
        IRuntimeInitContextPtr underlying,
        std::shared_ptr<TExternalManagerMap> externalManagers,
        std::shared_ptr<TExternalJoinerMap> externalJoiners)
        : Underlying_(std::move(underlying))
        , ExternalManagers_(std::move(externalManagers))
        , ExternalJoiners_(std::move(externalJoiners))
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
        return New<TExternalAwareInitContext>(Underlying_->WithPrefix(prefix), ExternalManagers_, ExternalJoiners_);
    }

    const std::string& GetPrefix() const override
    {
        return Underlying_->GetPrefix();
    }

    NYTree::IMapNodePtr GetParametersNode() const override
    {
        return Underlying_->GetParametersNode();
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
    InitContext_ = New<TExternalAwareInitContext>(
        New<TRuntimeInitContext>(StateManager_->CreateContext(), StateManager_),
        ExternalManagers_,
        ExternalJoiners_);
}

void TTestStateEnvironment::SetStaticParametersNode(NYTree::IMapNodePtr node)
{
    InitContext_ = New<TExternalAwareInitContext>(
        New<TRuntimeInitContext>(StateManager_->CreateContext(), StateManager_, std::move(node)),
        ExternalManagers_,
        ExternalJoiners_);
}

IRuntimeInitContextPtr TTestStateEnvironment::MakeReloadedInitContext()
{
    Sync();

    auto dynamicContext = New<TDynamicJobStateManagerContext>();
    dynamicContext->StateManager = New<TDynamicStateManagerSpec>();
    auto manager = New<TJobStateManager>(ManagerContext_, std::move(dynamicContext));
    return New<TRuntimeInitContext>(manager->CreateContext(), manager);
}

const IRuntimeInitContextPtr& TTestStateEnvironment::GetInitContext() const
{
    return InitContext_;
}

const TJobStateManagerPtr& TTestStateEnvironment::GetStateManager() const
{
    return StateManager_;
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
