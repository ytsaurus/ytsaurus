#include "hydra_facade.h"
#include "private.h"
#include "automaton.h"
#include "config.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/node_detail.h>

#include <yt/server/lib/election/election_manager.h>
#include <yt/server/lib/election/distributed_election_manager.h>
#include <yt/server/lib/election/election_manager_thunk.h>

#include <yt/server/lib/hive/transaction_supervisor.h>
#include <yt/server/lib/hive/hive_manager.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/composite_automaton.h>
#include <yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/server/lib/hydra/local_hydra_janitor.h>
#include <yt/server/lib/hydra/private.h>
#include <yt/server/lib/hydra/snapshot.h>

#include <yt/server/master/object_server/private.h>

#include <yt/server/master/security_server/acl.h>
#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/security_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/fair_share_action_queue.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NRpc;
using namespace NElection;
using namespace NHydra;
using namespace NHiveServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class THydraFacade::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);

        AutomatonQueue_ = New<TFairShareActionQueue>("Automaton", TEnumTraits<EAutomatonThreadQueue>::GetDomainNames());
        Automaton_ = New<TMasterAutomaton>(Bootstrap_);

        TransactionTrackerQueue_ = New<TActionQueue>("TxTracker");

        ResponseKeeper_ = New<TResponseKeeper>(
            Config_->HydraManager->ResponseKeeper,
            GetAutomatonInvoker(EAutomatonThreadQueue::ResponseKeeper),
            NObjectServer::ObjectServerLogger,
            NObjectServer::ObjectServerProfiler);

        auto electionManagerThunk = New<TElectionManagerThunk>();

        TDistributedHydraManagerOptions hydraManagerOptions;
        hydraManagerOptions.ResponseKeeper = ResponseKeeper_;
        hydraManagerOptions.UseFork = true;
        HydraManager_ = CreateDistributedHydraManager(
            Config_->HydraManager,
            Bootstrap_->GetControlInvoker(),
            GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
            Automaton_,
            Bootstrap_->GetRpcServer(),
            electionManagerThunk,
            Bootstrap_->GetCellManager()->GetCellId(),
            Bootstrap_->GetChangelogStoreFactory(),
            Bootstrap_->GetSnapshotStore(),
            hydraManagerOptions);

        HydraManager_->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        HydraManager_->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            auto unguardedInvoker = GetAutomatonInvoker(queue);
            GuardedInvokers_[queue] = HydraManager_->CreateGuardedAutomatonInvoker(unguardedInvoker);
        }

        ElectionManager_ = CreateDistributedElectionManager(
            Config_->ElectionManager,
            Bootstrap_->GetCellManager(),
            Bootstrap_->GetControlInvoker(),
            HydraManager_->GetElectionCallbacks(),
            Bootstrap_->GetRpcServer());

        electionManagerThunk->SetUnderlying(ElectionManager_);

        LocalJanitor_ = New<TLocalHydraJanitor>(
            Config_->Snapshots->Path,
            Config_->Changelogs->Path,
            Config_->HydraManager,
            GetHydraIOInvoker());
    }

    void Initialize()
    {
        ElectionManager_->Initialize();

        HydraManager_->Initialize();

        LocalJanitor_->Start();
    }

    void LoadSnapshot(
        ISnapshotReaderPtr reader,
        bool dump,
        bool enableTotalWriteCountReport,
        const TSerializationDumperConfigPtr& dumpConfig)
    {
        WaitFor(reader->Open())
            .ThrowOnError();

        Automaton_->SetSerializationDumpEnabled(dump);
        Automaton_->SetEnableTotalWriteCountReport(enableTotalWriteCountReport);
        if (dumpConfig) {
            Automaton_->SetLowerWriteCountDumpLimit(dumpConfig->LowerLimit);
            Automaton_->SetUpperWriteCountDumpLimit(dumpConfig->UpperLimit);
        }
        HydraManager_->ValidateSnapshot(reader);
    }

    const TMasterAutomatonPtr& GetAutomaton() const
    {
        return Automaton_;
    }

    const IElectionManagerPtr& GetElectionManager() const
    {
        return ElectionManager_;
    }

    const IHydraManagerPtr& GetHydraManager() const
    {
        return HydraManager_;
    }

    const TResponseKeeperPtr& GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }


    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) const
    {
        return AutomatonQueue_->GetInvoker(static_cast<int>(queue));
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const
    {
        return EpochInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const
    {
        return GuardedInvokers_[queue];
    }


    IInvokerPtr GetTransactionTrackerInvoker() const
    {
        return TransactionTrackerQueue_->GetInvoker();
    }


    void RequireLeader() const
    {
        if (!HydraManager_->IsLeader()) {
            if (HasMutationContext()) {
                // Just a precaution, not really expected to happen.
                auto error = TError("Request can only be served at leaders");
                YT_LOG_ERROR_UNLESS(HydraManager_->IsRecovery(), error, "Unexpected error");
                THROW_ERROR error;
            } else {
                throw TLeaderFallbackException();
            }
        }
    }

private:
    const TCellMasterConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    IElectionManagerPtr ElectionManager_;

    TFairShareActionQueuePtr AutomatonQueue_;
    TMasterAutomatonPtr Automaton_;
    IHydraManagerPtr HydraManager_;

    TActionQueuePtr TransactionTrackerQueue_;

    TResponseKeeperPtr ResponseKeeper_;

    TLocalHydraJanitorPtr LocalJanitor_;

    TEnumIndexedVector<EAutomatonThreadQueue, IInvokerPtr> GuardedInvokers_;
    TEnumIndexedVector<EAutomatonThreadQueue, IInvokerPtr> EpochInvokers_;


    void OnStartEpoch()
    {
        auto cancelableContext = HydraManager_->GetAutomatonCancelableContext();
        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            auto unguardedInvoker = GetAutomatonInvoker(queue);
            EpochInvokers_[queue] = cancelableContext->CreateInvoker(unguardedInvoker);
        }
    }

    void OnStopEpoch()
    {
        std::fill(EpochInvokers_.begin(), EpochInvokers_.end(), nullptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

THydraFacade::THydraFacade(
    TCellMasterConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

THydraFacade::~THydraFacade()
{ }

void THydraFacade::Initialize()
{
    Impl_->Initialize();
}

void THydraFacade::LoadSnapshot(
    ISnapshotReaderPtr reader,
    bool dump,
    bool enableTotalWriteCountReport,
    const TSerializationDumperConfigPtr& dumpConfig)
{
    Impl_->LoadSnapshot(reader, dump, enableTotalWriteCountReport, dumpConfig);
}

const TMasterAutomatonPtr& THydraFacade::GetAutomaton() const
{
    return Impl_->GetAutomaton();
}

const IElectionManagerPtr& THydraFacade::GetElectionManager() const
{
    return Impl_->GetElectionManager();
}

const IHydraManagerPtr& THydraFacade::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

const TResponseKeeperPtr& THydraFacade::GetResponseKeeper() const
{
    return Impl_->GetResponseKeeper();
}

IInvokerPtr THydraFacade::GetAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetAutomatonInvoker(queue);
}

IInvokerPtr THydraFacade::GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetEpochAutomatonInvoker(queue);
}

IInvokerPtr THydraFacade::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetGuardedAutomatonInvoker(queue);
}

IInvokerPtr THydraFacade::GetTransactionTrackerInvoker() const
{
    return Impl_->GetTransactionTrackerInvoker();
}

void THydraFacade::RequireLeader() const
{
    Impl_->RequireLeader();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

