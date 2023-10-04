#include "automaton.h"
#include "bootstrap.h"
#include "config.h"
#include "hydra_facade.h"
#include "private.h"

#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/distributed_election_manager.h>
#include <yt/yt/server/lib/election/election_manager_thunk.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/composite_automaton.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>
#include <yt/yt/server/lib/hydra_common/local_hydra_janitor.h>

#include <yt/yt/server/lib/hydra2/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra2/private.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/fair_share_action_queue.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NClusterClock {

using namespace NConcurrency;
using namespace NRpc;
using namespace NElection;
using namespace NHydra;
using namespace NHiveServer;

////////////////////////////////////////////////////////////////////////////////

class THydraFacade::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TClusterClockConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);

        // TODO(gepardo): Possibly add authentication here.

        AutomatonQueue_ = CreateEnumIndexedFairShareActionQueue<EAutomatonThreadQueue>("Automaton");
        Automaton_ = New<TClockAutomaton>(Bootstrap_);

        ResponseKeeper_ = CreateResponseKeeper(
            Config_->HydraManager->ResponseKeeper,
            GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            ClusterClockLogger,
            ClusterClockProfiler);

        auto electionManagerThunk = New<TElectionManagerThunk>();

        TDistributedHydraManagerOptions hydraManagerOptions;
        hydraManagerOptions.ResponseKeeper = ResponseKeeper_;
        hydraManagerOptions.UseFork = true;
        HydraManager_ = NHydra2::CreateDistributedHydraManager(
            Config_->HydraManager,
            Bootstrap_->GetControlInvoker(),
            GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
            Automaton_,
            Bootstrap_->GetRpcServer(),
            electionManagerThunk,
            Bootstrap_->GetCellManager()->GetCellId(),
            Bootstrap_->GetChangelogStoreFactory(),
            Bootstrap_->GetSnapshotStore(),
            /*authenticator*/ nullptr,
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
            Bootstrap_->GetRpcServer(),
            /*authenticator*/ nullptr);

        electionManagerThunk->SetUnderlying(ElectionManager_);

        LocalJanitor_ = CreateLocalHydraJanitor(
            Config_->Snapshots->Path,
            Config_->Changelogs->Path,
            Config_->HydraManager,
            Bootstrap_->GetSnapshotIOInvoker());
    }

    void Initialize()
    {
        ElectionManager_->Initialize();

        HydraManager_->Initialize();

        LocalJanitor_->Start();
    }

    void LoadSnapshot(ISnapshotReaderPtr reader, bool dump)
    {
        WaitFor(reader->Open())
            .ThrowOnError();

        Automaton_->SetSerializationDumpEnabled(dump);
        Automaton_->Clear();
        TSnapshotLoadContext context{
            .Reader = reader,
        };
        Automaton_->LoadSnapshot(context);
    }


    const TClockAutomatonPtr& GetAutomaton() const
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

    const IResponseKeeperPtr& GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }


    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) const
    {
        return AutomatonQueue_->GetInvoker(queue);
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const
    {
        return EpochInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const
    {
        return GuardedInvokers_[queue];
    }

private:
    const TClusterClockConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    IElectionManagerPtr ElectionManager_;

    IEnumIndexedFairShareActionQueuePtr<EAutomatonThreadQueue> AutomatonQueue_;
    TClockAutomatonPtr Automaton_;
    IHydraManagerPtr HydraManager_;

    IResponseKeeperPtr ResponseKeeper_;

    ILocalHydraJanitorPtr LocalJanitor_;

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
    TClusterClockConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

THydraFacade::~THydraFacade() = default;

void THydraFacade::Initialize()
{
    Impl_->Initialize();
}

void THydraFacade::LoadSnapshot(ISnapshotReaderPtr reader, bool dump)
{
    Impl_->LoadSnapshot(reader, dump);
}

const TClockAutomatonPtr& THydraFacade::GetAutomaton() const
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

const IResponseKeeperPtr& THydraFacade::GetResponseKeeper() const
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
