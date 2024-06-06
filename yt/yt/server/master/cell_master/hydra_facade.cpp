#include "hydra_facade.h"
#include "private.h"
#include "automaton.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/distributed_election_manager.h>
#include <yt/yt/server/lib/election/election_manager_thunk.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/epoch.h>
#include <yt/yt/server/lib/hydra/snapshot.h>
#include <yt/yt/server/lib/hydra/private.h>
#include <yt/yt/server/lib/hydra/local_hydra_janitor.h>
#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/dry_run_hydra_manager.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/server/master/object_server/object.h>
#include <yt/yt/server/master/object_server/private.h>

#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/fair_share_action_queue.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NRpc;
using namespace NElection;
using namespace NHiveServer;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class THydraFacade
    : public IHydraFacade
{
public:
    THydraFacade(TTestingTag)
        : Bootstrap_(nullptr)
    { }

    THydraFacade(TBootstrap* bootstrap)
        : Config_(bootstrap->GetConfig())
        , Bootstrap_(bootstrap)
    {
        AutomatonQueue_ = CreateEnumIndexedFairShareActionQueue<EAutomatonThreadQueue>(
            "Automaton",
            GetAutomatonThreadBuckets());
        VERIFY_INVOKER_THREAD_AFFINITY(AutomatonQueue_->GetInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        NObjectServer::SetupMasterBootstrap(bootstrap);

        BIND([this, this_ = MakeStrong(this)] {
            NObjectServer::SetupAutomatonThread();
            NObjectServer::SetupEpochContext(EpochContext_);
        })
            .AsyncVia(AutomatonQueue_->GetInvoker(EAutomatonThreadQueue::Default))
            .Run()
            .Get()
            .ThrowOnError();

        Automaton_ = New<TMasterAutomaton>(Bootstrap_);

        TransactionTrackerQueue_ = New<TActionQueue>("TxTracker");

        ResponseKeeper_ = CreatePersistentResponseKeeper(
            NObjectServer::ObjectServerLogger(),
            NObjectServer::ObjectServerProfiler);

        auto electionManagerThunk = New<TElectionManagerThunk>();

        TDistributedHydraManagerOptions hydraManagerOptions{
            .UseFork = true,
            .ResponseKeeper = ResponseKeeper_
        };
        if (Config_->DryRun->EnableDryRun) {
            hydraManagerOptions.UseFork = false;

            HydraManager_ = NHydra::CreateDryRunHydraManager(
                Config_->HydraManager,
                Bootstrap_->GetControlInvoker(),
                GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
                Automaton_,
                Bootstrap_->GetSnapshotStore(),
                hydraManagerOptions,
                Bootstrap_->GetCellManager());
        } else {
            HydraManager_ = NHydra::CreateDistributedHydraManager(
                Config_->HydraManager,
                Bootstrap_->GetControlInvoker(),
                GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
                Automaton_,
                Bootstrap_->GetRpcServer(),
                electionManagerThunk,
                Bootstrap_->GetCellManager()->GetCellId(),
                Bootstrap_->GetChangelogStoreFactory(),
                Bootstrap_->GetSnapshotStore(),
                Bootstrap_->GetNativeAuthenticator(),
                hydraManagerOptions);
        }

        HydraManager_->SubscribeStartLeading(BIND_NO_PROPAGATE(&THydraFacade::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopLeading(BIND_NO_PROPAGATE(&THydraFacade::OnStopEpoch, MakeWeak(this)));

        HydraManager_->SubscribeStartFollowing(BIND_NO_PROPAGATE(&THydraFacade::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopFollowing(BIND_NO_PROPAGATE(&THydraFacade::OnStopEpoch, MakeWeak(this)));

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
            Bootstrap_->GetNativeAuthenticator());

        electionManagerThunk->SetUnderlying(ElectionManager_);

        LocalJanitor_ = CreateLocalHydraJanitor(
            Config_->Snapshots->Path,
            Config_->Changelogs->Path,
            Config_->HydraManager,
            Bootstrap_->GetSnapshotIOInvoker());
    }

    void Initialize() override
    {
        ElectionManager_->Initialize();

        HydraManager_->Initialize();

        LocalJanitor_->Initialize();
    }

    const TMasterAutomatonPtr& GetAutomaton() override
    {
        return Automaton_;
    }

    const IElectionManagerPtr& GetElectionManager() override
    {
        return ElectionManager_;
    }

    const IHydraManagerPtr& GetHydraManager() override
    {
        return HydraManager_;
    }

    const IPersistentResponseKeeperPtr& GetResponseKeeper() override
    {
        return ResponseKeeper_;
    }

    const ILocalHydraJanitorPtr& GetLocalJanitor() override
    {
        return LocalJanitor_;
    }

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) override
    {
        return AutomatonQueue_->GetInvoker(queue);
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) override
    {
        return EpochInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) override
    {
        return GuardedInvokers_[queue];
    }


    IInvokerPtr GetTransactionTrackerInvoker() override
    {
        return TransactionTrackerQueue_->GetInvoker();
    }

    IThreadPoolPtr GetSnapshotSaveBackgroundThreadPool() override
    {
        // NB: We must be lazy here since this call is made in a forked child and all parent threads vanish upon fork.
        if (!SnapshotSaveBackgroundThreadPool_ && Config_->HydraManager->SnapshotBackgroundThreadCount > 0) {
            SnapshotSaveBackgroundThreadPool_ = CreateThreadPool(Config_->HydraManager->SnapshotBackgroundThreadCount, "SnapSaveBack");
        }
        return SnapshotSaveBackgroundThreadPool_;
    }

    IThreadPoolPtr GetSnapshotLoadBackgroundThreadPool() override
    {
        // This is just for symmetry with GetSnapshotSaveBackgroundThreadPool.
        if (!SnapshotLoadBackgroundThreadPool_ && Config_->HydraManager->SnapshotBackgroundThreadCount > 0) {
            SnapshotLoadBackgroundThreadPool_ = CreateThreadPool(Config_->HydraManager->SnapshotBackgroundThreadCount, "SnapLoadBack");
        }
        return SnapshotLoadBackgroundThreadPool_;
    }

    const NObjectServer::TEpochContextPtr& GetEpochContext() override
    {
        return EpochContext_;
    }

    void BlockAutomaton() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_ASSERT(!AutomatonBlocked_);
        AutomatonBlocked_ = true;

        YT_LOG_TRACE("Automaton thread blocked");
    }

    void UnblockAutomaton() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_ASSERT(AutomatonBlocked_);
        AutomatonBlocked_ = false;

        YT_LOG_TRACE("Automaton thread unblocked");
    }

    bool IsAutomatonLocked() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return AutomatonBlocked_;
    }

    void VerifyPersistentStateRead() override
    {
#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        if (IsAutomatonLocked()) {
            auto automatonThreadId = AutomatonThread_Slot.GetBoundThreadId();
            YT_VERIFY(automatonThreadId != NThreading::InvalidThreadId);
            YT_VERIFY(GetCurrentThreadId() != automatonThreadId);
        } else {
            VERIFY_THREAD_AFFINITY(AutomatonThread);
        }
#endif
    }

    void RequireLeader() override
    {
        if (!HydraManager_->IsLeader()) {
            if (HasMutationContext()) {
                // Just a precaution, not really expected to happen.
                auto error = TError("Request can only be served at leaders");
                YT_LOG_ALERT(error);
                THROW_ERROR error;
            } else {
                throw TLeaderFallbackException();
            }
        }
    }

    void Reconfigure(const TDynamicCellMasterConfigPtr& newConfig) override
    {
        AutomatonQueue_->Reconfigure(newConfig->AutomatonThreadBucketWeights);
    }

    IInvokerPtr CreateEpochInvoker(IInvokerPtr underlyingInvoker) override
    {
        VerifyPersistentStateRead();

        return EpochCancelableContext_->CreateInvoker(std::move(underlyingInvoker));
    }

    void CommitMutationWithSemaphore(
        const TAsyncSemaphorePtr& semaphore,
        NRpc::IServiceContextPtr context,
        TCallback<std::unique_ptr<TMutation>()> mutationBuilder,
        TCallback<void(const NHydra::TMutationResponse& response)> replyCallback) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochId = GetCurrentEpochId();

        auto timeBefore = NProfiling::GetInstant();

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto expectedMutationCommitDuration = config->CellMaster->ExpectedMutationCommitDuration;

        semaphore->AsyncAcquire().SubscribeUnique(
            BIND(
                [
                    =,
                    context = std::move(context),
                    mutationBuilder = std::move(mutationBuilder),
                    replyCallback = std::move(replyCallback)
                ] (TErrorOr<TAsyncSemaphoreGuard>&& guardOrError) {
                    if (!guardOrError.IsOK()) {
                        context->Reply(TError("Failed to acquire semaphore") << guardOrError);
                        return;
                    }

                    auto requestTimeout = context->GetTimeout();
                    auto timeAfter = NProfiling::GetInstant();
                    if (requestTimeout && timeAfter + expectedMutationCommitDuration >= timeBefore + *requestTimeout) {
                        context->Reply(TError(NYT::EErrorCode::Timeout, "Semaphore acquisition took too long"));
                        return;
                    }

                    auto mutation = mutationBuilder();
                    mutation->SetEpochId(epochId);

                    auto result = WaitFor(mutation->Commit());
                    if (!result.IsOK()) {
                        context->Reply(result);
                        return;
                    }

                    const auto& mutationResponse = result.Value();
                    if (replyCallback) {
                        replyCallback(mutationResponse);
                    } else {
                        context->Reply(mutationResponse.Data);
                    }
                }).Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const TCellMasterConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    IElectionManagerPtr ElectionManager_;

    IEnumIndexedFairShareActionQueuePtr<EAutomatonThreadQueue> AutomatonQueue_;
    TMasterAutomatonPtr Automaton_;
    IHydraManagerPtr HydraManager_;

    TActionQueuePtr TransactionTrackerQueue_;

    IThreadPoolPtr SnapshotSaveBackgroundThreadPool_;
    IThreadPoolPtr SnapshotLoadBackgroundThreadPool_;

    IPersistentResponseKeeperPtr ResponseKeeper_;

    ILocalHydraJanitorPtr LocalJanitor_;

    TEnumIndexedArray<EAutomatonThreadQueue, IInvokerPtr> GuardedInvokers_;
    TEnumIndexedArray<EAutomatonThreadQueue, IInvokerPtr> EpochInvokers_;

    NObjectServer::TEpochContextPtr EpochContext_ = New<NObjectServer::TEpochContext>();

    std::atomic<bool> AutomatonBlocked_ = false;

    TCancelableContextPtr EpochCancelableContext_;

    void OnStartEpoch()
    {
        EpochCancelableContext_ = HydraManager_->GetAutomatonCancelableContext();
        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            EpochInvokers_[queue] = CreateEpochInvoker(GetAutomatonInvoker(queue));
        }

        NObjectServer::BeginEpoch();
    }

    void OnStopEpoch()
    {
        std::fill(EpochInvokers_.begin(), EpochInvokers_.end(), nullptr);

        NObjectServer::EndEpoch();
        EpochCancelableContext_.Reset();
    }

    static THashMap<EAutomatonThreadBucket, std::vector<EAutomatonThreadQueue>> GetAutomatonThreadBuckets()
    {
        THashMap<EAutomatonThreadBucket, std::vector<EAutomatonThreadQueue>> buckets;
        buckets[EAutomatonThreadBucket::Gossips] = {
            EAutomatonThreadQueue::TabletGossip,
            EAutomatonThreadQueue::NodeTrackerGossip,
            EAutomatonThreadQueue::MulticellGossip,
            EAutomatonThreadQueue::SecurityGossip,
        };
        buckets[EAutomatonThreadBucket::ChunkMaintenance] = {
            EAutomatonThreadQueue::ChunkRefresher,
            EAutomatonThreadQueue::ChunkRequisitionUpdater,
            EAutomatonThreadQueue::ChunkSealer,
        };
        buckets[EAutomatonThreadBucket::Transactions] = {
            EAutomatonThreadQueue::TransactionSupervisor,
            EAutomatonThreadQueue::CypressTransactionService,
            EAutomatonThreadQueue::TransactionService,
        };
        return buckets;
    }
};

////////////////////////////////////////////////////////////////////////////////

IHydraFacadePtr CreateHydraFacade(TBootstrap* bootstrap)
{
    return New<THydraFacade>(bootstrap);
}

IHydraFacadePtr CreateHydraFacade(TTestingTag tag)
{
    return New<THydraFacade>(tag);
}

////////////////////////////////////////////////////////////////////////////////

TAutomatonBlockGuard::TAutomatonBlockGuard(IHydraFacadePtr hydraFacade)
    : HydraFacade_(std::move(hydraFacade))
{
    HydraFacade_->BlockAutomaton();
}

TAutomatonBlockGuard::~TAutomatonBlockGuard()
{
    HydraFacade_->UnblockAutomaton();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
