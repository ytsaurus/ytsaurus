#include "hydra_facade.h"
#include "private.h"
#include "automaton.h"
#include "config.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/cypress_manager.h>
#include <yt/server/cypress_server/node_detail.h>

#include <yt/server/election/election_manager.h>
#include <yt/server/election/distributed_election_manager.h>
#include <yt/server/election/election_manager_thunk.h>

#include <yt/server/hive/transaction_supervisor.h>
#include <yt/server/hive/hive_manager.h>

#include <yt/server/hydra/changelog.h>
#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/distributed_hydra_manager.h>
#include <yt/server/hydra/file_helpers.h>
#include <yt/server/hydra/private.h>
#include <yt/server/hydra/snapshot.h>

#include <yt/server/object_server/private.h>

#include <yt/server/security_server/acl.h>
#include <yt/server/security_server/group.h>
#include <yt/server/security_server/security_manager.h>

#include <yt/server/hydra/snapshot_quota_helpers.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/fair_share_action_queue.h>

#include <yt/core/misc/fs.h>

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
static const auto SnapshotCleanupPeriod = TDuration::Seconds(10);

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
        YCHECK(Config_);
        YCHECK(Bootstrap_);

        AutomatonQueue_ = New<TFairShareActionQueue>("Automaton", TEnumTraits<EAutomatonThreadQueue>::GetDomainNames());
        Automaton_ = New<TMasterAutomaton>(Bootstrap_);

        TransactionTrackerQueue_ = New<TActionQueue>("TxTracker");

        ResponseKeeper_ = New<TResponseKeeper>(
            Config_->HydraManager->ResponseKeeper,
            GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
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
            Bootstrap_->GetCellManager(),
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
        ElectionManager_->Initialize();

        electionManagerThunk->SetUnderlying(ElectionManager_);
    }

    void Initialize()
    {
        if (Bootstrap_->IsSecondaryMaster()) {
            // NB: This causes a cyclic reference but we don't care.
            HydraManager_->SubscribeUpstreamSync(BIND(&TImpl::OnUpstreamSync, MakeStrong(this)));
        }

        HydraManager_->Initialize();

        SnapshotCleanupExecutor_ = New<TPeriodicExecutor>(
            GetHydraIOInvoker(),
            BIND(&TImpl::OnSnapshotCleanup, MakeWeak(this)),
            SnapshotCleanupPeriod);
        SnapshotCleanupExecutor_->Start();
    }

    void LoadSnapshot(ISnapshotReaderPtr reader, bool dump)
    {
        WaitFor(reader->Open())
            .ThrowOnError();

        Automaton_->SetSerializationDumpEnabled(dump);
        Automaton_->Clear();
        Automaton_->LoadSnapshot(reader);
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

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> GuardedInvokers_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochInvokers_;

    TPeriodicExecutorPtr SnapshotCleanupExecutor_;


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


    void OnSnapshotCleanup()
    {
        const auto& snapshotsPath = Config_->Snapshots->Path;
        std::vector<TSnapshotInfo> snapshots;
        auto snapshotFileNames = NFS::EnumerateFiles(snapshotsPath);
        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            i64 snapshotSize;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                snapshotSize = NFS::GetFileStatistics(NFS::CombinePaths(snapshotsPath, fileName)).Size;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING("Unrecognized item %v in snapshot store",
                    fileName);
                continue;
            }
            snapshots.push_back({snapshotId, snapshotSize});
        }

        auto thresholdId = NHydra::GetSnapshotThresholdId(
            snapshots,
            Config_->HydraManager->MaxSnapshotCountToKeep,
            Config_->HydraManager->MaxSnapshotSizeToKeep);

        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                // Ignore, cf. logging above.
                continue;
            }

            if (snapshotId <= thresholdId) {
                YT_LOG_INFO("Removing snapshot %v",
                    snapshotId);

                try {
                    NFS::Remove(NFS::CombinePaths(snapshotsPath, fileName));
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Error removing %v from snapshot store",
                        fileName);
                }
            }
        }

        const auto& changelogsPath = Config_->Changelogs->Path;
        auto changelogFileNames = NFS::EnumerateFiles(changelogsPath);
        for (const auto& fileName : changelogFileNames) {
            if (NFS::GetFileExtension(fileName) != ChangelogExtension)
                continue;

            int changelogId;
            try {
                changelogId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                YT_LOG_WARNING("Unrecognized item %v in changelog store",
                    fileName);
                continue;
            }

            if (changelogId <= thresholdId) {
                YT_LOG_INFO("Removing changelog %v",
                    changelogId);
                try {
                    RemoveChangelogFiles(NFS::CombinePaths(changelogsPath, fileName));
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Error removing %v from changelog store",
                        fileName);
                }
            }
        }
    }


    TFuture<void> OnUpstreamSync()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto* mailbox = multicellManager->FindPrimaryMasterMailbox();
        if (!mailbox) {
            return VoidFuture;
        }
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        return hiveManager->SyncWith(mailbox);
    }
};

////////////////////////////////////////////////////////////////////////////////

THydraFacade::THydraFacade(
    TCellMasterConfigPtr config,
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

