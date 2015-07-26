#include "stdafx.h"
#include "hydra_facade.h"
#include "automaton.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/server.h>
#include <core/rpc/response_keeper.h>
#include <core/rpc/message.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/periodic_executor.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <server/election/election_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/changelog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/distributed_hydra_manager.h>
#include <server/hydra/file_helpers.h>
#include <server/hydra/private.h>

#include <server/hive/hive_manager.h>

#include <server/cell_master/bootstrap.h>

#include <server/object_server/private.h>

namespace NYT {
namespace NCellMaster {

using namespace NConcurrency;
using namespace NRpc;
using namespace NElection;
using namespace NHydra;
using namespace NHive;

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

        ResponseKeeper_ = New<TResponseKeeper>(
            Config_->HydraManager->ResponseKeeper,
            NObjectServer::ObjectServerLogger,
            NObjectServer::ObjectServerProfiler);

        TDistributedHydraManagerOptions hydraManagerOptions;
        hydraManagerOptions.ResponseKeeper = ResponseKeeper_;
        hydraManagerOptions.UseFork = true;
        HydraManager_ = CreateDistributedHydraManager(
            Config_->HydraManager,
            Bootstrap_->GetControlInvoker(),
            GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
            Automaton_,
            Bootstrap_->GetRpcServer(),
            Bootstrap_->GetCellManager(),
            Bootstrap_->GetChangelogStore(),
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
    }

    void Initialize()
    {
        if (Bootstrap_->IsSecondaryMaster()) {
            HydraManager_->SubscribeUpstreamSync(BIND(
                &THiveManager::SyncWith,
                Bootstrap_->GetHiveManager(),
                Bootstrap_->GetPrimaryCellId()));
        }

        HydraManager_->Initialize();

        SnapshotCleanupExecutor_ = New<TPeriodicExecutor>(
            GetHydraIOInvoker(),
            BIND(&TImpl::OnSnapshotCleanup, MakeWeak(this)),
            SnapshotCleanupPeriod);
        SnapshotCleanupExecutor_->Start();
    }

    void DumpSnapshot(ISnapshotReaderPtr reader)
    {
        WaitFor(reader->Open())
            .ThrowOnError();

        Automaton_->SetSerializationDumpEnabled(true);
        Automaton_->Clear();
        Automaton_->LoadSnapshot(reader);
    }


    TMasterAutomatonPtr GetAutomaton() const
    {
        return Automaton_;
    }

    IHydraManagerPtr GetHydraManager() const
    {
        return HydraManager_;
    }

    TResponseKeeperPtr GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }


    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return AutomatonQueue_->GetInvoker(static_cast<int>(queue));
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return EpochInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return GuardedInvokers_[queue];
    }

private:
    const TCellMasterConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    TFairShareActionQueuePtr AutomatonQueue_;
    TMasterAutomatonPtr Automaton_;
    IHydraManagerPtr HydraManager_;

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
        auto snapshotsPath = Config_->Snapshots->Path;

        std::vector<int> snapshotIds;
        auto snapshotFileNames = NFS::EnumerateFiles(snapshotsPath);
        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %v in snapshot store",
                    fileName);
                continue;
            }
            snapshotIds.push_back(snapshotId);
        }

        if (snapshotIds.size() <= Config_->HydraManager->MaxSnapshotsToKeep)
            return;

        std::sort(snapshotIds.begin(), snapshotIds.end());
        int thresholdId = snapshotIds[snapshotIds.size() - Config_->HydraManager->MaxSnapshotsToKeep];

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

            if (snapshotId < thresholdId) {
                LOG_INFO("Removing snapshot %v",
                    snapshotId);

                try {
                    NFS::Remove(NFS::CombinePaths(snapshotsPath, fileName));
                } catch (const std::exception& ex) {
                    LOG_WARNING(ex, "Error removing %v from snapshot store",
                        fileName);
                }
            }
        }

        auto changelogsPath = Config_->Changelogs->Path;
        auto changelogFileNames = NFS::EnumerateFiles(changelogsPath);
        for (const auto& fileName : changelogFileNames) {
            if (NFS::GetFileExtension(fileName) != ChangelogExtension)
                continue;

            int changelogId;
            try {
                changelogId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %v in changelog store",
                    fileName);
                continue;
            }

            if (changelogId < thresholdId) {
                LOG_INFO("Removing changelog %v",
                    changelogId);
                try {
                    RemoveChangelogFiles(NFS::CombinePaths(changelogsPath, fileName));
                } catch (const std::exception& ex) {
                    LOG_WARNING(ex, "Error removing %v from changelog store",
                        fileName);
                }
            }
        }
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

void THydraFacade::DumpSnapshot(ISnapshotReaderPtr reader)
{
    Impl_->DumpSnapshot(reader);
}

TMasterAutomatonPtr THydraFacade::GetAutomaton() const
{
    return Impl_->GetAutomaton();
}

IHydraManagerPtr THydraFacade::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

TResponseKeeperPtr THydraFacade::GetResponseKeeper() const
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

} // namespace NCellMaster
} // namespace NYT

