#include "stdafx.h"
#include "hydra_facade.h"
#include "automaton.h"
#include "config.h"

#include <core/misc/fs.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/ypath_client.h>

#include <core/ypath/token.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/server.h>
#include <core/rpc/transient_response_keeper.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/log.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/election/cell_manager.h>

#include <server/election/election_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/changelog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/distributed_hydra_manager.h>
#include <server/hydra/file_helpers.h>
#include <server/hydra/private.h>

#include <server/hive/transaction_supervisor.h>

#include <server/cell_master/bootstrap.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/node_detail.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/acl.h>
#include <server/security_server/group.h>

#include <server/object_server/private.h>

namespace NYT {
namespace NCellMaster {

using namespace NConcurrency;
using namespace NRpc;
using namespace NElection;
using namespace NHydra;
using namespace NYTree;
using namespace NYPath;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NHive;
using namespace NHive::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");

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

        HydraManager_ = CreateDistributedHydraManager(
            Config_->HydraManager,
            Bootstrap_->GetControlInvoker(),
            GetAutomatonInvoker(EAutomatonThreadQueue::Default),
            Automaton_,
            Bootstrap_->GetRpcServer(),
            Bootstrap_->GetCellManager(),
            Bootstrap_->GetChangelogStore(),
            Bootstrap_->GetSnapshotStore());

        HydraManager_->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));

        HydraManager_->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            auto unguardedInvoker = GetAutomatonInvoker(queue);
            GuardedInvokers_[queue] = HydraManager_->CreateGuardedAutomatonInvoker(unguardedInvoker);
        }

        ResponseKeeper_ = CreateTransientResponseKeeper(
            Config_->HydraManager->ResponseKeeper,
            NHydra::HydraProfiler);
    }

    void Start()
    {
        HydraManager_->Initialize();

        SnapshotCleanupExecutor_ = New<TPeriodicExecutor>(
            GetHydraIOInvoker(),
            BIND(&TImpl::OnSnapshotCleanup, MakeWeak(this)),
            SnapshotCleanupPeriod);
        SnapshotCleanupExecutor_->Start();
    }

    TMasterAutomatonPtr GetAutomaton() const
    {
        return Automaton_;
    }

    IHydraManagerPtr GetHydraManager() const
    {
        return HydraManager_;
    }

    IResponseKeeperPtr GetResponseKeeper() const
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

    void ValidateActiveLeader()
    {
        if (!HydraManager_->IsActiveLeader()) {
            throw TNotALeaderException()
                <<= ERROR_SOURCE_LOCATION()
                >>= TError(NRpc::EErrorCode::Unavailable, "Not an active leader");
        }
    }

private:
    TCellMasterConfigPtr Config_;
    TBootstrap* Bootstrap_;

    TFairShareActionQueuePtr AutomatonQueue_;
    TMasterAutomatonPtr Automaton_;
    IHydraManagerPtr HydraManager_;

    IResponseKeeperPtr ResponseKeeper_;

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> GuardedInvokers_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochInvokers_;

    TPeriodicExecutorPtr SnapshotCleanupExecutor_;


    void OnStartEpoch()
    {
        auto cancelableContext = HydraManager_
            ->GetAutomatonEpochContext()
            ->CancelableContext;

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
                    NFS::Remove(fileName);
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

void THydraFacade::Start()
{
    Impl_->Start();
}

TMasterAutomatonPtr THydraFacade::GetAutomaton() const
{
    return Impl_->GetAutomaton();
}

IHydraManagerPtr THydraFacade::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

IResponseKeeperPtr THydraFacade::GetResponseKeeper() const
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

void THydraFacade::ValidateActiveLeader()
{
    return Impl_->ValidateActiveLeader();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

