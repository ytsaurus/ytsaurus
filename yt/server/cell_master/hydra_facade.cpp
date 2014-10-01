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

#include <core/concurrency/scheduler.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/log.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/election/cell_manager.h>

#include <server/election/election_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/changelog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/distributed_hydra_manager.h>
#include <server/hydra/sync_file_changelog.h>
#include <server/hydra/persistent_response_keeper.h>

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
using namespace NTransactionClient::NProto;
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

        AutomatonQueue_ = New<TFairShareActionQueue>("Automaton", EAutomatonThreadQueue::GetDomainNames());
        Automaton_ = New<TMasterAutomaton>(Bootstrap_);

        HydraManager_ = CreateDistributedHydraManager(
            Config_->HydraManager,
            Bootstrap_->GetControlInvoker(),
            AutomatonQueue_->GetInvoker(EAutomatonThreadQueue::Default),
            Automaton_,
            Bootstrap_->GetRpcServer(),
            Bootstrap_->GetCellManager(),
            Bootstrap_->GetChangelogStore(),
            Bootstrap_->GetSnapshotStore());

        HydraManager_->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));

        HydraManager_->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        for (int index = 0; index < EAutomatonThreadQueue::GetDomainSize(); ++index) {
            auto unguardedInvoker = AutomatonQueue_->GetInvoker(index);
            GuardedInvokers_.push_back(HydraManager_->CreateGuardedAutomatonInvoker(unguardedInvoker));
        }

        ResponseKeeper_ = New<TPersistentResponseKeeper>(
            Config_->HydraManager->ResponseKeeper,
            GetAutomatonInvoker(),
            HydraManager_,
            Automaton_,
            NHydra::HydraProfiler);
    }

    void Start()
    {
        HydraManager_->Start();

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
        return ResponseKeeper_->GetResponseKeeper();
    }

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return AutomatonQueue_->GetInvoker(queue);
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

    TPersistentResponseKeeperPtr ResponseKeeper_;

    std::vector<IInvokerPtr> GuardedInvokers_;
    std::vector<IInvokerPtr> EpochInvokers_;

    TPeriodicExecutorPtr SnapshotCleanupExecutor_;


    void OnStartEpoch()
    {
        YCHECK(EpochInvokers_.empty());

        auto cancelableContext = HydraManager_
            ->GetAutomatonEpochContext()
            ->CancelableContext;
        for (int index = 0; index < EAutomatonThreadQueue::GetDomainSize(); ++index) {
            EpochInvokers_.push_back(cancelableContext->CreateInvoker(AutomatonQueue_->GetInvoker(index)));
        }
    }

    void OnStopEpoch()
    {
        EpochInvokers_.clear();
    }


    void OnSnapshotCleanup()
    {
        auto snapshotsPath = Config_->Snapshots->Path;

        std::vector<int> snapshotIds;
        auto snapshotFileNames = NFS::EnumerateFiles(snapshotsPath);
        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;
            try {
                int snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                snapshotIds.push_back(snapshotId);
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %Qv in snapshot store",
                    fileName);
            }
        }

        if (snapshotIds.size() <= Config_->HydraManager->MaxSnapshotsToKeep)
            return;

        std::sort(snapshotIds.begin(), snapshotIds.end());
        int thresholdId = snapshotIds[snapshotIds.size() - Config_->HydraManager->MaxSnapshotsToKeep];

        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            try {
                int snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                if (snapshotId < thresholdId) {
                    LOG_INFO("Removing snapshot %v",
                        snapshotId);

                    auto dataFile = NFS::CombinePaths(snapshotsPath, fileName);
                    NFS::Remove(dataFile);
                }
            } catch (const std::exception& ex) {
                // Ignore, cf. logging above.
            }
        }

        auto changelogsPath = Config_->Changelogs->Path;
        auto changelogFileNames = NFS::EnumerateFiles(changelogsPath);
        for (const auto& fileName : changelogFileNames) {
            if (NFS::GetFileExtension(fileName) != ChangelogExtension)
                continue;

            try {
                int changelogId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                if (changelogId < thresholdId) {
                    LOG_INFO("Removing changelog %v",
                        changelogId);
                    RemoveChangelogFiles(NFS::CombinePaths(changelogsPath, fileName));
                }
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %Qv in changelog store",
                    fileName);
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

