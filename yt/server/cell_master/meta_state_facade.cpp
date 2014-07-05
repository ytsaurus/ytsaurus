#include "stdafx.h"
#include "meta_state_facade.h"
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

#include <server/hive/transaction_supervisor.h>

#include <server/cell_master/bootstrap.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/node_detail.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/acl.h>
#include <server/security_server/group.h>

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

static const auto CleanupPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

class TMetaStateFacade::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap)
        : Config(config)
        , Bootstrap(bootstrap)
    {
        YCHECK(Config);
        YCHECK(Bootstrap);

        AutomatonQueue = New<TFairShareActionQueue>("Automaton", EAutomatonThreadQueue::GetDomainNames());
        Automaton = New<TMasterAutomaton>(Bootstrap);

        HydraManager = CreateDistributedHydraManager(
            Config->HydraManager,
            Bootstrap->GetControlInvoker(),
            AutomatonQueue->GetInvoker(EAutomatonThreadQueue::Default),
            Automaton,
            Bootstrap->GetRpcServer(),
            Bootstrap->GetCellManager(),
            Bootstrap->GetChangelogStore(),
            Bootstrap->GetSnapshotStore());

        HydraManager->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));

        HydraManager->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
        HydraManager->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        for (int index = 0; index < EAutomatonThreadQueue::GetDomainSize(); ++index) {
            auto unguardedInvoker = AutomatonQueue->GetInvoker(index);
            GuardedInvokers.push_back(HydraManager->CreateGuardedAutomatonInvoker(unguardedInvoker));
        }

    }

    void Start()
    {
        HydraManager->Start();

        CleanupExecutor = New<TPeriodicExecutor>(
            GetHydraIOInvoker(),
            BIND(&TImpl::OnCleanup, MakeWeak(this)),
            CleanupPeriod);
        CleanupExecutor->Start();
    }

    TMasterAutomatonPtr GetAutomaton() const
    {
        return Automaton;
    }

    IHydraManagerPtr GetManager() const
    {
        return HydraManager;
    }

    IInvokerPtr GetInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return AutomatonQueue->GetInvoker(queue);
    }

    IInvokerPtr GetEpochInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return EpochInvokers[queue];
    }

    IInvokerPtr GetGuardedInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return GuardedInvokers[queue];
    }

    void ValidateActiveLeader()
    {
        if (!HydraManager->IsActiveLeader()) {
            throw TNotALeaderException()
                <<= ERROR_SOURCE_LOCATION()
                >>= TError(NRpc::EErrorCode::Unavailable, "Not an active leader");
        }
    }

private:
    TCellMasterConfigPtr Config;
    TBootstrap* Bootstrap;

    TFairShareActionQueuePtr AutomatonQueue;
    TMasterAutomatonPtr Automaton;
    IHydraManagerPtr HydraManager;
    std::vector<IInvokerPtr> GuardedInvokers;
    std::vector<IInvokerPtr> EpochInvokers;

    TPeriodicExecutorPtr CleanupExecutor;


    void OnStartEpoch()
    {
        YCHECK(EpochInvokers.empty());

        auto cancelableContext = HydraManager
            ->GetAutomatonEpochContext()
            ->CancelableContext;
        for (int index = 0; index < EAutomatonThreadQueue::GetDomainSize(); ++index) {
            EpochInvokers.push_back(cancelableContext->CreateInvoker(AutomatonQueue->GetInvoker(index)));
        }
    }

    void OnStopEpoch()
    {
        EpochInvokers.clear();
    }


    void OnCleanup()
    {
        auto snapshotsPath = Config->Snapshots->Path;

        std::vector<int> snapshotIds;
        auto snapshotFileNames = NFS::EnumerateFiles(snapshotsPath);
        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;
            try {
                int snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                snapshotIds.push_back(snapshotId);
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %s in snapshot store",
                    ~fileName.Quote());
            }
        }

        if (snapshotIds.size() <= Config->HydraManager->MaxSnapshotsToKeep)
            return;

        std::sort(snapshotIds.begin(), snapshotIds.end());
        int thresholdId = snapshotIds[snapshotIds.size() - Config->HydraManager->MaxSnapshotsToKeep];

        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            try {
                int snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                if (snapshotId < thresholdId) {
                    LOG_INFO("Removing snapshot %d",
                        snapshotId);

                    auto dataFile = NFS::CombinePaths(snapshotsPath, fileName);
                    NFS::Remove(dataFile);
                }
            } catch (const std::exception& ex) {
                // Ignore, cf. logging above.
            }
        }

        auto changelogsPath = Config->Changelogs->Path;
        auto changelogFileNames = NFS::EnumerateFiles(changelogsPath);
        for (const auto& fileName : changelogFileNames) {
            if (NFS::GetFileExtension(fileName) != ChangelogExtension)
                continue;

            try {
                int changelogId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                if (changelogId < thresholdId) {
                    LOG_INFO("Removing changelog %d",
                        changelogId);
                    RemoveChangelogFiles(NFS::CombinePaths(changelogsPath, fileName));
                }
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %s in changelog store",
                    ~fileName.Quote());
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TMetaStateFacade::TMetaStateFacade(
    TCellMasterConfigPtr config,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TMetaStateFacade::~TMetaStateFacade()
{ }

TMasterAutomatonPtr TMetaStateFacade::GetAutomaton() const
{
    return Impl->GetAutomaton();
}

IHydraManagerPtr TMetaStateFacade::GetManager() const
{
    return Impl->GetManager();
}

IInvokerPtr TMetaStateFacade::GetInvoker(EAutomatonThreadQueue queue) const
{
    return Impl->GetInvoker(queue);
}

IInvokerPtr TMetaStateFacade::GetEpochInvoker(EAutomatonThreadQueue queue) const
{
    return Impl->GetEpochInvoker(queue);
}

IInvokerPtr TMetaStateFacade::GetGuardedInvoker(EAutomatonThreadQueue queue) const
{
    return Impl->GetGuardedInvoker(queue);
}

void TMetaStateFacade::Start()
{
    Impl->Start();
}

void TMetaStateFacade::ValidateActiveLeader()
{
    return Impl->ValidateActiveLeader();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

