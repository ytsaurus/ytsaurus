#include "private.h"
#include "replicated_table_manager.h"

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/scheduler_thread.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/rpc/helpers.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/world_initializer.h>

#include <yt/server/table_server/replicated_table_node.h>

#include <yt/server/tablet_server/config.h>
#include <yt/server/tablet_server/table_replica.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/server/hydra/composite_automaton.h>

#include <yt/server/hive/cluster_directory_synchronizer.h>
#include <yt/server/hive/config.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/public.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/public.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/tablet_client/table_replica_ypath.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/client/api/public.h>
#include <yt/client/transaction_client/timestamp_provider.h>

namespace NYT {
namespace NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTabletServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(const NTabletServer::TReplicatedTableManagerConfigPtr& config, TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Default)
        , Config_(config)
        , CheckerThreadPool_(New<TThreadPool>(Config_->ThreadCount, "TReplicatedTableManager"))
        , ClusterDirectory_(New<TClusterDirectory>())
        , ClusterDirectorySynchronizer_(New<NHiveServer::TClusterDirectorySynchronizer>(
            New<NHiveServer::TClusterDirectorySynchronizerConfig>(),
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            Bootstrap_->GetObjectManager(),
            ClusterDirectory_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic), AutomatonThread);
        VERIFY_INVOKER_THREAD_AFFINITY(CheckerThreadPool_->GetInvoker(), CheckerThread);
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SubscribeNodeCreated(BIND(&TImpl::OnNewNode, MakeStrong(this)));
    }

private:
    NTabletServer::TReplicatedTableManagerConfigPtr Config_;
    bool Enabled_ = false;

    class TReplica
        : public TRefCounted
    {
    public:
        TReplica(
            const TObjectId& id,
            ETableReplicaMode mode,
            const TString& clusterName,
            const TYPath& path,
            const NApi::IConnectionPtr& connection,
            const TDuration& lag)
            : Id_(id)
            , Mode_(mode)
            , ClusterName_(clusterName)
            , Path_(path)
            , Connection_(connection)
            , Client_(Connection_ ? Connection_->CreateClient(NApi::TClientOptions(RootUserName)) : NApi::IClientPtr())
            , Lag_(lag)
        { }

        const TDuration& GetLag() const
        {
            return Lag_;
        }

        bool IsSync() const
        {
            return Mode_ == ETableReplicaMode::Sync;
        }

        TFuture<void> Check()
        {
            if (!Connection_) {
                return VoidFuture;
            }

            auto check1 = Client_->ListNode("/").As<void>();
            auto check2 = Client_->NodeExists(Path_).Apply(BIND([] (const TErrorOr<bool>& error) {
                auto flag = error.ValueOrThrow();
                if (!flag) {
                    THROW_ERROR_EXCEPTION("Node is not found");
                }
            }));

            std::vector<TFuture<void>> checks = {check1, check2};
            return Combine(checks);
        }

        TFuture<void> SetMode(TBootstrap* const bootstrap, ETableReplicaMode mode)
        {
            auto automatonInvoker = bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::TabletManager);

            LOG_DEBUG("Switch replica mode (Id: %v, Mode: %v)",
                Id_,
                mode);

            return BIND([this, this_ = MakeStrong(this), mode, bootstrap] () {
                auto req = NTabletClient::TTableReplicaYPathProxy::Alter(NObjectClient::FromObjectId(Id_));

                NCypressClient::SetTransactionId(req, NObjectClient::NullTransactionId);
                NRpc::SetMutationId(req, NRpc::GenerateMutationId(), false);

                req->set_mode(static_cast<int>(mode));

                auto message = req->Serialize();
                auto context = NYTree::CreateYPathContext(message);
                const auto& objectManager = bootstrap->GetObjectManager();
                auto mutation = objectManager->CreateExecuteMutation(RootUserName, context);
                mutation->CommitAndLog(Logger);
            })
                .AsyncVia(automatonInvoker)
                .Run()
                .As<void>()
                .Apply(BIND([this, this_ = MakeStrong(this), mode] () {
                    Mode_ = mode;
                    LOG_DEBUG("Mode switched (Id: %v, Mode: %Qv)",
                        Id_,
                        mode);
                }));
        }

    private:
        const TObjectId Id_;
        ETableReplicaMode Mode_;
        const TString ClusterName_;
        const TYPath Path_;
        const NApi::IConnectionPtr Connection_;
        NApi::IClientPtr Client_;

        TDuration Lag_;
    };

    using TReplicaPtr = TIntrusivePtr<TReplica>;

    class TTable
        : public TRefCounted
    {
    public:
        TTable(TObjectId id, const NTableServer::TReplicatedTableOptionsPtr& config = NTableServer::TReplicatedTableOptionsPtr())
            : Id_(id)
            , Config_(config)
        { }

        const TObjectId& GetId() const
        {
            return Id_;
        }

        bool IsEnabled() const
        {
            auto guard = Guard(Lock_);
            return Config_ && Config_->EnableReplicatedTableManager;
        }

        void SetConfig(const NTableServer::TReplicatedTableOptionsPtr& config)
        {
            auto guard = Guard(Lock_);
            Config_ = config;
        }

        void SetReplicas(std::vector<TReplicaPtr>& replicas)
        {
            auto guard = Guard(Lock_);
            Replicas_.swap(replicas);
        }

        TFuture<void> Check(TBootstrap* const bootstrap)
        {
            if (!CheckFuture_ || CheckFuture_.IsSet()) {
                std::vector<TReplicaPtr> syncReplicas;
                std::vector<TReplicaPtr> asyncReplicas;
                int syncReplicasNeeded = 0;

                {
                    auto guard = Guard(Lock_);
                    syncReplicasNeeded = Config_->SyncReplicas;

                    asyncReplicas.reserve(Replicas_.size());

                    for (auto& replica : Replicas_) {
                        if (replica->IsSync()) {
                            syncReplicas.push_back(replica);
                        } else {
                            asyncReplicas.push_back(replica);
                        }
                    }
                }

                std::vector<TFuture<void>> futures;
                futures.reserve(syncReplicas.size());

                for (const auto& syncReplica : syncReplicas) {
                    futures.push_back(syncReplica->Check());
                }

                CheckFuture_ = CombineAll(futures)
                    .Apply(BIND([bootstrap, syncReplicas, asyncReplicas, syncReplicasNeeded] (const std::vector<TErrorOr<void>>& answers) mutable {
                        std::vector<TReplicaPtr> badSyncReplicas;
                        std::vector<TReplicaPtr> goodSyncReplicas;
                        goodSyncReplicas.reserve(syncReplicas.size());
                        badSyncReplicas.reserve(syncReplicas.size());
                        for (int index = 0; index < answers.size(); ++index) {
                            if (answers[index].IsOK()) {
                                goodSyncReplicas.push_back(syncReplicas[index]);
                            } else {
                                badSyncReplicas.push_back(syncReplicas[index]);
                            }
                        }

                        std::vector<TFuture<void>> futures;
                        futures.reserve(syncReplicas.size() + asyncReplicas.size());

                        std::sort(asyncReplicas.begin(), asyncReplicas.end(),
                            BIND([](const TReplicaPtr& a, const TReplicaPtr& b) -> bool {
                                return a->GetLag() > b->GetLag();
                            })
                        );
                        // Don't check async replicas
                        // If any is bad we will switch to anther one on the next iteration
                        for (int index = goodSyncReplicas.size(); index < syncReplicasNeeded && !asyncReplicas.empty(); ++index) {
                            futures.push_back(asyncReplicas.back()->SetMode(bootstrap, ETableReplicaMode::Sync));
                            asyncReplicas.pop_back();
                        }

                        for (auto& replica : badSyncReplicas) {
                            futures.push_back(replica->SetMode(bootstrap, ETableReplicaMode::Async));
                        }

                        for (int index = syncReplicasNeeded; index < goodSyncReplicas.size(); ++index) {
                            futures.push_back(goodSyncReplicas[index]->SetMode(bootstrap, ETableReplicaMode::Async));
                        }

                        return Combine(futures);
                    }));
            }

            return CheckFuture_;
        }

    private:
        TObjectId Id_;
        NTableServer::TReplicatedTableOptionsPtr Config_;

        TSpinLock Lock_;
        std::vector<TReplicaPtr> Replicas_;

        TFuture<void> CheckFuture_;
    };

    using TTablePtr = TIntrusivePtr<TTable>;

    TSpinLock Lock_;
    THashMap<TObjectId, TTablePtr> Tables_;

    TPeriodicExecutorPtr UpdaterExecutor_;

    TThreadPoolPtr CheckerThreadPool_;
    TPeriodicExecutorPtr CheckerExecutor_;

    const TClusterDirectoryPtr ClusterDirectory_;
    const NHiveServer::TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(CheckerThread);

    void CheckEnabled()
    {
        if (!Bootstrap_->IsPrimaryMaster()) {
            Enabled_ = false;
            return;
        }

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        if (!hydraFacade->GetHydraManager()->IsActiveLeader()) {
            Enabled_ = false;
            return;
        }

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->IsInitialized()) {
            Enabled_ = false;
            return;
        }

        const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicatedTableManager;

        if (!dynamicConfig->EnableReplicatedTableManager) {
            Enabled_ = false;
            LOG_INFO("Replicated table manager is disabled, see //sys/@config");
            return;
        }

        Enabled_ = true;
    }

    void UpdateTables()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        THashMap<TObjectId, TTablePtr> copy;

        {
            auto lock = Guard(Lock_);
            copy = Tables_;
        }

        for (auto it : copy) {
            auto& id = it.first;
            auto object = Bootstrap_->GetObjectManager()->FindObject(id);

            if (!object) {
                auto lock = Guard(Lock_);
                LOG_DEBUG("Object not found (Id: %v)",
                    id);
                Tables_.erase(id);
                return;
            } else {
                OnNewNode(object);
            }
        }
    }

    void CheckTables()
    {
        VERIFY_THREAD_AFFINITY(CheckerThread);

        std::vector<TFuture<void>> futures;

        {
            auto lock = Guard(Lock_);
            futures.reserve(Tables_.size());
            for (const auto& item : Tables_) {
                if (!item.second->IsEnabled()) {
                    LOG_DEBUG("Replicated Table Manager is disabled (Id: %v)",
                        item.first);
                    continue;
                }
                futures.push_back(item.second->Check(Bootstrap_));
            }
        }

        WaitFor(Combine(futures))
            .ThrowOnError();
    }

    void UpdateIteration()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        CheckEnabled();
        if (!Enabled_) {
            return;
        }

        try {
            UpdateTables();
        } catch (const std::exception& e) {
            LOG_WARNING(e, "Cannot update tables");
        }
    }

    void CheckIteration()
    {
        VERIFY_THREAD_AFFINITY(CheckerThread);

        if (!Enabled_) {
            return;
        }

        try {
            CheckTables();
        } catch (const std::exception& e) {
            LOG_WARNING(e, "Cannot check tables");
        }
    }

    /* automaton parts */
    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (Config_->EnableReplicatedTableManager && Bootstrap_->IsPrimaryMaster()) {
            ClusterDirectorySynchronizer_->Start();

            UpdaterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TImpl::UpdateIteration, MakeWeak(this)),
                Config_->UpdatePeriod);
            UpdaterExecutor_->Start();

            CheckerExecutor_ = New<TPeriodicExecutor>(
                CheckerThreadPool_->GetInvoker(), BIND(&TImpl::CheckIteration, MakeWeak(this)),
                Config_->CheckPeriod);
            CheckerExecutor_->Start();
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (CheckerExecutor_) {
            ClusterDirectorySynchronizer_->Stop();

            UpdaterExecutor_->Stop();
            UpdaterExecutor_.Reset();

            CheckerExecutor_->Stop();
            CheckerExecutor_.Reset();
        }
    }

    virtual void OnAfterSnapshotLoaded() noexcept override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnAfterSnapshotLoaded();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (const auto& pair : cypressManager->Nodes()) {
            auto* node = pair.second;
            if (node->IsTrunk() && node->GetType() == NCypressClient::EObjectType::ReplicatedTable) {
                auto* object = node->As<NTableServer::TReplicatedTableNode>();
                ProcessReplicatedTable(object);
            }
        }
    }

    void ProcessReplicatedTable(NTableServer::TReplicatedTableNode* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& id = object->GetId();
        const auto& config = object->GetReplicatedTableOptions();

        TTablePtr table;

        {
            auto lock = Guard(Lock_);
            auto it = Tables_.find(id);
            if (it == Tables_.end()) {
                table = New<TTable>(id, config);
                Tables_.insert(std::make_pair(id, table));
            } else {
                table = it->second;
            }
        }

        std::vector<TReplicaPtr> replicas;
        replicas.reserve(object->Replicas().size());

        auto lastestTimestamp = Bootstrap_->GetTimestampProvider()->GetLatestTimestamp();

        int skippedReplicas = 0;
        int syncReplicas = 0;
        int asyncReplicas = 0;

        for (const auto& replica : object->Replicas()) {
            if (!replica->GetEnableReplicatedTableManager()) {
                skippedReplicas += 1;
                continue;
            }

            switch (replica->GetMode())
            {
                case ETableReplicaMode::Sync:
                    syncReplicas += 1;
                    break;
                case ETableReplicaMode::Async:
                    asyncReplicas += 1;
                    break;
                default:
                    Y_UNREACHABLE();
            }

            auto connection = ClusterDirectory_->FindConnection(replica->GetClusterName());

            if (!connection) {
                LOG_WARNING("Unknown cluster name (Name: %Qv, Replica: %v,  Table %v)",
                    replica->GetClusterName(),
                    replica->GetId(),
                    table->GetId());
            }

            replicas.emplace_back(
                New<TReplica>(
                    replica->GetId(),
                    replica->GetMode(),
                    replica->GetClusterName(),
                    replica->GetReplicaPath(),
                    connection,
                    replica->ComputeReplicationLagTime(lastestTimestamp)));
        }

        LOG_DEBUG("Add table (Id: %v, Replicas: %v, SyncReplicas: %v, AsyncReplicas: %v, SkippedReplicas: %v, SyncReplicasWanted: %v)",
            object->GetId(),
            object->Replicas().size(),
            syncReplicas,
            asyncReplicas,
            skippedReplicas,
            config->SyncReplicas);

        table->SetConfig(config);
        table->SetReplicas(replicas);
    }

    void OnNewNode(TObjectBase* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (object->IsTrunk() && object->GetType() == NCypressClient::EObjectType::ReplicatedTable) {
            auto replicatedTable = object->As<NTableServer::TReplicatedTableNode>();
            ProcessReplicatedTable(replicatedTable);
        }
    }
};

TReplicatedTableManager::TReplicatedTableManager(const NTabletServer::TReplicatedTableManagerConfigPtr& config, TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
