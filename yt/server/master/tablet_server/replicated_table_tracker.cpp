#include "private.h"
#include "replicated_table_tracker.h"

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/scheduler_thread.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/rpc/helpers.h>

#include <yt/core/misc/async_expiring_cache.h>

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/world_initializer.h>

#include <yt/server/master/table_server/replicated_table_node.h>

#include <yt/server/master/tablet_server/config.h>
#include <yt/server/master/tablet_server/table_replica.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/lib/hydra/composite_automaton.h>

#include <yt/server/master/hive/cluster_directory_synchronizer.h>
#include <yt/server/lib/hive/config.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/tablet_client/table_replica_ypath.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/client/api/public.h>
#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTableServer;
using namespace NTabletServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NTabletClient;
using namespace NYTree;
using namespace NApi;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTracker::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(TReplicatedTableTrackerConfigPtr config, TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ReplicatedTableTracker)
        , Config_(std::move(config))
        , CheckerThreadPool_(New<TThreadPool>(Config_->ThreadCount, "ReplTableCheck"))
        , ClusterDirectory_(New<TClusterDirectory>())
        , ClusterDirectorySynchronizer_(New<NHiveServer::TClusterDirectorySynchronizer>(
            New<NHiveServer::TClusterDirectorySynchronizerConfig>(),
            Bootstrap_,
            ClusterDirectory_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ReplicatedTableTracker), AutomatonThread);
        VERIFY_INVOKER_THREAD_AFFINITY(CheckerThreadPool_->GetInvoker(), CheckerThread);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SubscribeNodeCreated(BIND(&TImpl::OnNodeCreated, MakeStrong(this)));
    }

private:
    NTabletServer::TReplicatedTableTrackerConfigPtr Config_;
    bool Enabled_ = false;

    static TAsyncExpiringCacheConfigPtr GetAsyncExpiringCacheConfig() {
        auto config = New<TAsyncExpiringCacheConfig>();
        config->ExpireAfterAccessTime = TDuration::Seconds(1);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(1);
        config->ExpireAfterFailedUpdateTime = TDuration::Seconds(1);
        return config;
    }

    class TBundleHealthCache
        : public TAsyncExpiringCache<std::pair<NApi::IClientPtr, TString>, ETabletCellHealth>
    {
    public:
        TBundleHealthCache()
            : TAsyncExpiringCache(GetAsyncExpiringCacheConfig())
        { }

    protected:
        virtual TFuture<ETabletCellHealth> DoGet(const std::pair<NApi::IClientPtr, TString>& key) override
        {
            const auto& [client, bundleName] = key;
            return client->GetNode("//sys/tablet_cell_bundles/" + bundleName + "/@health").ToUncancelable()
                .Apply(BIND([] (const TErrorOr<NYson::TYsonString>& error) {
                    // COMPAT(aozeritsky): Remove after updating all clusters
                    if (!error.IsOK() && error.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        return ETabletCellHealth::Good;
                    }
                    return ConvertTo<ETabletCellHealth>(error.ValueOrThrow());
                }));
        }
    };

    using TBundleHealthCachePtr = TIntrusivePtr<TBundleHealthCache>;
    const TBundleHealthCachePtr BundleHealthCache_ = New<TBundleHealthCache>();

    class TReplica
        : public TRefCounted
    {
    public:
        TReplica(
            TObjectId id,
            ETableReplicaMode mode,
            const TString& clusterName,
            const TYPath& path,
            const TBundleHealthCachePtr& bundleHealthCache,
            IConnectionPtr connection,
            IInvokerPtr checkerInvoker,
            TDuration lag)
            : Id_(id)
            , Mode_(mode)
            , ClusterName_(clusterName)
            , Path_(path)
            , BundleHealthCache_(bundleHealthCache)
            , Connection_(std::move(connection))
            , CheckerInvoker_(std::move(checkerInvoker))
            , Lag_(lag)
        {
            CreateClient();
        }

        TDuration GetLag() const
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

            auto check2 = Client_->NodeExists(Path_).Apply(BIND([path = Path_] (const TErrorOr<bool>& error) {
                auto flag = error.ValueOrThrow();
                if (!flag) {
                    THROW_ERROR_EXCEPTION("Node %v does not exist",
                        path);
                }
            }));

            auto check3 = CheckBundleHealth();

            return Combine(std::vector<TFuture<void>>{
                check1,
                check2,
                check3
            });
        }

        TFuture<void> CheckBundleHealth()
        {
            if (!Client_) {
                static const auto NoConnectionResult = MakeFuture<void>(TError("No connection is available"));
                return NoConnectionResult;
            }
            
            return GetAsyncTabletCellBundleName()
                .Apply(BIND([client = Client_, bundleHealthCache = BundleHealthCache_] (const TString& bundleName) {
                    return bundleHealthCache->Get({client, bundleName});
                })).Apply(BIND([path = Path_] (ETabletCellHealth health) {
                    if (health != ETabletCellHealth::Good) {
                        THROW_ERROR_EXCEPTION(
                            "Bad tablet cell health %Qlv for %v",
                            health,
                            path);
                    }
                }));
        }

        TFuture<void> SetMode(TBootstrap* const bootstrap, ETableReplicaMode mode)
        {
            YT_LOG_DEBUG("Switching table replica mode (Path: %v, ReplicaId: %v, Mode: %v)",
                Path_,
                Id_,
                mode);

            auto automatonInvoker = bootstrap
                ->GetHydraFacade()
                ->GetAutomatonInvoker(EAutomatonThreadQueue::TabletManager);

            return BIND([=, this_ = MakeStrong(this)] () {
                auto req = TTableReplicaYPathProxy::Alter(FromObjectId(Id_));
                GenerateMutationId(req);
                req->set_mode(static_cast<int>(mode));

                const auto& objectManager = bootstrap->GetObjectManager();
                auto rootService = objectManager->GetRootService();
                return ExecuteVerb(rootService, req);
            })
                .AsyncVia(automatonInvoker)
                .Run()
                .Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TTableReplicaYPathProxy::TRspAlterPtr>& rspOrError) {
                    if (rspOrError.IsOK()) {
                        Mode_ = mode;
                        YT_LOG_DEBUG("Table replica mode switched (Path: %v, ReplicaId: %v, Mode: %v)",
                            Path_,
                            Id_,
                            mode);
                    } else {
                        YT_LOG_DEBUG(rspOrError, "Error switching table replica mode (Path: %v, ReplicaId: %v, Mode: %v)",
                            Path_,
                            Id_,
                            mode);
                    }
                })
                .Via(CheckerInvoker_));
        }

        bool operator == (const TReplica& other) const
        {
            return Id_ == other.Id_
                && ClusterName_ == other.ClusterName_
                && Path_ == other.Path_;
        }

        bool operator != (const TReplica& other) const
        {
            return !(*this == other);
        }

        void Merge(const TReplica& other)
        {
            Mode_ = other.Mode_;
            if (Connection_ != other.Connection_) {
                Connection_ = other.Connection_;
                CreateClient();
            }
            Lag_ = other.Lag_;
        }

    private:
        const TObjectId Id_;
        ETableReplicaMode Mode_;
        const TString ClusterName_;
        const TYPath Path_;

        TBundleHealthCachePtr BundleHealthCache_;
        NApi::IConnectionPtr Connection_;
        NApi::IClientPtr Client_;
        const IInvokerPtr CheckerInvoker_;
        TDuration Lag_;
        TFuture<TString> AsyncTabletCellBundleName_ = MakeFuture<TString>(TError("<unknown>"));

        const TDuration TabletCellBundleNameTtl = TDuration::Seconds(300);
        const TDuration RetryOnFailureInterval = TDuration::Seconds(60);

        TInstant LastUpdateTime_;

        TFuture<TString> GetAsyncTabletCellBundleName()
        {
            auto now = NProfiling::GetInstant();
            auto interval = (AsyncTabletCellBundleName_.IsSet() && !AsyncTabletCellBundleName_.Get().IsOK())
                ? RetryOnFailureInterval
                : TabletCellBundleNameTtl;

            if (LastUpdateTime_ + interval < now) {
                LastUpdateTime_ = now;
                AsyncTabletCellBundleName_ = Client_->GetNode(Path_ + "/@tablet_cell_bundle")
                    .Apply(BIND([] (const TErrorOr<NYson::TYsonString>& bundleNameOrError) {
                        return ConvertTo<TString>(bundleNameOrError.ValueOrThrow());
                    }));
            }

            return AsyncTabletCellBundleName_;
        }

        void CreateClient()
        {
            Client_ = Connection_
                ? Connection_->CreateClient(NApi::TClientOptions(RootUserName))
                : IClientPtr();
        }
    };

    using TReplicaPtr = TIntrusivePtr<TReplica>;

    class TTable
        : public TRefCounted
    {
    public:
        explicit TTable(TObjectId id, TReplicatedTableOptionsPtr config = nullptr)
            : Id_(id)
            , Config_(std::move(config))
        { }

        TObjectId GetId() const
        {
            return Id_;
        }

        bool IsEnabled() const
        {
            auto guard = Guard(Lock_);
            return Config_ && Config_->EnableReplicatedTableTracker;
        }

        void SetConfig(const NTableServer::TReplicatedTableOptionsPtr& config)
        {
            auto guard = Guard(Lock_);
            Config_ = config;
        }

        void SetReplicas(const std::vector<TReplicaPtr>& replicas)
        {
            auto guard = Guard(Lock_);
            Replicas_.resize(replicas.size());
            for (int i = 0; i < static_cast<int>(replicas.size()); ++i) {
                if (!Replicas_[i] || *Replicas_[i] != *replicas[i]) {
                    Replicas_[i] = replicas[i];
                } else {
                    Replicas_[i]->Merge(*replicas[i]);
                }
            }
        }

        TFuture<void> Check(TBootstrap* bootstrap)
        {
            if (!CheckFuture_ || CheckFuture_.IsSet()) {
                std::vector<TReplicaPtr> syncReplicas;
                std::vector<TReplicaPtr> asyncReplicas;
                int maxSyncReplicas;
                int minSyncReplicas;

                {
                    auto guard = Guard(Lock_);
                    maxSyncReplicas = Config_->MaxSyncReplicaCount.value_or(static_cast<int>(Replicas_.size()));
                    minSyncReplicas = Config_->MinSyncReplicaCount.value_or(maxSyncReplicas);
                    asyncReplicas.reserve(Replicas_.size());
                    syncReplicas.reserve(Replicas_.size());
                    for (auto& replica : Replicas_) {
                        if (replica->IsSync()) {
                            syncReplicas.push_back(replica);
                        } else {
                            asyncReplicas.push_back(replica);
                        }
                    }
                }

                std::vector<TFuture<void>> futures;
                futures.reserve(syncReplicas.size() + asyncReplicas.size());

                for (const auto& syncReplica : syncReplicas) {
                    futures.push_back(syncReplica->Check());
                }
                for (const auto& asyncReplica : asyncReplicas) {
                    futures.push_back(asyncReplica->Check());
                }

                CheckFuture_ = CombineAll(futures)
                    .Apply(BIND([bootstrap, syncReplicas, asyncReplicas, maxSyncReplicas, minSyncReplicas] (const std::vector<TErrorOr<void>>& results) mutable {
                        std::vector<TReplicaPtr> badSyncReplicas;
                        std::vector<TReplicaPtr> goodSyncReplicas;
                        std::vector<TReplicaPtr> goodAsyncReplicas;
                        goodSyncReplicas.reserve(syncReplicas.size());
                        badSyncReplicas.reserve(syncReplicas.size());
                        goodAsyncReplicas.reserve(asyncReplicas.size());

                        int index = 0;
                        for (; index < syncReplicas.size(); ++index) {
                            if (results[index].IsOK()) {
                                goodSyncReplicas.push_back(syncReplicas[index]);
                            } else {
                                badSyncReplicas.push_back(syncReplicas[index]);
                            }
                        }

                        for (; index < results.size(); ++index) {
                            if (results[index].IsOK()) {
                                goodAsyncReplicas.push_back(asyncReplicas[index-syncReplicas.size()]);
                            }
                        }

                        std::vector<TFuture<void>> futures;
                        futures.reserve(syncReplicas.size() + asyncReplicas.size());

                        std::sort(
                            goodAsyncReplicas.begin(),
                            goodAsyncReplicas.end(),
                            [&] (const auto& lhs, const auto& rhs) {
                                return lhs->GetLag() > rhs->GetLag();
                            });

                        for (index = goodSyncReplicas.size(); index < maxSyncReplicas && !goodAsyncReplicas.empty(); ++index) {
                            futures.push_back(goodAsyncReplicas.back()->SetMode(bootstrap, ETableReplicaMode::Sync));
                            goodAsyncReplicas.pop_back();
                        }

                        int totalSyncReplicas = maxSyncReplicas < goodSyncReplicas.size()
                            ? maxSyncReplicas
                            : index;

                        YT_VERIFY(totalSyncReplicas <= maxSyncReplicas);

                        for (index = Max(0, minSyncReplicas - totalSyncReplicas); index < badSyncReplicas.size(); ++index) {
                            futures.push_back(badSyncReplicas[index]->SetMode(bootstrap, ETableReplicaMode::Async));
                        }

                        for (index = maxSyncReplicas; index < goodSyncReplicas.size(); ++index) {
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

        const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicatedTableTracker;

        if (!dynamicConfig->EnableReplicatedTableTracker) {
            Enabled_ = false;
            YT_LOG_INFO("Replicated table manager is disabled, see //sys/@config");
            return;
        }

        Enabled_ = true;
    }

    void UpdateTables()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        THashMap<TObjectId, TTablePtr> capturedTables;

        {
            auto lock = Guard(Lock_);
            capturedTables = Tables_;
        }

        for (const auto& pair : capturedTables) {
            auto& id = pair.first;
            auto object = Bootstrap_->GetObjectManager()->FindObject(id);
            if (!IsObjectAlive(object)) {
                auto lock = Guard(Lock_);
                YT_LOG_DEBUG("Table no longer exists (TableId: %v)",
                    id);
                Tables_.erase(id);
                continue;
            }

            OnNodeCreated(object);
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
                auto tableId = item.first;
                if (!item.second->IsEnabled()) {
                    YT_LOG_DEBUG("Replicated Table Tracker is disabled (TableId: %v)",
                        tableId);
                    continue;
                }
                auto future = item.second->Check(Bootstrap_);
                future.Subscribe(BIND([tableId] (const TErrorOr<void>& errorOr) {
                    YT_LOG_DEBUG_UNLESS(errorOr.IsOK(), errorOr, "Error on checking table (TableId: %v)",
                        tableId);
                }));
                futures.push_back(future);
            }
        }

        WaitFor(CombineAll(futures))
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
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Cannot update tables");
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
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Cannot check tables");
        }
    }

    /* automaton parts */
    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (Config_->EnableReplicatedTableTracker) {
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

        ClusterDirectorySynchronizer_->Stop();

        if (CheckerExecutor_) {
            CheckerExecutor_->Stop();
            CheckerExecutor_.Reset();
        }

        if (UpdaterExecutor_) {
            UpdaterExecutor_->Stop();
            UpdaterExecutor_.Reset();
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

        if (object->IsExternal()) {
            return;
        }

        const auto& id = object->GetId();
        const auto& config = object->GetReplicatedTableOptions();

        TTablePtr table;

        bool newTable = false;

        {
            auto lock = Guard(Lock_);
            auto it = Tables_.find(id);
            if (it == Tables_.end()) {
                table = New<TTable>(id, config);
                Tables_.insert(std::make_pair(id, table));
                newTable = true;
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
            if (!replica->GetEnableReplicatedTableTracker()) {
                skippedReplicas += 1;
                continue;
            }

            switch (replica->GetMode()) {
                case ETableReplicaMode::Sync:
                    syncReplicas += 1;
                    break;
                case ETableReplicaMode::Async:
                    asyncReplicas += 1;
                    break;
                default:
                    YT_ABORT();
            }

            auto connection = ClusterDirectory_->FindConnection(replica->GetClusterName());
            if (!connection) {
                YT_LOG_WARNING("Unknown replica cluster (Name: %v, ReplicaId: %v, TableId: %v)",
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
                    BundleHealthCache_,
                    connection,
                    CheckerThreadPool_->GetInvoker(),
                    replica->ComputeReplicationLagTime(lastestTimestamp)));
        }

        YT_LOG_DEBUG("Table %v (TableId: %v, Replicas: %v, SyncReplicas: %v, AsyncReplicas: %v, SkippedReplicas: %v, DesiredMaxSyncReplicas: %v, DesiredMinSyncReplicas: %v)",
            newTable ? "added" : "updated",
            object->GetId(),
            object->Replicas().size(),
            syncReplicas,
            asyncReplicas,
            skippedReplicas,
            config->MaxSyncReplicaCount,
            config->MinSyncReplicaCount);

        table->SetConfig(config);
        table->SetReplicas(replicas);
    }

    void OnNodeCreated(TObject* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (object->IsTrunk() && object->GetType() == NCypressClient::EObjectType::ReplicatedTable) {
            auto* replicatedTable = object->As<NTableServer::TReplicatedTableNode>();
            ProcessReplicatedTable(replicatedTable);
        }
    }
};

TReplicatedTableTracker::TReplicatedTableTracker(
    TReplicatedTableTrackerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
