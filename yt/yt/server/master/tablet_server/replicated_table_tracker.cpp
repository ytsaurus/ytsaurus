#include "private.h"
#include "replicated_table_tracker.h"

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_thread.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>

#include <yt/yt/server/master/table_server/replicated_table_node.h>

#include <yt/yt/server/master/tablet_server/config.h>
#include <yt/yt/server/master/tablet_server/table_replica.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/server/master/hive/cluster_directory_synchronizer.h>
#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/tablet_client/table_replica_ypath.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTableServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NApi;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TClusterStateKey
{
    NApi::IClientPtr Client;
    TString ClusterName; // for diagnostics only

    bool operator==(const TClusterStateKey& other) const
    {
        return Client == other.Client;
    }

    operator size_t() const
    {
        size_t result = 0;
        HashCombine(result, Client);
        return result;
    }
};

void FormatValue(TStringBuilderBase* builder, const TClusterStateKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", key.ClusterName);
}

TString ToString(const TClusterStateKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

struct TBundleHealthKey
{
    NApi::IClientPtr Client;
    TString ClusterName; // for diagnostics only
    TString BundleName;

    bool operator==(const TBundleHealthKey& other) const
    {
        return
            Client == other.Client &&
            BundleName == other.BundleName;
    }

    operator size_t() const
    {
        size_t result = 0;
        HashCombine(result, Client);
        HashCombine(result, BundleName);
        return result;
    }
};

void FormatValue(TStringBuilderBase* builder, const TBundleHealthKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v@%v",
        key.BundleName,
        key.ClusterName);
}

TString ToString(const TBundleHealthKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBundleHealthCache)

class TBundleHealthCache
    : public TAsyncExpiringCache<TBundleHealthKey, ETabletCellHealth>
{
public:
    explicit TBundleHealthCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            TabletServerLogger.WithTag("Cache: BundleHealth"))
    { }

protected:
    TFuture<ETabletCellHealth> DoGet(
        const TBundleHealthKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return key.Client->GetNode("//sys/tablet_cell_bundles/" + ToYPathLiteral(key.BundleName) + "/@health").ToUncancelable()
            .Apply(BIND([] (const TErrorOr<TYsonString>& error) {
                return ConvertTo<ETabletCellHealth>(error.ValueOrThrow());
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TBundleHealthCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterStateCache)

class TClusterStateCache
    : public TAsyncExpiringCache<TClusterStateKey, void>
{
public:
    explicit TClusterStateCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            TabletServerLogger.WithTag("Cache: ClusterLivenessCheck"))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterStateKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return AllSucceeded(std::vector<TFuture<void>>{
            CheckClusterLiveness(key),
            CheckClusterSafeMode(key),
            CheckHydraIsReadOnly(key)});
    }

private:
    TFuture<void> CheckClusterLiveness(const TClusterStateKey& key) const
    {
        TCheckClusterLivenessOptions options;
        options.CheckCypressRoot = true;
        return key.Client->CheckClusterLiveness(options)
            .Apply(BIND([clusterName = key.ClusterName] (const TError& result) {
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error checking cluster %Qlv liveness",
                    clusterName);
            }));
    }

    TFuture<void> CheckClusterSafeMode(const TClusterStateKey& key) const
    {
        return key.Client->GetNode("//sys/@config/enable_safe_mode")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& error) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting enable_safe_mode attribute for cluster %Qlv",
                    clusterName);
                if (auto isSafeModeEnabled = ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Safe mode is enabled for cluster %Qlv", clusterName);
                }
            }));
    }

    TFuture<void> CheckHydraIsReadOnly(const TClusterStateKey& key) const
    {
        return key.Client->GetNode("//sys/@hydra_read_only")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& error) {
                if (error.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return;
                }
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting hydra_read_only attribute for cluster %Qlv",
                    clusterName);
                if (auto isHydraReadOnly = ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Hydra read only mode is activated for cluster %Qlv", clusterName);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterStateCache)

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTracker::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(TReplicatedTableTrackerConfigPtr config, TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ReplicatedTableTracker)
        , Config_(std::move(config))
        , ConnectionThread_(New<TActionQueue>("RTTConnection"))
        , ClusterDirectory_(New<TClusterDirectory>(NApi::TConnectionOptions{ConnectionThread_->GetInvoker()}))
        , BundleHealthCache_(New<TBundleHealthCache>(BundleHealthCacheConfig_))
        , ClusterStateCache_(New<TClusterStateCache>(ClusterStateCacheConfig_))
        , ReplicatorHintConfig_(New<NTabletNode::TReplicatorHintConfig>())
        , CheckerThreadPool_(New<TThreadPool>(Config_->CheckerThreadCount, "RplTableTracker"))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ReplicatedTableTracker), AutomatonThread);
        VERIFY_INVOKER_THREAD_AFFINITY(CheckerThreadPool_->GetInvoker(), CheckerThread);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SubscribeNodeCreated(BIND(&TImpl::OnNodeCreated, MakeStrong(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
    }

private:
    const TReplicatedTableTrackerConfigPtr Config_;
    const TAsyncExpiringCacheConfigPtr BundleHealthCacheConfig_ = New<TAsyncExpiringCacheConfig>();
    const TAsyncExpiringCacheConfigPtr ClusterStateCacheConfig_ = New<TAsyncExpiringCacheConfig>();

    const TActionQueuePtr ConnectionThread_;
    const TClusterDirectoryPtr ClusterDirectory_;

    std::atomic<bool> Enabled_ = false;

    std::atomic<TDuration> GeneralCheckTimeout_ = TDuration::Minutes(1);

    TAtomicObject<TBundleHealthCachePtr> BundleHealthCache_;
    TAtomicObject<TClusterStateCachePtr> ClusterStateCache_;
    TAtomicObject<NTabletNode::TReplicatorHintConfigPtr> ReplicatorHintConfig_;
    std::atomic<i64> MaxIterationsWithoutAcceptableBundleHealth_{1};

    class TReplica
        : public TRefCounted
    {
    public:
        TReplica(
            TObjectId id,
            ETableReplicaMode mode,
            const TString& clusterName,
            const TYPath& path,
            TBundleHealthCachePtr bundleHealthCache,
            TClusterStateCachePtr clusterStateCache,
            NTabletNode::TReplicatorHintConfigPtr replicatorHintConfig,
            IClientPtr client,
            IInvokerPtr checkerInvoker,
            TDuration lag,
            TDuration tabletCellBundleNameTtl,
            TDuration retryOnFailureInterval,
            TDuration syncReplicaLagThreshold,
            bool checkPreloadState,
            i64 maxIterationsWithoutAcceptableBundleHealth,
            TDuration generalCheckTimeout)
            : Id_(id)
            , Mode_(mode)
            , ClusterName_(clusterName)
            , Path_(path)
            , BundleHealthCache_(std::move(bundleHealthCache))
            , ClusterStateCache_(std::move(clusterStateCache))
            , ReplicatorHintConfig_(std::move(replicatorHintConfig))
            , Client_(std::move(client))
            , CheckerInvoker_(std::move(checkerInvoker))
            , Lag_(lag)
            , TabletCellBundleNameTtl_(tabletCellBundleNameTtl)
            , RetryOnFailureInterval_(retryOnFailureInterval)
            , SyncReplicaLagThreshold_(syncReplicaLagThreshold)
            , CheckPreloadState_(checkPreloadState)
            , GeneralCheckTimeout_(generalCheckTimeout)
            , MaxIterationsWithoutAcceptableBundleHealth_(maxIterationsWithoutAcceptableBundleHealth)
        { }

        TObjectId GetId()
        {
            return Id_;
        }

        const TString& GetClusterName()
        {
            return ClusterName_;
        }

        const TYPath& GetPath()
        {
            return Path_;
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
            if (!Client_) {
                static const auto NoClientResult = MakeFuture<void>(TError("No connection is available"));
                return NoClientResult;
            }

            return CheckClusterState()
                .Apply(BIND([=, weakThis_ = MakeWeak(this)] {
                    if (auto this_ = weakThis_.Lock()) {
                        return CheckReplicaState();
                    } else {
                        return VoidFuture;
                    }
                }));
        }

        TFuture<void> SetMode(TBootstrap* const bootstrap, ETableReplicaMode mode)
        {
            YT_LOG_DEBUG("Switching table replica mode (Path: %v, ReplicaId: %v, ClusterName: %v, Mode: %v)",
                Path_,
                Id_,
                ClusterName_,
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
                        YT_LOG_DEBUG("Table replica mode switched (Path: %v, ReplicaId: %v, ClusterName: %v, Mode: %v)",
                            Path_,
                            Id_,
                            ClusterName_,
                            mode);
                    } else {
                        YT_LOG_DEBUG(rspOrError, "Error switching table replica mode (Path: %v, ReplicaId: %v, ClusterName: %v, Mode: %v)",
                            Path_,
                            Id_,
                            ClusterName_,
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

        void Merge(const TIntrusivePtr<TReplica>& oldReplica)
        {
            AsyncTabletCellBundleName_.Store(oldReplica->AsyncTabletCellBundleName_.Load());
            LastUpdateTime_.store(oldReplica->LastUpdateTime_.load());
            IterationsWithoutAcceptableBundleHealth_.store(oldReplica->IterationsWithoutAcceptableBundleHealth_.load());
        }

    private:
        const TObjectId Id_;
        ETableReplicaMode Mode_;
        const TString ClusterName_;
        const TYPath Path_;

        const TBundleHealthCachePtr BundleHealthCache_;
        const TClusterStateCachePtr ClusterStateCache_;

        const NTabletNode::TReplicatorHintConfigPtr ReplicatorHintConfig_;
        const NApi::IClientPtr Client_;
        const IInvokerPtr CheckerInvoker_;
        const TDuration Lag_;

        const TDuration TabletCellBundleNameTtl_;
        const TDuration RetryOnFailureInterval_;
        const TDuration SyncReplicaLagThreshold_;
        const bool CheckPreloadState_;
        const TDuration GeneralCheckTimeout_;
        const i64 MaxIterationsWithoutAcceptableBundleHealth_;

        std::atomic<i64> IterationsWithoutAcceptableBundleHealth_ = 0;
        std::atomic<TInstant> LastUpdateTime_ = {};
        TAtomicObject<TFuture<TString>> AsyncTabletCellBundleName_ = MakeFuture<TString>(TError("<unknown>"));


        TFuture<void> CheckClusterState()
        {
            return ClusterStateCache_->Get({Client_, ClusterName_});
        }

        TFuture<void> CheckReplicaState()
        {
            if (ReplicatorHintConfig_->BannedReplicaClusters.contains(GetClusterName())) {
                return MakeFuture<void>(TError("Replica cluster is banned"));
            }

            auto replicaLagError = CheckReplicaLag();
            if (!replicaLagError.IsOK()) {
                return MakeFuture<void>(std::move(replicaLagError));
            }

            return
                AllSucceeded(std::vector<TFuture<void>>{
                    CheckTableAttributes(),
                    CheckBundleHealth()})
                .WithTimeout(GeneralCheckTimeout_);
        }

        TFuture<void> CheckBundleHealth()
        {
            return GetAsyncTabletCellBundleName()
                .Apply(BIND([client = Client_, clusterName = ClusterName_, bundleHealthCache = BundleHealthCache_] (const TErrorOr<TString>& bundleNameOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(bundleNameOrError, "Error getting table bundle name");

                    const auto& bundleName = bundleNameOrError.Value();
                    return bundleHealthCache->Get({client, clusterName, bundleName});
                })).Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<ETabletCellHealth>& healthOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(healthOrError, "Error getting tablet cell bundle health");

                    auto iterationCount = IterationsWithoutAcceptableBundleHealth_.load();
                    auto health = healthOrError.Value();
                    if (health != ETabletCellHealth::Good && health != ETabletCellHealth::Degraded) {
                        auto newIterationCount = iterationCount + 1;
                        IterationsWithoutAcceptableBundleHealth_.store(newIterationCount);
                        if (newIterationCount <= MaxIterationsWithoutAcceptableBundleHealth_) {
                            return;
                        }

                        THROW_ERROR_EXCEPTION("Bad tablet cell health %Qlv for %v times in a row",
                            health,
                            newIterationCount);
                    } else {
                        if (iterationCount != 0) {
                            IterationsWithoutAcceptableBundleHealth_.store(0);
                        }
                    }
                }));
        }

        TFuture<void> CheckTableAttributes()
        {
            auto checkPreloadState = CheckPreloadState_;

            TGetNodeOptions options;
            if (checkPreloadState) {
                options.Attributes = {"preload_state"};
            }

            return Client_->GetNode(Path_, options)
                .Apply(BIND([checkPreloadState] (const TErrorOr<TYsonString>& resultOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error checking table attributes");

                    if (checkPreloadState) {
                        auto resultNode = ConvertToNode(resultOrError.Value());
                        auto preloadState = resultNode->Attributes().Get<NTabletNode::EStorePreloadState>("preload_state");
                        if (preloadState != NTabletNode::EStorePreloadState::Complete) {
                            THROW_ERROR_EXCEPTION("Table preload is not completed; actual state is %Qlv",
                                preloadState);
                        }
                    }
                }));
        }

        TError CheckReplicaLag()
        {
            auto lag = GetLag();
            return lag < SyncReplicaLagThreshold_
                ? TError()
                : TError("Replica lag time is over the threshold ")
                    << TErrorAttribute("replica_lag_time", lag)
                    << TErrorAttribute("replica_lag_threshold", SyncReplicaLagThreshold_);
        }

        TFuture<TString> GetAsyncTabletCellBundleName()
        {
            auto now = NProfiling::GetInstant();
            auto asyncTabletCellBundleName = AsyncTabletCellBundleName_.Load();

            auto interval = (asyncTabletCellBundleName.IsSet() && !asyncTabletCellBundleName.Get().IsOK())
                ? RetryOnFailureInterval_
                : TabletCellBundleNameTtl_;

            if (LastUpdateTime_ + interval < now) {
                LastUpdateTime_ = now;
                asyncTabletCellBundleName = Client_->GetNode(Path_ + "/@tablet_cell_bundle")
                    .Apply(BIND([] (const TErrorOr<TYsonString>& bundleNameOrError) {
                        THROW_ERROR_EXCEPTION_IF_FAILED(bundleNameOrError, "Error getting table bundle name");
                        return ConvertTo<TString>(bundleNameOrError.Value());
                    }));
                AsyncTabletCellBundleName_.Store(asyncTabletCellBundleName);
            }

            return asyncTabletCellBundleName;
        }
    };

    using TReplicaPtr = TIntrusivePtr<TReplica>;

    class TTable
        : public TRefCounted
    {
    public:
        TTable(TObjectId id, NProfiling::TCounter replicaSwitchCounter, TReplicatedTableOptionsPtr config = nullptr)
            : Id_(id)
            , ReplicaSwitchCounter_(replicaSwitchCounter)
            , Config_(std::move(config))
        { }

        TObjectId GetId() const
        {
            return Id_;
        }

        NProfiling::TCounter GetReplicaSwitchCounter() const
        {
            return ReplicaSwitchCounter_;
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
                if (Replicas_[i] && *replicas[i] == *Replicas_[i]) {
                    replicas[i]->Merge(Replicas_[i]);
                }
                Replicas_[i] = replicas[i];
            }
        }

        TFuture<int> Check(TBootstrap* bootstrap)
        {
            if (!CheckFuture_ || CheckFuture_.IsSet()) {
                std::vector<TReplicaPtr> syncReplicas;
                std::vector<TReplicaPtr> asyncReplicas;
                int maxSyncReplicaCount;
                int minSyncReplicaCount;

                {
                    auto guard = Guard(Lock_);
                    std::tie(minSyncReplicaCount, maxSyncReplicaCount) = Config_->GetEffectiveMinMaxReplicaCount(static_cast<int>(Replicas_.size()));
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

                CheckFuture_ = AllSet(futures)
                    .Apply(BIND([
                        bootstrap,
                        syncReplicas,
                        asyncReplicas,
                        maxSyncReplicaCount,
                        minSyncReplicaCount,
                        id = Id_
                    ] (const std::vector<TError>& results) mutable {
                        std::vector<TReplicaPtr> badSyncReplicas;
                        std::vector<TReplicaPtr> goodSyncReplicas;
                        std::vector<TReplicaPtr> goodAsyncReplicas;
                        goodSyncReplicas.reserve(syncReplicas.size());
                        badSyncReplicas.reserve(syncReplicas.size());
                        goodAsyncReplicas.reserve(asyncReplicas.size());

                        auto logLivenessCheckResult = [&] (const TError& error, const TReplicaPtr& replica) {
                            YT_LOG_DEBUG_IF(!error.IsOK(), error, "Replica liveness check failed (ReplicatedTableId: %v, ReplicaId: %v, "
                                "ReplicaTablePath: %v, ReplicaClusterName: %v)",
                                id,
                                replica->GetId(),
                                replica->GetPath(),
                                replica->GetClusterName());
                        };

                        {
                            int index = 0;
                            for (; index < static_cast<int>(syncReplicas.size()); ++index) {
                                const auto& result = results[index];
                                const auto& replica = syncReplicas[index];
                                logLivenessCheckResult(result, replica);
                                if (result.IsOK()) {
                                    goodSyncReplicas.push_back(replica);
                                } else {
                                    badSyncReplicas.push_back(replica);
                                }
                            }
                            for (; index < static_cast<int>(results.size()); ++index) {
                                const auto& result = results[index];
                                const auto& replica = asyncReplicas[index - syncReplicas.size()];
                                logLivenessCheckResult(result, replica);
                                if (result.IsOK()) {
                                    goodAsyncReplicas.push_back(replica);
                                }
                            }
                        }

                        std::sort(
                            goodAsyncReplicas.begin(),
                            goodAsyncReplicas.end(),
                            [] (const auto& lhs, const auto& rhs) {
                                return lhs->GetLag() > rhs->GetLag();
                            });

                        std::vector<TFuture<void>> futures;
                        futures.reserve(syncReplicas.size() + asyncReplicas.size());

                        int switchCount = 0;
                        int currentSyncReplicaCount = std::min(maxSyncReplicaCount, static_cast<int>(goodSyncReplicas.size()));
                        while (currentSyncReplicaCount < maxSyncReplicaCount && !goodAsyncReplicas.empty()) {
                            futures.push_back(goodAsyncReplicas.back()->SetMode(bootstrap, ETableReplicaMode::Sync));
                            ++switchCount;
                            goodAsyncReplicas.pop_back();
                            ++currentSyncReplicaCount;
                        }

                        for (int index = Max(0, minSyncReplicaCount - currentSyncReplicaCount); index < static_cast<int>(badSyncReplicas.size()); ++index) {
                            futures.push_back(badSyncReplicas[index]->SetMode(bootstrap, ETableReplicaMode::Async));
                            ++switchCount;
                        }
                        for (int index = maxSyncReplicaCount; index < static_cast<int>(goodSyncReplicas.size()); ++index) {
                            futures.push_back(goodSyncReplicas[index]->SetMode(bootstrap, ETableReplicaMode::Async));
                            ++switchCount;
                        }

                        return AllSucceeded(futures)
                            .Apply(BIND([switchCount] {
                                return switchCount;
                            }));
                    }));
            }

            return CheckFuture_;
        }

    private:
        const TObjectId Id_;
        const NProfiling::TCounter ReplicaSwitchCounter_;
        NTableServer::TReplicatedTableOptionsPtr Config_;

        YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
        std::vector<TReplicaPtr> Replicas_;

        TFuture<int> CheckFuture_;
    };

    using TTablePtr = TIntrusivePtr<TTable>;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
    THashMap<TObjectId, TTablePtr> Tables_;

    struct TClusterConnectionInfo
    {
        IConnectionPtr Connection;
        NApi::IClientPtr Client;
    };

    TAdaptiveLock ClusterToConnectionLock_;
    THashMap<TString, TClusterConnectionInfo> ClusterToConnection_;

    TPeriodicExecutorPtr UpdaterExecutor_;

    TThreadPoolPtr CheckerThreadPool_;
    TPeriodicExecutorPtr CheckerExecutor_;

    const NHiveServer::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizerConfig_ =
        New<NHiveServer::TClusterDirectorySynchronizerConfig>();
    NHiveServer::IClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(CheckerThread);


    void CheckEnabled()
    {
        Enabled_ = false;

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        if (!hydraFacade->GetHydraManager()->IsActiveLeader()) {
            return;
        }

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->IsInitialized()) {
            return;
        }

        const auto& dynamicConfig = GetDynamicConfig();
        if (!dynamicConfig->EnableReplicatedTableTracker) {
            YT_LOG_INFO("Replicated table tracker is disabled, see //sys/@config");
            return;
        }

        Enabled_ = true;
    }

    IClientPtr CreateClient(TStringBuf clusterName, IConnectionPtr connection, const TGuard<TAdaptiveLock>& /*guard*/)
    {
        YT_VERIFY(connection);

        auto client = connection->CreateClient(NApi::TClientOptions::FromUser(RootUserName));
        ClusterToConnection_[clusterName] = {
            .Connection = std::move(connection),
            .Client = client};

        YT_LOG_DEBUG("Created new client for cluster %v in replicated table tracker",
            clusterName);

        return client;
    }

    void UpdateClusterClients()
    {
        auto guard = Guard(ClusterToConnectionLock_);

        for (auto it = ClusterToConnection_.begin(); it != ClusterToConnection_.end(); ) {
            auto jt = it++;
            const auto& [clusterName, connectionInfo] = *jt;
            auto connection = ClusterDirectory_->FindConnection(clusterName);
            if (!connection) {
                YT_LOG_WARNING("Removed unknown cluster %v from replicated table tracker",
                    clusterName);
                ClusterToConnection_.erase(jt);
            } else if (connection != connectionInfo.Connection) {
                CreateClient(clusterName, std::move(connection), guard);
            }
        }
    }

    IClientPtr GetOrCreateClusterClient(const TString& clusterName)
    {
        auto guard = Guard(ClusterToConnectionLock_);

        // TODO(akozhikhov): Try hash table with hazard ptrs.
        auto connectionInfo = ClusterToConnection_.find(clusterName);
        if (connectionInfo != ClusterToConnection_.end()) {
            return connectionInfo->second.Client;
        } else if (auto connection = ClusterDirectory_->FindConnection(clusterName)) {
            return CreateClient(clusterName, std::move(connection), guard);
        }

        return nullptr;
    }

    void UpdateTables()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        UpdateClusterClients();

        THashMap<TObjectId, TTablePtr> capturedTables;

        {
            auto guard = Guard(Lock_);
            capturedTables = Tables_;
        }

        for (const auto& [id, table] : capturedTables) {
            auto* object = Bootstrap_->GetObjectManager()->FindObject(id);
            if (!IsObjectAlive(object)) {
                YT_LOG_DEBUG("Table no longer exists (TableId: %v)",
                    id);
                {
                    auto guard = Guard(Lock_);
                    Tables_.erase(id);
                }
                continue;
            }

            OnNodeCreated(object);
        }
    }

    void CheckTables()
    {
        VERIFY_THREAD_AFFINITY(CheckerThread);

        std::vector<TFuture<int>> futures;

        {
            auto guard = Guard(Lock_);
            futures.reserve(Tables_.size());

            for (const auto& [id, table] : Tables_) {
                if (!table->IsEnabled()) {
                    YT_LOG_DEBUG("Replicated Table Tracker is disabled (TableId: %v)",
                        id);
                    continue;
                }

                auto switchCounter = table->GetReplicaSwitchCounter();
                auto future = table->Check(Bootstrap_);
                future.Subscribe(BIND([id = id, switchCounter] (const TErrorOr<int>& result) {
                    YT_LOG_DEBUG_UNLESS(result.IsOK(), result, "Error checking table (TableId: %v)",
                        id);

                    if (result.IsOK()) {
                        switchCounter.Increment(result.Value());
                    }
                }));
                futures.push_back(future);
            }
        }

        WaitFor(AllSet(futures))
            .ValueOrThrow();
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


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        UpdaterExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ReplicatedTableTracker),
            BIND(&TImpl::UpdateIteration, MakeWeak(this)));
        UpdaterExecutor_->Start();

        CheckerExecutor_ = New<TPeriodicExecutor>(
            CheckerThreadPool_->GetInvoker(),
            BIND(&TImpl::CheckIteration, MakeWeak(this)));
        CheckerExecutor_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (CheckerExecutor_) {
            CheckerExecutor_->Stop();
            CheckerExecutor_.Reset();
        }

        if (UpdaterExecutor_) {
            UpdaterExecutor_->Stop();
            UpdaterExecutor_.Reset();
        }

        if (ClusterDirectorySynchronizer_) {
            ClusterDirectorySynchronizer_->Stop();
            ClusterDirectorySynchronizer_.Reset();
        }

        Enabled_ = false;
    }

    void OnAfterSnapshotLoaded() noexcept override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnAfterSnapshotLoaded();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (auto [id, node] : cypressManager->Nodes()) {
            if (IsObjectAlive(node) && node->IsTrunk() && node->GetType() == EObjectType::ReplicatedTable) {
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

        auto id = object->GetId();
        const auto& config = object->GetReplicatedTableOptions();

        TTablePtr table;

        bool newTable = false;

        {
            auto guard = Guard(Lock_);
            auto it = Tables_.find(id);
            if (it == Tables_.end()) {
                table = New<TTable>(id, object->GetTabletCellBundle()->ProfilingCounters().ReplicaSwitch, config);
                Tables_.emplace(id, table);
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
            if (replica->GetState() != ETableReplicaState::Enabled ||
                !replica->GetEnableReplicatedTableTracker())
            {
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

            auto client = GetOrCreateClusterClient(replica->GetClusterName());
            if (!client) {
                YT_LOG_WARNING("Unknown replica cluster (ClusterName: %v, ReplicaId: %v, TableId: %v)",
                    replica->GetClusterName(),
                    replica->GetId(),
                    table->GetId());
            }

            replicas.push_back(New<TReplica>(
                replica->GetId(),
                replica->GetMode(),
                replica->GetClusterName(),
                replica->GetReplicaPath(),
                BundleHealthCache_.Load(),
                ClusterStateCache_.Load(),
                ReplicatorHintConfig_.Load(),
                std::move(client),
                CheckerThreadPool_->GetInvoker(),
                replica->ComputeReplicationLagTime(lastestTimestamp),
                config->TabletCellBundleNameTtl,
                config->RetryOnFailureInterval,
                config->SyncReplicaLagThreshold,
                config->EnablePreloadStateCheck,
                MaxIterationsWithoutAcceptableBundleHealth_.load(),
                GeneralCheckTimeout_.load()));
        }

        const auto [maxSyncReplicaCount,  minSyncReplicaCount] = config->GetEffectiveMinMaxReplicaCount(static_cast<int>(replicas.size()));

        YT_LOG_DEBUG("Table %v (TableId: %v, Replicas: %v, SyncReplicas: %v, AsyncReplicas: %v, SkippedReplicas: %v, DesiredMaxSyncReplicaCount: %v, DesiredMinSyncReplicaCount: %v)",
            newTable ? "added" : "updated",
            object->GetId(),
            object->Replicas().size(),
            syncReplicas,
            asyncReplicas,
            skippedReplicas,
            maxSyncReplicaCount,
            minSyncReplicaCount);

        table->SetConfig(config);
        table->SetReplicas(replicas);
    }

    void OnNodeCreated(TObject* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (object->IsTrunk() && object->GetType() == EObjectType::ReplicatedTable) {
            auto* replicatedTable = object->As<NTableServer::TReplicatedTableNode>();
            ProcessReplicatedTable(replicatedTable);
        }
    }


    const TDynamicReplicatedTableTrackerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicatedTableTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& dynamicConfig = GetDynamicConfig();

        if (UpdaterExecutor_) {
            UpdaterExecutor_->SetPeriod(dynamicConfig->UpdatePeriod);
        }

        if (CheckerExecutor_) {
            CheckerExecutor_->SetPeriod(dynamicConfig->CheckPeriod);
        }

        GeneralCheckTimeout_.store(dynamicConfig->GeneralCheckTimeout);

        if (ReconfigureYsonSerializable(BundleHealthCacheConfig_, dynamicConfig->BundleHealthCache)) {
            BundleHealthCache_.Store(New<TBundleHealthCache>(BundleHealthCacheConfig_));
        }

        if (ReconfigureYsonSerializable(ClusterStateCacheConfig_, dynamicConfig->ClusterStateCache)) {
            ClusterStateCache_.Store(New<TClusterStateCache>(ClusterStateCacheConfig_));
        }

        ReplicatorHintConfig_.Store(dynamicConfig->ReplicatorHint);

        MaxIterationsWithoutAcceptableBundleHealth_ = dynamicConfig->MaxIterationsWithoutAcceptableBundleHealth;

        if (IsLeader() && (ReconfigureYsonSerializable(ClusterDirectorySynchronizerConfig_, dynamicConfig->ClusterDirectorySynchronizer) || !ClusterDirectorySynchronizer_)) {
            if (ClusterDirectorySynchronizer_) {
                ClusterDirectorySynchronizer_->Stop();
            }
            ClusterDirectorySynchronizer_ = CreateClusterDirectorySynchronizer(
                dynamicConfig->ClusterDirectorySynchronizer,
                Bootstrap_,
                ClusterDirectory_);
            ClusterDirectorySynchronizer_->Start();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableTracker::TReplicatedTableTracker(
    TReplicatedTableTrackerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TReplicatedTableTracker::~TReplicatedTableTracker() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
