#include "private.h"
#include "replicated_table_tracker.h"
#include "tablet_manager.h"

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

#include <yt/yt/server/master/table_server/public.h>
#include <yt/yt/server/master/table_server/replicated_table_node.h>
#include <yt/yt/server/master/table_server/table_collocation.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/tablet_server/config.h>
#include <yt/yt/server/master/tablet_server/table_replica.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/hive/cluster_directory_synchronizer.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/server/lib/tablet_node/config.h>

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

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/traceless_guard.h>

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
using namespace NReplicatedTableTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TClusterStateCacheKey
{
    NApi::IClientPtr Client;
    TString ClusterName; // for diagnostics only

    bool operator==(const TClusterStateCacheKey& other) const
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

void FormatValue(TStringBuilderBase* builder, const TClusterStateCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", key.ClusterName);
}

TString ToString(const TClusterStateCacheKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

struct TBundleHealthCacheKey
{
    NApi::IClientPtr Client;
    TString ClusterName; // for diagnostics only
    TString BundleName;

    bool operator==(const TBundleHealthCacheKey& other) const
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

void FormatValue(TStringBuilderBase* builder, const TBundleHealthCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v@%v",
        key.BundleName,
        key.ClusterName);
}

TString ToString(const TBundleHealthCacheKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBundleHealthCache)

class TBundleHealthCache
    : public TAsyncExpiringCache<TBundleHealthCacheKey, ETabletCellHealth>
{
public:
    explicit TBundleHealthCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            TabletServerLogger.WithTag("Cache: BundleHealth"))
    { }

protected:
    TFuture<ETabletCellHealth> DoGet(
        const TBundleHealthCacheKey& key,
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
    : public TAsyncExpiringCache<TClusterStateCacheKey, void>
{
public:
    explicit TClusterStateCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            TabletServerLogger.WithTag("Cache: ClusterLivenessCheck"))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterStateCacheKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return AllSucceeded(std::vector<TFuture<void>>{
            CheckClusterLiveness(key),
            CheckClusterSafeMode(key),
            CheckHydraIsReadOnly(key),
            CheckClusterIncomingReplication(key),
        });
    }

private:
    TFuture<void> CheckClusterLiveness(const TClusterStateCacheKey& key) const
    {
        TCheckClusterLivenessOptions options{
            .CheckCypressRoot = true,
            .CheckSecondaryMasterCells = true,
        };
        return key.Client->CheckClusterLiveness(options)
            .Apply(BIND([clusterName = key.ClusterName] (const TError& result) {
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error checking cluster %Qv liveness",
                    clusterName);
            }));
    }

    TFuture<void> CheckClusterSafeMode(const TClusterStateCacheKey& key) const
    {
        return key.Client->GetNode("//sys/@config/enable_safe_mode")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& error) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting enable_safe_mode attribute for cluster %Qv",
                    clusterName);
                if (ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Safe mode is enabled for cluster %Qv",
                        clusterName);
                }
            }));
    }

    TFuture<void> CheckHydraIsReadOnly(const TClusterStateCacheKey& key) const
    {
        return key.Client->GetNode("//sys/@hydra_read_only")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& error) {
                // COMPAT(akozhikhov).
                if (error.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return;
                }
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting hydra_read_only attribute for cluster %Qv",
                    clusterName);
                if (ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Hydra read only mode is activated for cluster %Qv",
                        clusterName);
                }
            }));
    }

    TFuture<void> CheckClusterIncomingReplication(const TClusterStateCacheKey& key) const
    {
        return key.Client->GetNode("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& resultOrError) {
                // COMPAT(akozhikhov).
                if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return;
                }
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    resultOrError,
                    "Failed to check whether incoming replication to cluster %Qv is enabled",
                    clusterName);
                if (!ConvertTo<bool>(resultOrError.Value())) {
                    THROW_ERROR_EXCEPTION("Replica cluster %Qv incoming replication is disabled",
                        clusterName);
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
        , ClusterDirectory_(New<TClusterDirectory>(NApi::NNative::TConnectionOptions(ConnectionThread_->GetInvoker())))
        , BundleHealthCache_(New<TBundleHealthCache>(BundleHealthCacheConfig_))
        , ClusterStateCache_(New<TClusterStateCache>(ClusterStateCacheConfig_))
        , ReplicatorHintConfig_(New<NTabletNode::TReplicatorHintConfig>())
        , CheckerThreadPool_(CreateThreadPool(Config_->CheckerThreadCount, "RplTableTracker"))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ReplicatedTableTracker), AutomatonThread);
        VERIFY_INVOKER_THREAD_AFFINITY(CheckerThreadPool_->GetInvoker(), CheckerThread);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SubscribeNodeCreated(BIND_NO_PROPAGATE(&TImpl::OnNodeCreated, MakeStrong(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
    }

private:
    const TReplicatedTableTrackerConfigPtr Config_;
    const TAsyncExpiringCacheConfigPtr BundleHealthCacheConfig_ = New<TAsyncExpiringCacheConfig>();
    const TAsyncExpiringCacheConfigPtr ClusterStateCacheConfig_ = New<TAsyncExpiringCacheConfig>();

    const TActionQueuePtr ConnectionThread_;
    const TClusterDirectoryPtr ClusterDirectory_;

    std::atomic<bool> Enabled_ = false;

    std::atomic<TDuration> GeneralCheckTimeout_ = TDuration::Minutes(1);

    TAtomicIntrusivePtr<TBundleHealthCache> BundleHealthCache_;
    TAtomicIntrusivePtr<TClusterStateCache> ClusterStateCache_;
    TAtomicIntrusivePtr<NTabletNode::TReplicatorHintConfig> ReplicatorHintConfig_;
    std::atomic<i64> MaxIterationsWithoutAcceptableBundleHealth_ = 1;

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
                .Apply(BIND([=, this, weakThis_ = MakeWeak(this)] {
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

            return BIND([=, this, this_ = MakeStrong(this)] () {
                auto req = TTableReplicaYPathProxy::Alter(FromObjectId(Id_));
                GenerateMutationId(req);
                req->set_mode(static_cast<int>(mode));

                const auto& objectManager = bootstrap->GetObjectManager();
                auto rootService = objectManager->GetRootService();
                return ExecuteVerb(rootService, req);
            })
                .AsyncVia(automatonInvoker)
                .Run()
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TTableReplicaYPathProxy::TRspAlterPtr>& rspOrError) {
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
                })).Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<ETabletCellHealth>& healthOrError) {
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
        struct TCheckResult
        {
            int SwitchCount;
            std::optional<THashSet<TString>> SyncReplicaClusters;
        };

        TTable(
            TObjectId id,
            NProfiling::TCounter replicaSwitchCounter,
            TReplicatedTableOptionsPtr config,
            TTableCollocationId collocationId)
            : Id_(id)
            , ReplicaSwitchCounter_(replicaSwitchCounter)
            , Config_(std::move(config))
            , CollocationId_(collocationId)
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

        void SetConfig(const TReplicatedTableOptionsPtr& config)
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

        void SetCollocationId(TTableCollocationId collocationId)
        {
            auto guard = Guard(Lock_);
            CollocationId_ = collocationId;
        }

        TTableCollocationId GetCollocationId()
        {
            auto guard = Guard(Lock_);
            return CollocationId_;
        }

        TFuture<TCheckResult> Check(
            TBootstrap* bootstrap,
            std::optional<THashSet<TString>> referenceReplicaClusters)
        {
            if (!CheckFuture_ || CheckFuture_.IsSet()) {
                std::vector<TReplicaPtr> syncReplicas;
                std::vector<TReplicaPtr> asyncReplicas;
                int maxSyncReplicaCount;
                int minSyncReplicaCount;

                std::optional<std::vector<TString>> preferredSyncReplicaClusters;
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
                    if (!referenceReplicaClusters) {
                        preferredSyncReplicaClusters = Config_->PreferredSyncReplicaClusters;
                    }
                }

                if (preferredSyncReplicaClusters) {
                    referenceReplicaClusters.emplace();
                    for (const auto& clusterName : *preferredSyncReplicaClusters) {
                        referenceReplicaClusters->insert(clusterName);
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
                        referenceReplicaClusters = std::move(referenceReplicaClusters),
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

                        int switchCount = 0;
                        int currentSyncReplicaCount = std::min(maxSyncReplicaCount, static_cast<int>(goodSyncReplicas.size()));
                        while (currentSyncReplicaCount < maxSyncReplicaCount && !goodAsyncReplicas.empty()) {
                            futures.push_back(goodAsyncReplicas.back()->SetMode(bootstrap, ETableReplicaMode::Sync));
                            ++switchCount;
                            goodSyncReplicas.push_back(std::move(goodAsyncReplicas.back()));
                            goodAsyncReplicas.pop_back();
                            ++currentSyncReplicaCount;
                        }

                        for (int index = Max(0, minSyncReplicaCount - currentSyncReplicaCount);
                            index < static_cast<int>(badSyncReplicas.size());
                            ++index)
                        {
                            futures.push_back(badSyncReplicas[index]->SetMode(bootstrap, ETableReplicaMode::Async));
                            ++switchCount;
                        }

                        for (int index = maxSyncReplicaCount; index < static_cast<int>(goodSyncReplicas.size()); ++index) {
                            futures.push_back(goodSyncReplicas[index]->SetMode(bootstrap, ETableReplicaMode::Async));
                            ++switchCount;
                            goodAsyncReplicas.push_back(goodSyncReplicas[index]);
                            --currentSyncReplicaCount;
                        }
                        goodSyncReplicas.resize(currentSyncReplicaCount);

                        // NB: We use hash map here so the bizarre case of multiple replicas
                        // on a single replica cluster would be processed in a more reliable way.
                        THashMap<TString, int> actualSyncReplicaClusterMap;
                        for (const auto& replica : goodSyncReplicas) {
                            ++actualSyncReplicaClusterMap[replica->GetClusterName()];
                        }

                        if (referenceReplicaClusters) {
                            std::vector<TReplicaPtr> undesiredSyncReplicas;
                            for (const auto& replica : goodSyncReplicas) {
                                if (!referenceReplicaClusters->contains(replica->GetClusterName())) {
                                    undesiredSyncReplicas.push_back(replica);
                                }
                            }

                            std::vector<TReplicaPtr> desiredAsyncReplicas;
                            for (const auto& replica : goodAsyncReplicas) {
                                if (referenceReplicaClusters->contains(replica->GetClusterName())) {
                                    desiredAsyncReplicas.push_back(replica);
                                }
                            }

                            // NB: These swaps preserve min/max sync replica count bounds.
                            while (!undesiredSyncReplicas.empty() && !desiredAsyncReplicas.empty()) {
                                --actualSyncReplicaClusterMap[undesiredSyncReplicas.back()->GetClusterName()];
                                ++actualSyncReplicaClusterMap[desiredAsyncReplicas.back()->GetClusterName()];

                                futures.push_back(undesiredSyncReplicas.back()->SetMode(bootstrap, ETableReplicaMode::Async));
                                undesiredSyncReplicas.pop_back();
                                futures.push_back(desiredAsyncReplicas.back()->SetMode(bootstrap, ETableReplicaMode::Sync));
                                desiredAsyncReplicas.pop_back();

                                switchCount += 2;
                            }
                        }

                        THashSet<TString> actualSyncReplicaClusters;
                        for (const auto& [replicaCluster, replicaCount] : actualSyncReplicaClusterMap) {
                            if (replicaCount > 0) {
                                actualSyncReplicaClusters.insert(replicaCluster);
                            }
                        }

                        return AllSucceeded(std::move(futures))
                            .Apply(BIND([switchCount, syncReplicaClusters = std::move(actualSyncReplicaClusters)] {
                                return TCheckResult{
                                    .SwitchCount = switchCount,
                                    .SyncReplicaClusters = std::move(syncReplicaClusters)
                                };
                            }));
                    }));
            }

            return CheckFuture_;
        }

    private:
        const TObjectId Id_;
        const NProfiling::TCounter ReplicaSwitchCounter_;

        TReplicatedTableOptionsPtr Config_;
        TTableCollocationId CollocationId_;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
        std::vector<TReplicaPtr> Replicas_;

        TFuture<TCheckResult> CheckFuture_;
    };

    using TTablePtr = TIntrusivePtr<TTable>;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TObjectId, TTablePtr> Tables_;

    struct TClusterConnectionInfo
    {
        IConnectionPtr Connection;
        NApi::IClientPtr Client;
    };

    NThreading::TSpinLock ClusterToConnectionLock_;
    THashMap<TString, TClusterConnectionInfo> ClusterToConnection_;

    TPeriodicExecutorPtr UpdaterExecutor_;

    IThreadPoolPtr CheckerThreadPool_;
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

        if (dynamicConfig->UseNewReplicatedTableTracker) {
            YT_LOG_DEBUG("Old replicated table tracker is disabled");
            return;
        }

        Enabled_ = true;
    }

    IClientPtr CreateClient(TStringBuf clusterName, IConnectionPtr connection, const TGuard<NThreading::TSpinLock>& /*guard*/)
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
            auto connection = static_cast<NApi::IConnectionPtr>(ClusterDirectory_->FindConnection(clusterName));
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

        std::vector<TFuture<TTable::TCheckResult>> futures;
        THashMap<TTableCollocationId, TFuture<TTable::TCheckResult>> collocationIdToLeaderFuture;

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

                TFuture<TTable::TCheckResult> future;

                auto collocationId = table->GetCollocationId();
                if (collocationId != NullObjectId) {
                    auto it = collocationIdToLeaderFuture.find(collocationId);
                    if (it == collocationIdToLeaderFuture.end()) {
                        YT_LOG_DEBUG("Picked replication collocation leader (CollocationId: %v, LeaderTableId: %v)",
                            collocationId,
                            id);
                        future = table->Check(
                            Bootstrap_,
                            /*referenceReplicaClusters*/ std::nullopt);
                        collocationIdToLeaderFuture.emplace(collocationId, future);
                    } else {
                        future = it->second.Apply(BIND(
                            [table = table, bootstrap = Bootstrap_]
                            (const TErrorOr<TTable::TCheckResult>& leaderResult)
                            {
                                auto referenceReplicaClusters = leaderResult.IsOK()
                                    ? leaderResult.Value().SyncReplicaClusters
                                    : std::nullopt;

                                return table->Check(
                                    bootstrap,
                                    referenceReplicaClusters);
                            })
                            .AsyncVia(CheckerThreadPool_->GetInvoker()));
                    }
                } else {
                    future = table->Check(
                        Bootstrap_,
                        /*referenceReplicaClusters*/ std::nullopt);
                }

                future.Subscribe(BIND([id = id, switchCounter] (const TErrorOr<TTable::TCheckResult>& result) {
                    YT_LOG_DEBUG_UNLESS(result.IsOK(), result, "Error checking table (TableId: %v)",
                        id);

                    if (result.IsOK()) {
                        switchCounter.Increment(result.Value().SwitchCount);
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
        auto collocationId = object->GetReplicationCollocation()
            ? object->GetReplicationCollocation()->GetId()
            : NullObjectId;

        TTablePtr table;

        bool newTable = false;

        {
            auto guard = Guard(Lock_);
            auto it = Tables_.find(id);
            if (it == Tables_.end()) {
                table = New<TTable>(
                    id,
                    object->TabletCellBundle()->ProfilingCounters().ReplicaModeSwitch,
                    config,
                    collocationId);
                Tables_.emplace(id, table);
                newTable = true;
            } else {
                table = it->second;
            }
        }

        std::vector<TReplicaPtr> replicas;
        replicas.reserve(object->Replicas().size());

        auto latestTimestamp = Bootstrap_->GetTimestampProvider()->GetLatestTimestamp();

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
                BundleHealthCache_.Acquire(),
                ClusterStateCache_.Acquire(),
                ReplicatorHintConfig_.Acquire(),
                std::move(client),
                CheckerThreadPool_->GetInvoker(),
                replica->ComputeReplicationLagTime(latestTimestamp),
                config->TabletCellBundleNameTtl,
                config->RetryOnFailureInterval,
                config->SyncReplicaLagThreshold,
                config->EnablePreloadStateCheck,
                MaxIterationsWithoutAcceptableBundleHealth_.load(),
                GeneralCheckTimeout_.load()));
        }

        const auto [maxSyncReplicaCount,  minSyncReplicaCount] = config->GetEffectiveMinMaxReplicaCount(static_cast<int>(replicas.size()));

        YT_LOG_DEBUG("Table %v "
            "(TableId: %v, CollocationId: %v, "
            "Replicas: %v, SyncReplicas: %v, AsyncReplicas: %v, SkippedReplicas: %v, "
            "DesiredMaxSyncReplicaCount: %v, DesiredMinSyncReplicaCount: %v)",
            newTable ? "added" : "updated",
            object->GetId(),
            collocationId,
            object->Replicas().size(),
            syncReplicas,
            asyncReplicas,
            skippedReplicas,
            maxSyncReplicaCount,
            minSyncReplicaCount);

        table->SetConfig(config);
        table->SetReplicas(replicas);
        table->SetCollocationId(collocationId);
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

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
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

class TReplicatedTableTrackerStateProvider
    : public IReplicatedTableTrackerStateProvider
{
public:
    TReplicatedTableTrackerStateProvider(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        Bootstrap_->GetTabletManager()->SubscribeReplicatedTableCreated(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnReplicatedTableCreated,
            MakeWeak(this)));
        Bootstrap_->GetTabletManager()->SubscribeReplicatedTableDestroyed(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnReplicatedTableDestroyed,
            MakeWeak(this)));

        Bootstrap_->GetTabletManager()->SubscribeReplicaCreated(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnReplicaCreated,
            MakeWeak(this)));
        Bootstrap_->GetTabletManager()->SubscribeReplicaDestroyed(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnReplicaDestroyed,
            MakeWeak(this)));

        Bootstrap_->GetTableManager()->SubscribeReplicationCollocationCreated(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnReplicationCollocationCreated,
            MakeWeak(this)));
        Bootstrap_->GetTableManager()->SubscribeReplicationCollocationDestroyed(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnReplicationCollocationDestroyed,
            MakeWeak(this)));

        Bootstrap_->GetConfigManager()->SubscribeConfigChanged(BIND_NO_PROPAGATE(
            &TReplicatedTableTrackerStateProvider::OnConfigChanged,
            MakeWeak(this)));

        GetAutomatonInvoker()->Invoke(BIND(
            &TReplicatedTableTrackerStateProvider::OnConfigChanged,
            MakeWeak(this),
            /*oldConfig*/ nullptr));
    }

    void DrainUpdateQueue(
        TQueueDrainResult* result,
        TTrackerStateRevision revision,
        bool snapshotRequested) override
    {
        auto drainQueueGuard = NThreading::TracelessTryGuard(DrainQueueLock_);
        if (!drainQueueGuard.WasAcquired()) {
            THROW_ERROR_EXCEPTION(NReplicatedTableTrackerClient::EErrorCode::RttServiceDisabled,
                "Failed to acquire update queue spinlock");
        }

        if (!IsEnabled()) {
            THROW_ERROR_EXCEPTION(NReplicatedTableTrackerClient::EErrorCode::RttServiceDisabled,
                "Rtt state provider is disabled");
        }

        if (!snapshotRequested) {
            if (revision == InvalidTrackerStateRevision ||
                ClientRevision_ == InvalidTrackerStateRevision ||
                revision != ClientRevision_)
            {
                THROW_ERROR_EXCEPTION(NReplicatedTableTrackerClient::EErrorCode::StateRevisionMismatch,
                    "Rtt client state revision mismatch: %v != %v",
                    ClientRevision_,
                    revision);
            }
        }

        while (true) {
            auto guard = Guard(ActionQueueLock_);

            if (LoadingFromSnapshotRequested_.load() || snapshotRequested) {
                LoadingFromSnapshotRequested_.store(false);
                ActionQueue_.clear();
                guard.Release();

                YT_LOG_DEBUG("Rtt state provider started loading snapshot");

                auto [revision, snapshot] = WaitFor(GetSnapshotFuture())
                    .ValueOrThrow();

                ClientRevision_ = revision;
                result->set_snapshot_revision(revision);
                ToProto(result->mutable_snapshot(), snapshot);

                {
                    // NB: Some outdated actions could have been enqueued while blocked on GetSnapshotFuture.
                    auto guard = Guard(ActionQueueLock_);
                    while (!ActionQueue_.empty()) {
                        YT_VERIFY(ActionQueue_.front().revision() != revision);
                        if (ActionQueue_.front().revision() > revision) {
                            break;
                        }
                        ActionQueue_.pop_front();
                    }
                }

                YT_LOG_DEBUG("Rtt state provider finished loading snapshot (ClientRevision: %v)",
                    ClientRevision_);

                result->clear_update_actions();
                break;
            }

            if (ActionQueue_.empty()) {
                break;
            }

            auto action = std::move(ActionQueue_.front());
            ActionQueue_.pop_front();
            guard.Release();

            YT_LOG_DEBUG("Rtt state provider dequeued an action (Revision: %v)",
                action.revision());

            YT_LOG_ALERT_IF(action.revision() <= ClientRevision_,
                "Rtt state provider encountered oudated action upon queue drain");

            ClientRevision_ = action.revision();
            *result->add_update_actions() = std::move(action);
        }
    }

    TFuture<TReplicaLagTimes> ComputeReplicaLagTimes(
        std::vector<NTabletClient::TTableReplicaId> replicaIds) override
    {
        if (!IsEnabled()) {
            THROW_ERROR_EXCEPTION(NReplicatedTableTrackerClient::EErrorCode::RttServiceDisabled,
                "Rtt state provider is disabled");
        }

        return BIND([bootstrap = Bootstrap_, replicaIds = std::move(replicaIds)] {
            auto latestTimestamp = bootstrap->GetTimestampProvider()->GetLatestTimestamp();
            const auto& tabletManager = bootstrap->GetTabletManager();

            TReplicaLagTimes results;
            results.reserve(replicaIds.size());

            for (auto replicaId : replicaIds) {
                auto* replica = tabletManager->FindTableReplica(replicaId);
                if (IsObjectAlive(replica)) {
                    results.emplace_back(replicaId, replica->ComputeReplicationLagTime(latestTimestamp));
                }
            }

            return results;
        })
            .AsyncVia(GetAutomatonInvoker())
            .Run();
    }

    TFuture<TApplyChangeReplicaCommandResults> ApplyChangeReplicaModeCommands(
        std::vector<TChangeReplicaModeCommand> commands) override
    {
        if (!IsEnabled()) {
            THROW_ERROR_EXCEPTION(NReplicatedTableTrackerClient::EErrorCode::RttServiceDisabled,
                "Rtt state provider is disabled");
        }

        return BIND([bootstrap = Bootstrap_, commands = std::move(commands)] {
            const auto& tabletManager = bootstrap->GetTabletManager();

            std::vector<TFuture<void>> futures;
            futures.reserve(commands.size());

            for (const auto& command : commands) {
                auto* replica = tabletManager->FindTableReplica(command.ReplicaId);
                if (!IsObjectAlive(replica)) {
                    futures.push_back(MakeFuture(TError(NReplicatedTableTrackerClient::EErrorCode::NonResidentReplica,
                        "Replica %v does not reside on this cell",
                        command.ReplicaId)));
                    continue;
                }

                auto req = TTableReplicaYPathProxy::Alter(FromObjectId(command.ReplicaId));
                GenerateMutationId(req);
                req->set_mode(static_cast<int>(command.TargetMode));

                const auto& objectManager = bootstrap->GetObjectManager();
                auto rootService = objectManager->GetRootService();
                futures.push_back(ExecuteVerb(rootService, req).AsVoid());
            }

            return AllSet(std::move(futures));
        })
            .AsyncVia(GetAutomatonInvoker())
            .Run();
    }

    void EnableStateMonitoring() override
    {
        LoadingFromSnapshotRequested_.store(true);
        StateMonitoringEnabled_.store(true);
    }

    void DisableStateMonitoring() override
    {
        StateMonitoringEnabled_.store(false);
    }

    bool IsEnabled() const override
    {
        return StateMonitoringEnabled_.load() && TrackerEnabled_.load();
    }

private:
    TBootstrap* const Bootstrap_;

    using TUpdateAction = NReplicatedTableTrackerClient::NProto::TTrackerStateUpdateAction;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DrainQueueLock_);
    TTrackerStateRevision ClientRevision_ = InvalidTrackerStateRevision;

    std::atomic<TTrackerStateRevision> Revision_ = NullTrackerStateRevision;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ActionQueueLock_);
    std::deque<TUpdateAction> ActionQueue_;

    std::atomic<bool> StateMonitoringEnabled_ = false;
    std::atomic<bool> TrackerEnabled_ = false;

    std::atomic<bool> LoadingFromSnapshotRequested_ = false;

    std::atomic<i64> MaxActionQueueSize_ = -1;


    bool CheckIfCanEnqueue()
    {
        if (IsEnabled() && !LoadingFromSnapshotRequested_.load()) {
            return true;
        }

        // NB: Now we have to load from snapshot because some action is skipped.
        LoadingFromSnapshotRequested_.store(true);
        return false;
    }

    void OnReplicatedTableCreated(const TReplicatedTableData& tableData)
    {
        if (!CheckIfCanEnqueue()) {
            return;
        }

        TUpdateAction action;
        ToProto(action.mutable_created_replicated_table_data(), tableData);
        EnqueueAction(std::move(action), "ReplicatedTableCreated");
    }

    void OnReplicatedTableDestroyed(TTableId tableId)
    {
        if (!CheckIfCanEnqueue()) {
            return;
        }

        TUpdateAction action;
        ToProto(action.mutable_destroyed_replicated_table_id(), tableId);
        EnqueueAction(std::move(action), "ReplicatedTableDestroyed");
    }

    void OnReplicaCreated(const TReplicaData& replicaData)
    {
        if (!CheckIfCanEnqueue()) {
            return;
        }

        TUpdateAction action;
        ToProto(action.mutable_created_replica_data(), replicaData);
        EnqueueAction(std::move(action), "ReplicaCreated");
    }

    void OnReplicaDestroyed(TTableReplicaId replicaId)
    {
        if (!CheckIfCanEnqueue()) {
            return;
        }

        TUpdateAction action;
        ToProto(action.mutable_destroyed_replica_id(), replicaId);
        EnqueueAction(std::move(action), "ReplicaDestroyed");
    }

    void OnReplicationCollocationCreated(const TTableCollocationData& collocationData)
    {
        if (!CheckIfCanEnqueue()) {
            return;
        }

        TUpdateAction action;
        ToProto(action.mutable_created_collocation_data(), collocationData);
        EnqueueAction(std::move(action), "ReplicationCollocationCreated");
    }

    void OnReplicationCollocationDestroyed(TTableCollocationId collocationId)
    {
        if (!CheckIfCanEnqueue()) {
            return;
        }

        TUpdateAction action;
        ToProto(action.mutable_destroyed_collocation_id(), collocationId);
        EnqueueAction(std::move(action), "ReplicationCollocationDestroyed");
    }

    void EnqueueAction(TUpdateAction action, const TString& actionString)
    {
        auto revision = ++Revision_;
        action.set_revision(revision);

        auto guard = Guard(ActionQueueLock_);

        if (std::ssize(ActionQueue_) >= MaxActionQueueSize_.load()) {
            LoadingFromSnapshotRequested_.store(true);
            ActionQueue_.clear();
            guard.Release();
            YT_LOG_WARNING("Action queue is reset due to overflow (MaxActionQueueSize: %v, LastAction: %v)",
                MaxActionQueueSize_.load(),
                actionString);
            return;
        }

        ActionQueue_.push_back(std::move(action));

        guard.Release();

        YT_LOG_DEBUG("Rtt state provider enqueued new action (ActionRevision: %v, Action: %v)",
            revision,
            actionString);
    }

    void OnConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& newConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicatedTableTracker;
        TrackerEnabled_ = newConfig->EnableReplicatedTableTracker && newConfig->UseNewReplicatedTableTracker;
        MaxActionQueueSize_ = newConfig->MaxActionQueueSize;
    }

    TFuture<std::pair<TTrackerStateRevision, TReplicatedTableTrackerSnapshot>> GetSnapshotFuture()
    {
        return BIND([bootstrap = Bootstrap_, this_ = MakeStrong(this), this] {
            YT_LOG_DEBUG("Started building replicated table tracker snapshot");

            TReplicatedTableTrackerSnapshot snapshot;

            const auto& cypressManager = bootstrap->GetCypressManager();
            for (auto [id, node] : cypressManager->Nodes()) {
                if (IsObjectAlive(node) &&
                    node->IsTrunk() &&
                    node->GetType() == EObjectType::ReplicatedTable &&
                    !node->IsExternal())
                {
                    auto* table = node->As<NTableServer::TReplicatedTableNode>();
                    snapshot.ReplicatedTables.push_back(TReplicatedTableData{
                        .Id = table->GetId(),
                        .Options = table->GetReplicatedTableOptions(),
                    });

                    for (auto* replica : table->Replicas()) {
                        snapshot.Replicas.push_back(TReplicaData{
                            .TableId = table->GetId(),
                            .Id = replica->GetId(),
                            .Mode = replica->GetMode(),
                            .Enabled = replica->GetState() == ETableReplicaState::Enabled,
                            .ClusterName = replica->GetClusterName(),
                            .TablePath = replica->GetReplicaPath(),
                            .TrackingEnabled = replica->GetEnableReplicatedTableTracker(),
                            .ContentType = ETableReplicaContentType::Data,
                        });
                    }
                }
            }

            const auto& tableManager = bootstrap->GetTableManager();
            for (auto [id, tableCollocation] : tableManager->TableCollocations()) {
                if (IsObjectAlive(tableCollocation)) {
                    if (tableCollocation->GetType() == ETableCollocationType::Replication) {
                        std::vector<TTableId> tableIds;
                        tableIds.reserve(tableCollocation->Tables().size());
                        for (auto* table : tableCollocation->Tables()) {
                            if (IsObjectAlive(table)) {
                                tableIds.push_back(table->GetId());
                            }
                        }
                        if (!tableIds.empty()) {
                            snapshot.Collocations.push_back(TTableCollocationData{
                                .Id = tableCollocation->GetId(),
                                .TableIds = std::move(tableIds)
                            });
                        }
                    }
                }
            }

            YT_LOG_DEBUG("Finished building replicated table tracker snapshot");

            return std::make_pair(++Revision_, std::move(snapshot));
        })
            .AsyncVia(GetAutomatonInvoker())
            .Run();
    }

    IInvokerPtr GetAutomatonInvoker() const
    {
        return Bootstrap_
            ->GetHydraFacade()
            ->GetAutomatonInvoker(EAutomatonThreadQueue::ReplicatedTableTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicatedTableTrackerStateProviderPtr CreateReplicatedTableTrackerStateProvider(TBootstrap* bootstrap)
{
    return New<TReplicatedTableTrackerStateProvider>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
