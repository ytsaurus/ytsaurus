#include "cluster_state_provider.h"

#include "bootstrap.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NTabletClient;
using namespace NThreading;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

namespace {

static const TYPath BannedReplicaClustersPath("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters");

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> GetBannedReplicaClusters(const NApi::NNative::IClientPtr& client)
{
    auto bannedReplicaClusters = ConvertTo<IListNodePtr>(
        WaitFor(client->GetNode(BannedReplicaClustersPath))
            .ValueOrThrow());

    THashSet<std::string> bannedReplicaClustersSet;
    for (const auto& cluster : bannedReplicaClusters->GetChildren()) {
        bannedReplicaClustersSet.insert(cluster->AsString()->GetValue());
    }
    return bannedReplicaClustersSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

class TClusterStateProvider
    : public IClusterStateProvider
{
public:
    TClusterStateProvider(
        IBootstrap* bootstrap,
        TClusterStateProviderConfigPtr config,
        IInvokerPtr controlInvoker);

    void Start() override;
    void Stop() override;

    void Reconfigure(TClusterStateProviderConfigPtr config) override;

    TFuture<IListNodePtr> GetBundles() override;
    TFuture<IListNodePtr> GetNodes() override;
    TFuture<THashMap<std::string, std::vector<std::string>>> GetUnhealthyBundles() override;
    TFuture<THashSet<std::string>> GetBannedReplicasFromMetaCluster() override;

private:
    IBootstrap* const Bootstrap_;
    const IInvokerPtr ControlInvoker_;

    const IThreadPoolPtr WorkerPool_;
    const TPeriodicExecutorPtr PollExecutor_;

    TAtomicIntrusivePtr<TClusterStateProviderConfig> Config_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    TInstant LastBundlesSuccessfulFetchTime_;
    TInstant LastNodesSuccessfulFetchTime_;
    TInstant LastUnhealthyBundlesSuccessfulFetchTime_;
    TInstant LastBannedReplicasSuccessfulFetchTime_;

    IListNodePtr Bundles_;
    IListNodePtr Nodes_;
    THashMap<std::string, std::vector<std::string>> UnhealthyBundles_;
    THashSet<std::string> BannedReplicasFromMetaCluster_;

    TFuture<IListNodePtr> BundlesFuture_;
    TFuture<IListNodePtr> NodesFuture_;
    TFuture<THashMap<std::string, std::vector<std::string>>> UnhealthyBundlesFuture_;
    TFuture<THashSet<std::string>> BannedReplicasFuture_;

private:
    void FetchState();

    IListNodePtr FetchBundles();
    IListNodePtr FetchNodes();
    THashMap<std::string, std::vector<std::string>> TryFetchUnhealthyBundles();
    THashMap<std::string, std::vector<std::string>> FetchUnhealthyBundles() const;
    THashSet<std::string> FetchBannedReplicasFromMetaCluster(const std::string& cluster);
};

TClusterStateProvider::TClusterStateProvider(
    IBootstrap* bootstrap,
    TClusterStateProviderConfigPtr config,
    IInvokerPtr controlInvoker)
    : Bootstrap_(bootstrap)
    , ControlInvoker_(std::move(controlInvoker))
    , WorkerPool_(CreateThreadPool(
        config->WorkerThreadPoolSize,
        "ClusterStatePool"))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TClusterStateProvider::FetchState, MakeWeak(this)),
        config->FetchPlannerPeriod))
    , Config_(std::move(config))
{ }

void TClusterStateProvider::Start()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    {
        auto guard = WriterGuard(Lock_);

        BundlesFuture_.Reset();
        NodesFuture_.Reset();
        UnhealthyBundlesFuture_.Reset();
        BannedReplicasFuture_.Reset();
    }

    PollExecutor_->Start();
}

void TClusterStateProvider::Stop()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    YT_UNUSED_FUTURE(PollExecutor_->Stop());
}

void TClusterStateProvider::Reconfigure(TClusterStateProviderConfigPtr config)
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    auto oldConfig = Config_.Acquire();
    auto oldFetchPeriod = oldConfig->FetchPlannerPeriod;
    auto newFetchPeriod = config->FetchPlannerPeriod;

    auto oldThreadCount = oldConfig->WorkerThreadPoolSize;
    auto newThreadCount = config->WorkerThreadPoolSize;

    Config_.Store(std::move(config));
    if (oldFetchPeriod != newFetchPeriod) {
        PollExecutor_->SetPeriod(newFetchPeriod);
    }

    if (oldThreadCount != newThreadCount) {
        WorkerPool_->SetThreadCount(newThreadCount);
    }
}

TFuture<IListNodePtr> TClusterStateProvider::GetBundles()
{
    auto now = Now();
    auto readerGuard = ReaderGuard(Lock_);
    if (now <= LastBundlesSuccessfulFetchTime_ + Config_.Acquire()->BundlesFreshnessTime) {
        return MakeFuture(Bundles_);
    }

    if (!BundlesFuture_) {
        readerGuard.Release();
        YT_LOG_DEBUG("Fetching bundles out-of-band due to a direct request");
        auto writerGuard = WriterGuard(Lock_);
        if (!BundlesFuture_) {
            BundlesFuture_ = BIND(&TClusterStateProvider::FetchBundles, MakeStrong(this))
                .AsyncVia(WorkerPool_->GetInvoker())
                .Run();
        }
        return BundlesFuture_;
    }

    return BundlesFuture_;
}

TFuture<IListNodePtr> TClusterStateProvider::GetNodes()
{
    auto now = Now();
    auto readerGuard = ReaderGuard(Lock_);
    if (now <= LastNodesSuccessfulFetchTime_ + Config_.Acquire()->NodesFreshnessTime) {
        return MakeFuture(Nodes_);
    }

    if (!NodesFuture_) {
        readerGuard.Release();
        YT_LOG_DEBUG("Planning to fetch node statistics due to a direct request");
        auto writerGuard = WriterGuard(Lock_);
        if (!NodesFuture_) {
            NodesFuture_ = BIND(&TClusterStateProvider::FetchNodes, MakeStrong(this))
                .AsyncVia(WorkerPool_->GetInvoker())
                .Run();
        }
        return NodesFuture_;
    }

    return NodesFuture_;
}

TFuture<THashMap<std::string, std::vector<std::string>>> TClusterStateProvider::GetUnhealthyBundles()
{
    auto now = Now();
    auto readerGuard = ReaderGuard(Lock_);
    if (now <= LastUnhealthyBundlesSuccessfulFetchTime_ + Config_.Acquire()->UnhealthyBundlesFreshnessTime) {
        return MakeFuture(UnhealthyBundles_);
    }

    if (!UnhealthyBundlesFuture_) {
        readerGuard.Release();
        YT_LOG_DEBUG("Planning to fetch unhealthy bundles due to a direct request");
        auto writerGuard = WriterGuard(Lock_);
        if (!UnhealthyBundlesFuture_) {
            UnhealthyBundlesFuture_ = BIND(&TClusterStateProvider::TryFetchUnhealthyBundles, MakeStrong(this))
                .AsyncVia(WorkerPool_->GetInvoker())
                .Run();
        }
        return UnhealthyBundlesFuture_;
    }

    return UnhealthyBundlesFuture_;
}

TFuture<THashSet<std::string>> TClusterStateProvider::GetBannedReplicasFromMetaCluster()
{
    auto config = Config_.Acquire();
    if (config->MetaClusterForBannedReplicas.empty()) {
        return MakeFuture(THashSet<std::string>{});
    }

    auto now = Now();
    auto readerGuard = ReaderGuard(Lock_);
    if (now <= LastBannedReplicasSuccessfulFetchTime_ + config->BannedReplicasFreshnessTime) {
        return MakeFuture(BannedReplicasFromMetaCluster_);
    }

    if (!BannedReplicasFuture_) {
        readerGuard.Release();
        YT_LOG_DEBUG("Planning to fetch banned replica clusters due to a direct request");
        auto writerGuard = WriterGuard(Lock_);
        if (!BannedReplicasFuture_) {
            BannedReplicasFuture_ = BIND(
                &TClusterStateProvider::FetchBannedReplicasFromMetaCluster,
                MakeStrong(this),
                config->MetaClusterForBannedReplicas)
                .AsyncVia(WorkerPool_->GetInvoker())
                .Run();
        }
        return BannedReplicasFuture_;
    }

    return BannedReplicasFuture_;
}

void TClusterStateProvider::FetchState()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    auto config = Config_.Acquire();

    auto writerGuard = WriterGuard(Lock_);
    auto now = Now();

    YT_LOG_DEBUG("Started to plan cluster state provider fetches (Config: %v, "
        "LastBundlesSuccessfulFetchTime: %v, LastNodesSuccessfulFetchTime: %v, "
        "LastUnhealthyBundlesSuccessfulFetchTime: %v, HasBundleFuture: %v, "
        "HasNodeFuture: %v, HasUnhealthyBundlesFuture: %v)",
        ConvertToYsonString(config, NYson::EYsonFormat::Text),
        LastBundlesSuccessfulFetchTime_,
        LastNodesSuccessfulFetchTime_,
        LastUnhealthyBundlesSuccessfulFetchTime_,
        static_cast<bool>(BundlesFuture_),
        static_cast<bool>(NodesFuture_),
        static_cast<bool>(UnhealthyBundlesFuture_));

    if (LastBundlesSuccessfulFetchTime_ + config->BundlesFetchPeriod < now && !BundlesFuture_) {
        BundlesFuture_ = BIND(&TClusterStateProvider::FetchBundles, MakeStrong(this))
            .AsyncVia(WorkerPool_->GetInvoker())
            .Run();
    }

    if (LastNodesSuccessfulFetchTime_ + config->NodesFetchPeriod < now && !NodesFuture_) {
        NodesFuture_ = BIND(&TClusterStateProvider::FetchNodes, MakeStrong(this))
            .AsyncVia(WorkerPool_->GetInvoker())
            .Run();
    }

    if (!config->ClustersForBundleHealthCheck.empty() &&
        LastUnhealthyBundlesSuccessfulFetchTime_ + config->UnhealthyBundlesFetchPeriod < now &&
        !UnhealthyBundlesFuture_)
    {
        UnhealthyBundlesFuture_ = BIND(&TClusterStateProvider::TryFetchUnhealthyBundles, MakeStrong(this))
            .AsyncVia(WorkerPool_->GetInvoker())
            .Run();
    }

    if (LastBannedReplicasSuccessfulFetchTime_ + config->BannedReplicasFetchPeriod < now &&
        !BannedReplicasFuture_)
    {
        BannedReplicasFuture_ = BIND(
            &TClusterStateProvider::FetchBannedReplicasFromMetaCluster,
            MakeStrong(this),
            config->MetaClusterForBannedReplicas)
            .AsyncVia(WorkerPool_->GetInvoker())
            .Run();
    }
}

IListNodePtr TClusterStateProvider::FetchBundles()
{
    auto config = Config_.Acquire();

    std::vector<std::string> attributeKeys{"health", "tablet_balancer_config", "tablet_cell_ids"};
    if (config->FetchTabletActionsBundleAttribute) {
        attributeKeys.push_back("tablet_actions");
    }

    TListNodeOptions options;
    options.Attributes = attributeKeys;

    YT_LOG_DEBUG("Started fetching bundle list");

    auto bundleOrError = WaitFor(Bootstrap_
        ->GetClient()
        ->ListNode(TabletCellBundlesPath, options));

    if (!bundleOrError.IsOK()) {
        YT_LOG_ERROR(bundleOrError, "Failed to fetch bundle list");
        auto guard = WriterGuard(Lock_);
        BundlesFuture_.Reset();
        bundleOrError.ThrowOnError();
    }

    auto now = Now();
    auto bundleList = ConvertTo<IListNodePtr>(bundleOrError.Value());
    auto guard = WriterGuard(Lock_);
    if (LastBundlesSuccessfulFetchTime_ < now) {
        LastBundlesSuccessfulFetchTime_ = now;
        Bundles_ = std::move(bundleList);
    }

    BundlesFuture_.Reset();

    YT_LOG_DEBUG("Finished fetching bundle list");
    return Bundles_;
}

IListNodePtr TClusterStateProvider::FetchNodes()
{
    TListNodeOptions options;
    options.Attributes = TAttributeFilter({}, {TabletStaticPath, TabletSlotsPath});

    static const TString TabletNodesPath = "//sys/tablet_nodes";

    YT_LOG_DEBUG("Started fetching node statistics");

    auto nodesOrError = WaitFor(Bootstrap_
        ->GetClient()
        ->ListNode(TabletNodesPath, options));

    if (!nodesOrError.IsOK()) {
        YT_LOG_ERROR(nodesOrError, "Failed to fetch node statistics");
        auto guard = WriterGuard(Lock_);
        NodesFuture_.Reset();
        nodesOrError.ThrowOnError();
    }

    auto now = Now();
    auto nodesList = ConvertTo<IListNodePtr>(nodesOrError.Value());
    auto guard = WriterGuard(Lock_);
    if (LastNodesSuccessfulFetchTime_ < now) {
        LastNodesSuccessfulFetchTime_ = now;
        Nodes_ = std::move(nodesList);
    }

    NodesFuture_.Reset();

    YT_LOG_DEBUG("Finished fetching node statistics");
    return Nodes_;
}

THashMap<std::string, std::vector<std::string>> TClusterStateProvider::TryFetchUnhealthyBundles()
{
    try {
        YT_LOG_DEBUG("Started fetching unhealthy bundles");
        auto unhealthyBundles = FetchUnhealthyBundles();

        auto now = Now();
        auto guard = WriterGuard(Lock_);
        if (LastUnhealthyBundlesSuccessfulFetchTime_ < now) {
            LastUnhealthyBundlesSuccessfulFetchTime_ = now;
            UnhealthyBundles_ = std::move(unhealthyBundles);
        }

        UnhealthyBundlesFuture_.Reset();

        YT_LOG_DEBUG("Finished fetching unhealthy bundles");
        return UnhealthyBundles_;
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to fetch unhealthy bundles");
        auto guard = WriterGuard(Lock_);
        UnhealthyBundlesFuture_.Reset();
        throw;
    }
}

THashMap<std::string, std::vector<std::string>> TClusterStateProvider::FetchUnhealthyBundles() const
{
    auto config = Config_.Acquire();
    if (config->ClustersForBundleHealthCheck.empty()) {
        return {};
    }

    auto bannedReplicaClusters = GetBannedReplicaClusters(Bootstrap_->GetClient());
    YT_LOG_DEBUG_IF(
        !bannedReplicaClusters.empty(),
        "Fetched banned replica clusters (Clusters: %v)",
        bannedReplicaClusters);

    TListNodeOptions options{.Attributes = {"health"}};
    const auto& clientDirectory = Bootstrap_->GetClientDirectory();
    THashMap<std::string, std::vector<std::string>> unhealthyBundles;
    for (const auto& cluster : config->ClustersForBundleHealthCheck) {
        if (bannedReplicaClusters.contains(cluster)) {
            continue;
        }

        auto client = clientDirectory->GetClientOrThrow(cluster);
        auto bundles = WaitFor(client->ListNode(TabletCellBundlesPath, options))
            .ValueOrThrow();
        auto bundlesList = ConvertTo<IListNodePtr>(bundles);
        std::vector<std::string> clusterUnhealthyBundles;
        for (const auto& bundle : bundlesList->GetChildren()) {
            auto health = bundle->Attributes().Get<ETabletCellHealth>("health");
            if (health != ETabletCellHealth::Good) {
                clusterUnhealthyBundles.push_back(bundle->AsString()->GetValue());
            }
        }

        if (!clusterUnhealthyBundles.empty()) {
            EmplaceOrCrash(unhealthyBundles, cluster, std::move(clusterUnhealthyBundles));
        }
    }

    return unhealthyBundles;
}

THashSet<std::string> TClusterStateProvider::FetchBannedReplicasFromMetaCluster(
    const std::string& cluster)
{
    try {
        THashSet<std::string> bannedReplicaClusters;
        YT_LOG_DEBUG("Started fetching banned replica clusters");

        if (!cluster.empty()) {
            const auto& clientDirectory = Bootstrap_->GetClientDirectory();
            auto client = clientDirectory->GetClientOrThrow(cluster);
            bannedReplicaClusters = GetBannedReplicaClusters(client);
            YT_LOG_DEBUG_IF(
                !bannedReplicaClusters.empty(),
                "Fetched banned replica clusters (Clusters: %v, MetaCluster: %v)",
                bannedReplicaClusters,
                cluster);
        }

        auto now = Now();
        auto guard = WriterGuard(Lock_);
        if (LastBannedReplicasSuccessfulFetchTime_ < now) {
            LastBannedReplicasSuccessfulFetchTime_ = now;
            BannedReplicasFromMetaCluster_ = std::move(bannedReplicaClusters);
        }

        BannedReplicasFuture_.Reset();

        YT_LOG_DEBUG("Finished fetching banned replica clusters");
        return BannedReplicasFromMetaCluster_;

    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to fetch banned replica clusters");
        auto guard = WriterGuard(Lock_);
        BannedReplicasFuture_.Reset();
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

IClusterStateProviderPtr CreateClusterStateProvider(
    IBootstrap* bootstrap,
    TClusterStateProviderConfigPtr config,
    IInvokerPtr controlInvoker)
{
    return New<TClusterStateProvider>(bootstrap, config, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
