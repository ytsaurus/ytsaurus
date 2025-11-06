#include "cluster_state_provider.h"

#include "bootstrap.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NThreading;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

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

private:
    const IBootstrap* Bootstrap_;
    const IInvokerPtr ControlInvoker_;

    const IThreadPoolPtr WorkerPool_;
    const TPeriodicExecutorPtr PollExecutor_;

    TAtomicIntrusivePtr<TClusterStateProviderConfig> Config_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    TInstant LastBundlesSuccessfulFetchTime_;
    TInstant LastNodesSuccessfulFetchTime_;

    IListNodePtr Bundles_;
    IListNodePtr Nodes_;

    TFuture<IListNodePtr> BundlesFuture_;
    TFuture<IListNodePtr> NodesFuture_;

private:
    void FetchState();

    IListNodePtr FetchBundles();
    IListNodePtr FetchNodes();
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

void TClusterStateProvider::FetchState()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    auto config = Config_.Acquire();

    auto writerGuard = WriterGuard(Lock_);
    auto now = Now();

    YT_LOG_DEBUG("Started to plan cluster state provider fetches (Config: %v, "
        "LastBundlesSuccessfulFetchTime: %v, LastNodesSuccessfulFetchTime: %v, "
        "HasBundleFuture: %v, HasNodeFuture: %v)",
        ConvertToYsonString(config, NYson::EYsonFormat::Text),
        LastBundlesSuccessfulFetchTime_,
        LastNodesSuccessfulFetchTime_,
        static_cast<bool>(BundlesFuture_),
        static_cast<bool>(NodesFuture_));

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
}

IListNodePtr TClusterStateProvider::FetchBundles()
{
    TListNodeOptions options;
    options.Attributes = {"health", "tablet_balancer_config", "tablet_cell_ids", "tablet_actions"};

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
        Bundles_ = bundleList;
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
        Nodes_ = nodesList;
    }

    NodesFuture_.Reset();

    YT_LOG_DEBUG("Finished fetching node statistics");
    return Nodes_;
}

IClusterStateProviderPtr CreateClusterStateProvider(
    IBootstrap* bootstrap,
    TClusterStateProviderConfigPtr config,
    IInvokerPtr controlInvoker)
{
    return New<TClusterStateProvider>(bootstrap, config, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
