#include "overload_reporter.h"

#include "bootstrap.h"
#include "config.h"
#include "tablet.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/orm/library/query/helpers.h>

#include <yt/yt/orm/library/query/heavy/expression_evaluator.h>

#include <yt/yt/ytlib/api/connection.h>

#include <yt/yt/ytlib/tablet_balancer_client/proto/tablet_balancer_service.pb.h>
#include <yt/yt/ytlib/tablet_balancer_client/tablet_balancer_service_proxy.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTabletNode {

using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NLogging;
using namespace NThreading;
using namespace NConcurrency;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletBalancer;
using namespace NTabletBalancerClient;

////////////////////////////////////////////////////////////////////////////////

class TExpressionEvaluatorCache
{
public:
    explicit TExpressionEvaluatorCache(i64 maxSize)
        : EvaluatorCache_(maxSize)
    { }

    void SetMaxSize(i64 maxSize)
    {
        auto guard = Guard(SpinLock_);
        EvaluatorCache_.SetMaxWeight(maxSize);
    }

    NOrm::NQuery::IExpressionEvaluatorPtr GetOrCreateExpressionEvaluator(const std::string& metric)
    {
        auto guard = Guard(SpinLock_);

        if (auto* evaluator = EvaluatorCache_.Find(metric)) {
            return *evaluator;
        }

        auto evaluator = NOrm::NQuery::CreateOrmExpressionEvaluator(
            ParseSource(metric, EParseMode::Expression),
            ParameterizedBalancingAttributes);

        EvaluatorCache_.Insert(metric, evaluator);

        return evaluator;
    }

private:
    TSpinLock SpinLock_;
    TSimpleLruCache<std::string, NOrm::NQuery::IExpressionEvaluatorPtr> EvaluatorCache_;
};

////////////////////////////////////////////////////////////////////////////////

class TOverloadReporter
    : public IOverloadReporter
{
public:
    TOverloadReporter(IBootstrap* const bootstrap)
        : Bootstrap_(bootstrap)
        , Logger(TabletNodeLogger().WithTag("OverloadReporter"))
        , ActionQueue_(New<TActionQueue>("TabOverloadReporter"))
        , Config_(Bootstrap_->GetTabletNodeDynamicConfig()->OverloadReporter)
        , EvaluatorCache_(Config_.Acquire()->MaxEvaluatorCacheSize)
    { }

    void Start() override
    {
        YT_LOG_INFO("Starting overload reporter");

        YT_VERIFY(!Executor_);

        auto config = Config_.Acquire();

        Executor_ = New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TOverloadReporter::DoReportOverload, MakeWeak(this)),
            config->PeriodicExecutor);

        if (config->Enable) {
            Executor_->Start();
        }
    }

    void Reconfigure(const TTabletNodeDynamicConfigPtr& config) override
    {
        YT_LOG_INFO("Reconfiguring overload reporter");

        Config_.Store(config->OverloadReporter);

        EvaluatorCache_.SetMaxSize(config->OverloadReporter->MaxEvaluatorCacheSize);

        // Reporter is not started.
        if (!Executor_) {
            return;
        }

        Executor_->SetOptions(config->OverloadReporter->PeriodicExecutor);

        if (config->OverloadReporter->Enable) {
            Executor_->Start();
        } else {
            YT_UNUSED_FUTURE(Executor_->Stop());
        }
    }

private:
    IBootstrap* const Bootstrap_;

    const TLogger Logger;

    const NConcurrency::TActionQueuePtr ActionQueue_;

    TAtomicIntrusivePtr<TOverloadReporterConfig> Config_;
    NConcurrency::TPeriodicExecutorPtr Executor_;

    TExpressionEvaluatorCache EvaluatorCache_;

    void LogTabletBalancerResponse(
        const std::string& bundleName,
        const std::vector<TTabletId>& tabletIds,
        const TTabletBalancerServiceProxy::TErrorOrRspRequestBalancingPtr& rspOrError) const
    {
        if (rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to send balancing request caused by tablets overload"
                "(BundleName: %v, TabletCount: %v)",
                bundleName,
                tabletIds.size());
        } else {
            YT_LOG_DEBUG("Successfully sent balancing request caused by tablets overload "
                "(BundleName: %v, TabletCount: %v)",
                bundleName,
                tabletIds.size());
        }
    }

    void DoReportOverload()
    {
        YT_ASSERT_INVOKER_AFFINITY(ActionQueue_->GetInvoker());

        auto config = Config_.Acquire();

        if (!config->Enable) {
            return;
        }

        YT_LOG_DEBUG("Starting overload reporter iteration");

        THashMap<std::string, std::vector<TTabletId>> bundleToTabletIds;

        for (const auto& tabletSnapshot : Bootstrap_->GetTabletSnapshotStore()->GetTabletSnapshots()) {
            if (IsTabletOverloaded(tabletSnapshot)) {
                bundleToTabletIds[tabletSnapshot->TabletCellBundle].push_back(tabletSnapshot->TabletId);
            }
        }

        for (const auto& [bundleName, tabletIds] : bundleToTabletIds) {
            TTabletBalancerServiceProxy proxy(Bootstrap_->GetConnection()->GetTabletBalancerChannel());
            auto req = proxy.RequestBalancing();

            req->set_bundle_name(bundleName);
            ToProto(req->mutable_tablet_ids(), tabletIds);

            req->set_mode(ToProto(EBalancingRequestMode::Reshard));
            req->set_reason("overload");

            req->Invoke().Subscribe(BIND(
                &TOverloadReporter::LogTabletBalancerResponse,
                MakeWeak(this),
                std::move(bundleName),
                std::move(tabletIds)));
        }

        YT_LOG_DEBUG("Finished overload reporter iteration");
    }

    bool IsTabletOverloaded(const TTabletSnapshotPtr& tabletSnapshot)
    {
        YT_ASSERT_INVOKER_AFFINITY(ActionQueue_->GetInvoker());

        const auto& overloadConfig = tabletSnapshot->Settings.MountConfig->OverloadReactiveBalancing;
        if (!overloadConfig) {
            return false;
        }

        auto evaluator = EvaluatorCache_.GetOrCreateExpressionEvaluator(overloadConfig->Metric);

        auto rowBuffer = New<TRowBuffer>();
        auto metricValueOrError = evaluator->Evaluate(
            {CollectSizeMetrics(tabletSnapshot), CollectPerformanceCounters(tabletSnapshot)},
            rowBuffer);

        if (!metricValueOrError.IsOK()) {
            YT_LOG_DEBUG(metricValueOrError, "Failed to calculate if tablet is overloaded, ignored "
                "(%v, Metric: %v)",
                tabletSnapshot->LoggingTag,
                overloadConfig->Metric);

            return false;
        }

        try {
            double metricValue = ExtractMetricValue(
                metricValueOrError.Value(),
                overloadConfig->Metric,
                tabletSnapshot->TabletId,
                tabletSnapshot->TableId);

            bool overloaded = metricValue > overloadConfig->Limit;

            YT_LOG_DEBUG_IF(overloaded, "Tablet is overloaded (%v, Metric: %v, MetricValue: %v, Limit: %v)",
                tabletSnapshot->LoggingTag,
                overloadConfig->Metric,
                metricValue,
                overloadConfig->Limit);

            return overloaded;
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to extract metric value calculating tablet is overloaded "
                "(%v, Metric: %v)",
                tabletSnapshot->LoggingTag,
                overloadConfig->Metric);

            return false;
        }
    }

    static TYsonString CollectSizeMetrics(const TTabletSnapshotPtr& tabletSnapshot)
    {
        const auto& sizeMetrics = tabletSnapshot->TabletSizeMetrics;

        i64 memorySize;
        switch (tabletSnapshot->Settings.MountConfig->InMemoryMode) {
            case EInMemoryMode::Compressed:
                memorySize = sizeMetrics.CompressedDataSize;
                break;

            case EInMemoryMode::Uncompressed:
                memorySize = sizeMetrics.UncompressedDataSize;
                break;

            case EInMemoryMode::None:
                memorySize = 0;
                break;
        }

        return BuildYsonStringFluently()
            .BeginMap()
                .Item("compressed_data_size").Value(sizeMetrics.CompressedDataSize)
                .Item("uncompressed_data_size").Value(sizeMetrics.UncompressedDataSize)
                .Item("memory_size").Value(memorySize)
                .Item("partition_count").Value(ssize(tabletSnapshot->PartitionList))
            .EndMap();
    }

    static TYsonString CollectPerformanceCounters(const TTabletSnapshotPtr& tabletSnapshot)
    {
        const auto& performanceCounters = tabletSnapshot->PerformanceCounters;

        #define XX(name, Name) performanceCounters->Name.UpdateEma();
        ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
        ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
        #undef XX

        return BuildYsonStringFluently()
            .BeginMap()
                #define XX(name, Name) \
                .Item(#name "_count").Value(performanceCounters->Name.Ema.Count) \
                .Item(#name "_rate").Value(performanceCounters->Name.Ema.ImmediateRate) \
                .Item(#name "_10m_rate").Value(performanceCounters->Name.Ema.WindowRates[0]) \
                .Item(#name "_1h_rate").Value(performanceCounters->Name.Ema.WindowRates[1])
                ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
                ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
                #undef XX
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IOverloadReporterPtr CreateOverloadReporter(IBootstrap* const bootstrap)
{
    return New<TOverloadReporter>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
