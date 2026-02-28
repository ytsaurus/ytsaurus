#include "compaction_hint_fetching.h"
#include "compaction_hint_controllers.h"
#include "config.h"
#include "tablet.h"
#include "sorted_chunk_store.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NTabletNode {

using namespace NYTree;
using namespace NLogging;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipeline::TCompactionHintFetchPipeline(TSortedChunkStore* store)
    : Store_(store)
{ }

void TCompactionHintFetchPipeline::Enqueue()
{
    // Fetcher can be null in tests.
    if (const auto& fetcher = GetFetcher()) {
        fetcher->EnqueuePipeline(this);
    }
}

void TCompactionHintFetchPipeline::Fetch()
{
    YT_VERIFY(std::holds_alternative<std::monostate>(Payload_));

    const auto& Logger = GetFetcher()->Context().Logger;

    YT_LOG_DEBUG("Requesting compaction hint for store (StoreId: %v, ChunkId: %v)",
        Store_->GetId(),
        Store_->GetChunkId());

    DoFetch();
}

const TCompactionHintFetcherPtr& TCompactionHintFetchPipeline::GetFetcher() const
{
    return Store_->GetTablet()->GetCompactionHintFetcher(GetStoreCompactionHintKind());
}

void TCompactionHintFetchPipeline::ExecuteParse(const std::function<void()>& parser) const
{
    TWallTimer timer;
    parser();
    GetFetcher()->Context().ParseCumulativeTime.Add(timer.GetElapsedTime());
}

void TCompactionHintFetchPipeline::OnStoreHasNoHint()
{
    const auto& Logger = GetFetcher()->Context().Logger;

    YT_LOG_DEBUG("No compaction hint for store (StoreId: %v, ChunkId: %v)",
        Store_->GetId(),
        Store_->GetChunkId());

    auto* partition = Store_->GetPartition();

    // NB(dave11ar): Be careful!
    // OnStoreHasNoHint will destroy |this|, because it resets strong pointers to TCompactionHintFetchPipeline.
    partition->CompactionHints().OnStoreHasNoHint(partition, Store_, GetStoreCompactionHintKind());
}

void TCompactionHintFetchPipeline::FinishFetch(NLsm::TStoreCompactionHint::TPayload&& payload)
{
    const auto& context = GetFetcher()->Context();
    const auto& Logger = context.Logger;

    YT_LOG_DEBUG("Finished fetching compaction hint for store (StoreId: %v, ChunkId: %v)",
        Store_->GetId(),
        Store_->GetChunkId());

    Payload_ = std::move(payload);

    context.FinishedRequestCount.Increment();
}

IInvokerPtr TCompactionHintFetchPipeline::GetEpochAutomatonInvoker() const
{
    return Store_->GetTablet()->GetEpochAutomatonInvoker();
}

void TCompactionHintFetchPipeline::OnRequestFailed(const TError& error)
{
    const auto& context = GetFetcher()->Context();
    const auto& Logger = context.Logger;

    YT_LOG_WARNING(error, "Failed to fetch compaction hint for store, retry (StoreId: %v, ChunkId: %v)",
        Store_->GetId(),
        Store_->GetChunkId());

    context.FailedRequestCount.Increment();

    // Pipeline is stateless, so we can just put it in fetcher to retry.
    GetFetcher()->EnqueuePipeline(this);
}

////////////////////////////////////////////////////////////////////////////////

const TPerCellCompactionHintFetchingContext& TCompactionHintFetcher::Context() const noexcept
{
    return Context_;
}

TCompactionHintFetcher::TCompactionHintFetcher(
    TTabletCellId cellId,
    TLogger logger,
    const TProfiler& profiler,
    TCompactionHintFetcherConfigPtr config)
    : Config_(std::move(config))
    , Profiler_(profiler.WithTag("cell_id", ToString(cellId)))
    , RequestCount_(Profiler_.Counter("/request_count"))
    , ThrottledRequestCount_(Profiler_.Counter("/throttled_request_count"))
    , RequestThrottler_(CreateReconfigurableThroughputThrottler(Config_->RequestThrottler))
    , Context_{
        .FinishedRequestCount = Profiler_.Counter("/finished_request_count"),
        .FailedRequestCount = Profiler_.Counter("/failed_request_count"),
        .ParseCumulativeTime = Profiler_.TimeCounter("/parse_cumulative_time"),
        .Logger = std::move(logger).WithTag("CellId: %v", cellId),
    }
    , Logger(Context_.Logger)
{ }

void TCompactionHintFetcher::Start(IInvokerPtr epochAutomatonInvoker, TCompactionHintFetcherConfigPtr config)
{
    YT_ASSERT_INVOKER_THREAD_AFFINITY(epochAutomatonInvoker, AutomatonThread);
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsStopped());

    YT_LOG_DEBUG("Starting compaction hint fetcher");

    Config_ = std::move(config);

    FetchingExecutor_ = New<TPeriodicExecutor>(
        std::move(epochAutomatonInvoker),
        BIND(&TCompactionHintFetcher::ExecuteEnqueuedPipelines, MakeWeak(this)),
        Config_->PeriodicExecutor);
    FetchingExecutor_->Start();

    RequestThrottler_->Reconfigure(Config_->RequestThrottler);
}

void TCompactionHintFetcher::Stop()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(Pipelines_.Empty());

    if (IsStopped()) {
        return;
    }

    YT_LOG_DEBUG("Stopping compaction hint fetcher");

    YT_VERIFY(FetchingExecutor_->Stop().IsSet());
    FetchingExecutor_.Reset();
}

void TCompactionHintFetcher::Reconfigure(const TCompactionHintFetcherConfigPtr& config)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    if (IsStopped()) {
        return;
    }

    YT_LOG_DEBUG("Reconfigure compaction hint fetcher");

    Config_ = config;

    FetchingExecutor_->SetOptions(Config_->PeriodicExecutor);

    RequestThrottler_->Reconfigure(Config_->RequestThrottler);
}

void TCompactionHintFetcher::EnqueuePipeline(const TCompactionHintFetchPipelinePtr& pipeline)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(!IsStopped());
    YT_VERIFY(std::holds_alternative<std::monostate>(pipeline->Payload()));
    YT_VERIFY(pipeline->Empty());

    Pipelines_.PushFront(pipeline.Get());
}

bool TCompactionHintFetcher::IsStopped() const
{
    return !FetchingExecutor_;
}

void TCompactionHintFetcher::ExecuteEnqueuedPipelines()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(!IsStopped());

    if (Pipelines_.Empty()) {
        return;
    }

    i64 limit = RequestThrottler_->GetAvailable();

    if (limit == 0) {
        ThrottledRequestCount_.Increment();
        return;
    }

    i64 remainingLimit = limit;
    for (; remainingLimit > 0; --remainingLimit) {
        // NB(dave11ar): Be careful!
        // Fetch can cancel fetching of other pipelines and remove element from Pipelines_.
        if (Pipelines_.Empty()) {
            break;
        }

        Pipelines_.PopBack()->Fetch();
    }

    i64 requestCount = limit - remainingLimit;
    YT_VERIFY(RequestThrottler_->TryAcquire(remainingLimit));
    RequestCount_.Increment(requestCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
