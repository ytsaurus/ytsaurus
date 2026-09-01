#include "compaction_hint_fetching.h"
#include "compaction_hint_controllers.h"
#include "config.h"
#include "tablet.h"
#include "sorted_chunk_store.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTabletNode {

using namespace NYTree;
using namespace NLogging;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchThrottlers::TCompactionHintFetchThrottlers(
    const NLsm::TStoreCompactionHintArray<TCompactionHintFetcherConfigPtr>& configs)
{
    for (auto [storeKind, partitionKind] : NLsm::StoreCompactionHintKinds) {
        RequestThrottlers_[storeKind] = CreateReconfigurableThroughputThrottler(configs[storeKind]->RequestThrottler);
    }
}

void TCompactionHintFetchThrottlers::Reconfigure(
    const NLsm::TStoreCompactionHintArray<TCompactionHintFetcherConfigPtr>& configs)
{
    for (auto [storeKind, partitionKind] : NLsm::StoreCompactionHintKinds) {
        RequestThrottlers_[storeKind]->Reconfigure(configs[storeKind]->RequestThrottler);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipeline::TCompactionHintFetchPipeline(
    TSortedChunkStore* store,
    const TExponentialBackoffOptions& retryBackoffOptions)
    : Store_(store)
    , RetryBackoff_(retryBackoffOptions)
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

    TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("CompactionHintFetcher"));

    const auto& Logger = GetFetcher()->Context().Logger;

    YT_TLOG_DEBUG("Requesting compaction hint for store")
        .With("StoreId", Store_->GetId())
        .With("ChunkId", Store_->GetChunkId());

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

    YT_TLOG_DEBUG("No compaction hint for store")
        .With("StoreId", Store_->GetId())
        .With("ChunkId", Store_->GetChunkId());

    auto* partition = Store_->GetPartition();

    // NB(dave11ar): Be careful!
    // OnStoreHasNoHint will destroy |this|, because it resets strong pointers to TCompactionHintFetchPipeline.
    partition->CompactionHints().OnStoreHasNoHint(partition, Store_, GetStoreCompactionHintKind());
}

void TCompactionHintFetchPipeline::FinishFetch(NLsm::TStoreCompactionHint::TPayload&& payload)
{
    const auto& context = GetFetcher()->Context();
    const auto& Logger = context.Logger;

    YT_TLOG_DEBUG("Finished fetching compaction hint for store")
        .With("StoreId", Store_->GetId())
        .With("ChunkId", Store_->GetChunkId());

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

    RetryBackoff_.Next();

    auto backoffTime = RetryBackoff_.GetBackoff();

    YT_TLOG_WARNING("Failed to fetch compaction hint for store; retrying with backoff")
        .With("StoreId", Store_->GetId())
        .With("ChunkId", Store_->GetChunkId())
        .With("RetryIndex", RetryBackoff_.GetInvocationIndex())
        .With("BackoffTime", backoffTime)
        .With(error);

    context.FailedRequestCount.Increment();

    // The delayed callback only re-enqueues the pipeline; the actual request remains subject
    // to the fetcher throttler.
    TDelayedExecutor::Submit(
        BIND(&TCompactionHintFetchPipeline::Enqueue, MakeWeak(this)),
        backoffTime,
        GetEpochAutomatonInvoker());
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
    TCompactionHintFetcherConfigPtr config,
    IReconfigurableThroughputThrottlerPtr requestThrottler)
    : Config_(std::move(config))
    , Profiler_(profiler.WithTag("cell_id", ToString(cellId)))
    , RequestCount_(Profiler_.Counter("/request_count"))
    , ThrottledRequestCount_(Profiler_.Counter("/throttled_request_count"))
    , RequestThrottler_(std::move(requestThrottler))
    , Context_{
        .FinishedRequestCount = Profiler_.Counter("/finished_request_count"),
        .FailedRequestCount = Profiler_.Counter("/failed_request_count"),
        .ParseCumulativeTime = Profiler_.TimeCounter("/parse_cumulative_time"),
        .Logger = std::move(logger).WithTag("CellId", cellId),
    }
    , Logger(Context_.Logger)
{ }

void TCompactionHintFetcher::Start(IInvokerPtr epochAutomatonInvoker, TCompactionHintFetcherConfigPtr config)
{
    YT_ASSERT_INVOKER_THREAD_AFFINITY(epochAutomatonInvoker, AutomatonThread);
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsStopped());

    YT_TLOG_DEBUG("Starting compaction hint fetcher");

    Config_ = std::move(config);

    FetchingExecutor_ = New<TPeriodicExecutor>(
        std::move(epochAutomatonInvoker),
        BIND(&TCompactionHintFetcher::ExecuteEnqueuedPipelines, MakeWeak(this)),
        Config_->PeriodicExecutor);
    FetchingExecutor_->Start();
}

void TCompactionHintFetcher::Stop()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(Pipelines_.Empty());

    if (IsStopped()) {
        return;
    }

    YT_TLOG_DEBUG("Stopping compaction hint fetcher");

    YT_VERIFY(FetchingExecutor_->Stop().IsSet());
    FetchingExecutor_.Reset();
}

void TCompactionHintFetcher::Reconfigure(const TCompactionHintFetcherConfigPtr& config)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    if (IsStopped()) {
        return;
    }

    YT_TLOG_DEBUG("Reconfigure compaction hint fetcher");

    Config_ = config;

    FetchingExecutor_->SetOptions(Config_->PeriodicExecutor);
}

const TExponentialBackoffOptions& TCompactionHintFetcher::GetRetryBackoffOptions() const
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    return Config_->RetryBackoff;
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

    i64 requestCount = 0;
    while (!Pipelines_.Empty()) {
        if (RequestThrottler_->TryAcquireAvailable(1) == 0) {
            ThrottledRequestCount_.Increment();
            break;
        }

        // NB(dave11ar): Be careful!
        // Fetch can cancel fetching of other pipelines and remove element from Pipelines_.
        Pipelines_.PopBack()->Fetch();
        ++requestCount;
    }

    RequestCount_.Increment(requestCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
