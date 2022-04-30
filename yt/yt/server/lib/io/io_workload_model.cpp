#include "io_workload_model.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/actions/bind.h>

#include <util/generic/bitops.h>

#include <library/cpp/histogram/hdr/histogram.h>

#include <yt/yt/core/profiling/timing.h>

#include <numeric>

namespace NYT::NIO {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const std::vector<i64> RequestSizeBins = {
    4_KB, 16_KB, 64_KB, 128_KB, 512_KB, 1_MB, 4_MB, 16_MB, 64_MB, 128_MB
};

static const std::vector<i64> RequestLatencyBins = {
    1, 5, 10, 20, 40, 60, 80, 100, 200, 400, 600, 1000, 4000
};

////////////////////////////////////////////////////////////////////////////////

void TFixedBinsHistogramBase::RecordValue(i64 value, i64 count)
{
    const int binsCount = std::ssize(BinValues_);
    auto it = std::lower_bound(BinValues_.begin(), BinValues_.end(), value);
    int index = std::distance(BinValues_.begin(), it);
    if (index == binsCount) {
        index = binsCount - 1;
    }
    Counters_[index] += count;
}

TFixedBinsHistogramBase::TFixedBinsHistogramBase(TBins bins)
    : BinValues_(std::move(bins))
    , Counters_(BinValues_.size())
{ }

const TFixedBinsHistogramBase::TBins& TFixedBinsHistogramBase::GetBins() const
{
    return BinValues_;
}

const TFixedBinsHistogramBase::TCounters& TFixedBinsHistogramBase::GetCounters() const
{
    return Counters_;
}

TRequestSizeHistogram::TRequestSizeHistogram()
    : TFixedBinsHistogramBase(RequestSizeBins)
{ }

TRequestLatencyHistogram::TRequestLatencyHistogram()
    : TFixedBinsHistogramBase(RequestLatencyBins)
{ }

THistogramSummary ComputeHistogramSummary(const TFixedBinsHistogramBase& hist)
{
    const auto& counters = hist.GetCounters();

    THistogramSummary summary;
    int currentBinIndex = 0;
    summary.TotalCount = std::accumulate(counters.begin(), counters.end(), 0LL);

    i64 counter = 0;
    auto computeNextQuantile = [&] (double quantile) {
        while (currentBinIndex < std::ssize(counters) - 1 && counter < quantile * summary.TotalCount) {
            counter += counters[currentBinIndex];
            ++currentBinIndex;
        }
        return hist.GetBins()[currentBinIndex];
    };

    summary.P90 = computeNextQuantile(0.9);
    summary.P99 = computeNextQuantile(0.99);
    summary.P99_9 = computeNextQuantile(0.999);
    summary.P99_99 = computeNextQuantile(0.9999);
    summary.Max = computeNextQuantile(1);
    return summary;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    TEnumIndexedVector<EWorkloadCategory, TRequestSizeHistogram>& statsByCategory,
    TStringBuf /*spec*/)
{
    builder->AppendString("{");

    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        const auto& histogram = statsByCategory[category];
        const auto& counters = histogram.GetCounters();
        const auto& bins = histogram.GetBins();

        if (std::reduce(counters.begin(), counters.end()) == 0) {
            continue;
        }

        builder->AppendFormat("\"%v\": {", category);
        for (int index = 0; index < std::ssize(counters); ++index) {
            if (counters[index]) {
                builder->AppendFormat("%v:%v, ", bins[index], counters[index]);
            }
        }
        builder->AppendString("}, ");
    }

    builder->AppendString("}");
}

////////////////////////////////////////////////////////////////////////////////

// TODO(capone212): make config parameter.
static const auto RequestSizesModelingPeriod = TDuration::Minutes(5);
static const auto RequestLatenciesModelingPeriod = TDuration::Seconds(5);

class TWorkloadModelManager
    : public virtual TRefCounted
{
public:
    TWorkloadModelManager(TString locationId, NLogging::TLogger logger)
        : LocationId_(std::move(locationId))
        , Logger(std::move(logger))
        , ActionQueue_(New<TActionQueue>("WorkloadModelManager"))
        , Invoker_(ActionQueue_->GetInvoker())
        , ModelCreationRoundExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TWorkloadModelManager::OnModelCreationRound, MakeWeak(this)),
            RequestSizesModelingPeriod))
        , LatenciesMeasuringExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TWorkloadModelManager::OnLatenciesReportingRound, MakeWeak(this)),
            RequestLatenciesModelingPeriod))
    {
        YT_LOG_DEBUG("Initialized workload model manager");

        ModelCreationRoundExecutor_->Start();
        LatenciesMeasuringExecutor_->Start();
    }

    ~TWorkloadModelManager()
    {
        ModelCreationRoundExecutor_->Stop();
        LatenciesMeasuringExecutor_->Stop();
    }

    void RegisterRead(IIOEngine::TReadRequest request, EWorkloadCategory category, TDuration requestTime)
    {
        Invoker_->Invoke(BIND(&TWorkloadModelManager::DoRegisterRead,
            MakeStrong(this),
            std::move(request),
            category,
            requestTime));
    }

    void RegisterWrite(IIOEngine::TWriteRequest request, EWorkloadCategory category, TDuration requestTime)
    {
        Invoker_->Invoke(BIND(&TWorkloadModelManager::DoRegisterWrite,
            MakeStrong(this),
            std::move(request),
            category,
            requestTime));
    }

    DEFINE_SIGNAL(void(const TRequestSizes&), RequestSizesSignal);
    DEFINE_SIGNAL(void(const TRequestLatencies&), RequestLatenciesSignal);

private:
    const TString LocationId_;
    const NLogging::TLogger Logger;

    const TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;

    const TPeriodicExecutorPtr ModelCreationRoundExecutor_;
    const TPeriodicExecutorPtr LatenciesMeasuringExecutor_;

    TRequestSizes RequestSizes_;
    TRequestLatencies RequestLatencies_;

    void DoRegisterRead(
        const IIOEngine::TReadRequest& request,
        EWorkloadCategory category, TDuration requestTime)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        RequestSizes_.Reads[category].RecordValue(request.Size);
        RequestLatencies_.Reads[category].RecordValue(requestTime.MilliSeconds());
    }

    void DoRegisterWrite(
        const IIOEngine::TWriteRequest& request,
        EWorkloadCategory category, TDuration requestTime)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        RequestSizes_.Writes[category].RecordValue(GetByteSize(request.Buffers));
        RequestLatencies_.Writes[category].RecordValue(requestTime.MilliSeconds());
    }

    void OnModelCreationRound()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TRequestSizes model;
        std::swap(RequestSizes_, model);

        YT_LOG_DEBUG("Observed IO request sizes (Reads: %v, Writes: %v)",
            model.Reads,
            model.Writes);

        RequestSizesSignal_.Fire(std::move(model));
    }

    void OnLatenciesReportingRound()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TRequestLatencies latencies;
        std::swap(latencies, RequestLatencies_);

        RequestLatenciesSignal_.Fire(std::move(latencies));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIOModelInterceptor
    : public IIOEngineWorkloadModel
{
public:
    TIOModelInterceptor(TString locationId, IIOEnginePtr underlying, NLogging::TLogger logger)
        : Underlying_(std::move(underlying))
        , Logger(std::move(logger))
        , ModelManager_(New<TWorkloadModelManager>(std::move(locationId), Logger))
    {
        ModelManager_->SubscribeRequestLatenciesSignal(
            BIND(&TIOModelInterceptor::OnUpdateRequestLatencies, MakeWeak(this)));
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId) override
    {
        NProfiling::TWallTimer requestTimer;
        auto future = Underlying_->Read(
            requests,
            category,
            tagCookie,
            sessionId);

        future.Subscribe(BIND([=, this_ = MakeStrong(this)] (const NYT::TErrorOr<TReadResponse>&) {
            for (const auto& request : requests) {
                ModelManager_->RegisterRead(request, category, requestTimer.GetElapsedTime());
            }
        }));

        return future;
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        NProfiling::TWallTimer requestTimer;

        auto future = Underlying_->Write(request, category, sessionId);

        future.Subscribe(BIND([=, this_ = MakeStrong(this)] (const NYT::TErrorOr<void>&) {
            ModelManager_->RegisterWrite(request, category, requestTimer.GetElapsedTime());
        }));

        return future;
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category) override
    {
        return Underlying_->FlushFile(std::move(request), category);
    }

    TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        return Underlying_->FlushFileRange(std::move(request), category, sessionId);
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        EWorkloadCategory category) override
    {
        return Underlying_->FlushDirectory(std::move(request), category);
    }

    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        EWorkloadCategory category) override
    {
        return Underlying_->Open(std::move(request), category);
    }

    TFuture<void> Close(
        TCloseRequest request,
        EWorkloadCategory category) override
    {
        return Underlying_->Close(std::move(request), category);
    }

    TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory category) override
    {
        return Underlying_->Allocate(std::move(request), category);
    }

    bool IsSick() const override
    {
        return Underlying_->IsSick();
    }

    const IInvokerPtr& GetAuxPoolInvoker() override
    {
        return Underlying_->GetAuxPoolInvoker();
    }

    std::optional<TRequestSizes> GetRequestSizes() override
    {
        // TODO(capone212): implement and use.
        return {};
    }

    std::optional<TRequestLatencies> GetRequestLatencies() override
    {
        auto guard = Guard(StatsLock_);
        return LatencyStats_;
    }

    i64 GetTotalReadBytes() const override
    {
        return Underlying_->GetTotalReadBytes();
    }

    i64 GetTotalWrittenBytes() const override
    {
        return Underlying_->GetTotalWrittenBytes();
    }

    void Reconfigure(const NYTree::INodePtr& config) override
    {
        Underlying_->Reconfigure(config);
    }

private:
    const IIOEnginePtr Underlying_;
    const NLogging::TLogger Logger;

    TIntrusivePtr<TWorkloadModelManager> ModelManager_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StatsLock_);
    std::optional<TRequestLatencies> LatencyStats_;

    void OnUpdateRequestLatencies(const TRequestLatencies& latencies) {
        auto guard = Guard(StatsLock_);
        LatencyStats_ = latencies;
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOEngineWorkloadModelPtr CreateIOModelInterceptor(
    TString locationId,
    IIOEnginePtr underlying,
    NLogging::TLogger logger)
{
    return New<TIOModelInterceptor>(std::move(locationId), std::move(underlying), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
