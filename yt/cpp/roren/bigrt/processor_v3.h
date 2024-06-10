#pragma once

#include "processor_v2.h"  // TSharedRorenProcessorState
#include "parse_graph_v3.h"
#include "prepare_data_processor_v3.h"
#include "process_prepared_data_processor_v3.h"
#include <yt/cpp/roren/library/cpu_account_invoker/avg_window.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TBigRtProcessorV3
    : public NBigRT::TStatelessShardProcessor
{
private:
    using TBase = NBigRT::TStatelessShardProcessor;
public:
    using TMessageBatch = NBigRT::TMessageBatch;
    using TRowMeta = TBase::TRowMeta;
    using TRowWithMeta = TBase::TRowWithMeta;

    TBigRtProcessorV3(
        TBase::TConstructionArgs args,
        int fibers,
        NYT::IInvokerPtr invoker,
        std::shared_ptr<TTimers> timers,
        TParsedPipelineV3& parsedPipeline,
        const TString& inputTag,
        const NYT::NProfiling::TProfiler& profiler,
        TSharedRorenProcessorStatePtr sharedState,
        std::optional<NBigRT::TThrottlerQuota> throttlerQuota = std::nullopt);
    ~TBigRtProcessorV3() override;

private:
    TPreparedData PrepareData(TMessageBatch& data) override;
    void ProcessPreparedData(const TString& dataSource, TPreparedData& preparedData) override;
    NYT::TFuture<TPrepareForAsyncWriteResult> FinishEpoch(TCommitContext& commitContext) override final;
    NBigRT::TConsumingSystem::TVCpu GetEstimatedVCpuMetrics() const override;

    TPreparedData PrepareTimers(TMessageBatch& data);
    TPreparedData PrepareInputData(TMessageBatch& data);

    void ThrottleIfNecessary();

private:
    bool FatalError_ = false;
    bool EpochStarted_ = false;
    std::shared_ptr<TTimers> Timers_;
    TSharedRorenProcessorStatePtr SharedState_;
    std::optional<NBigRT::TAmortizedThrottler> Throttler_;

    TBigRtExecutionContextArgs RorenContextArgsTemplate_;
    TPrepareDataProcessorV3 PrepareDataProcessor_;
    TProcessPreparedDataProcessorV3 ProcessPreparedDataProcessor_;

    NYT::NConcurrency::TPeriodicExecutorPtr CpuMetricsCollector_;
    TAvgWindow<NBigRT::TConsumingSystem::TVCpu, 30> CpuMetricsWindow_;

    //TODO: remove
    struct TMetrics {
        NYT::NProfiling::TProfiler Profiler;
        NYT::NProfiling::TTimeCounter ThrottleTotalTime = Profiler.TimeCounter(".throttle_total_time");
    };
    TMetrics Metrics_;

private:
    void Process(TString dataSource, TMessageBatch data) override;
    void CollectCpuMetrics();
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
