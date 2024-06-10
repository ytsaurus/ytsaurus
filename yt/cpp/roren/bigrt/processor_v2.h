#pragma once

#include "bigrt_execution_context.h"
#include "table_poller.h"
#include "vcpu_metrics.h"

#include <yt/cpp/roren/interface/executor.h>
#include <yt/cpp/roren/library/logger/logger.h>

#include <bigrt/lib/consuming_system/consuming_system.h>
#include <bigrt/lib/processing/shard_processor/fallback/processor.h>
#include <bigrt/lib/processing/shard_processor/stateless/processor.h>
#include <bigrt/lib/utility/profiling/fiber_vcpu_time.h>
#include <bigrt/lib/writer/swift/factory.h>
#include <bigrt/lib/writer/yt_queue/factory.h>
#include <bigrt/lib/utility/profiling/vcpu_factor/vcpu_factor.h>
#include <bigrt/lib/utility/inflight/inflight.h>
#include <bigrt/lib/utility/logging/logging.h>  // MainLogger
#include <bigrt/lib/utility/profiling/safe_stats_over_yt.h>
#include <bigrt/lib/utility/throttler/throttler.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

// The same (unix) process might run several TRorenProcessors for different shards.
// This class incapsulates state that is shared between such TRorenProcessors.
struct TSharedRorenProcessorState: public TThrRefBase
{
    NYT::NConcurrency::IThreadPoolPtr BackgroundThreadPool_;
    NYT::IInvokerPtr BackgroundInvoker_;
    ::TIntrusivePtr<TTablePoller> TablePoller_;

    explicit TSharedRorenProcessorState(const NYT::TCancelableContextPtr& cancelableContext);
};
using TSharedRorenProcessorStatePtr = ::TIntrusivePtr<TSharedRorenProcessorState>;

////////////////////////////////////////////////////////////////////////////////

class TRorenProcessorV2
    : public NBigRT::TStatelessShardProcessor
{
protected:
    using TBase = NBigRT::TStatelessShardProcessor;

public:
    TRorenProcessorV2(
        TBase::TConstructionArgs args,
        std::shared_ptr<THashMap<ui64, std::shared_ptr<NPrivate::TTimers>>> timers,
        const TPipeline& pipeline,
        size_t executionBlockNumber,
        const NYT::NProfiling::TProfiler& profiler,
        TSharedRorenProcessorStatePtr sharedState,
        NYT::IInvokerPtr userInvoker,
        bool waitProcessStarted,
        std::optional<NBigRT::TThrottlerQuota> throttlerQuota = std::nullopt);

    ~TRorenProcessorV2() override;
    void Process(TString dataSource, NBigRT::TMessageBatch messageBatch) override;
    void ThrottleIfNecessary();
    void ProcessInput(TString /*inputAlias*/, NBigRT::TMessageBatch messageBatch);
    void ProcessTimers(TString dataSource, NBigRT::TMessageBatch messageBatch);
    NYT::TFuture<TPrepareForAsyncWriteResult> FinishEpoch(TCommitContext& commitContext) override final;

    NBigRT::TConsumingSystem::TVCpu GetEstimatedVCpuMetrics() const override;

private:
    static void RunUserComputation(
        const NRoren::TBigRtExecutorPoolPtr& pool,
        const IBigRtExecutionContextPtr& context, const NBigRT::TMessageBatch& messageBatch);

protected:
    struct TEpochContext
    {
        IBigRtExecutionContextPtr ExecutionContext;
        NYT::TFuture<void> ExecutionBlockFinished;
    };

    std::shared_ptr<NPrivate::TTimers> Timers_;
    NPrivate::IExecutionBlockPtr ExecutionBlock_;
    std::optional<TEpochContext> EpochContext_;

    NYT::IInvokerPtr UserInvoker_;
    bool WaitProcessStarted_;

    TSharedRorenProcessorStatePtr SharedState_;
    TBigRtExecutionContextArgs RorenContextArgsTemplate_;
    THashMap<TString, NPrivate::IExecutionBlockPtr> TimersCallbacks_;

    std::optional<NBigRT::TAmortizedThrottler> Throttler_;
    struct TMetrics {
        NYT::NProfiling::TProfiler Profiler;
        NYT::NProfiling::TTimeCounter ThrottleTotalTime = Profiler.TimeCounter(".throttle_total_time");
    };
    TMetrics Metrics_;
    TVCpuMetricsPtr VCpuMetrics_;
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren
