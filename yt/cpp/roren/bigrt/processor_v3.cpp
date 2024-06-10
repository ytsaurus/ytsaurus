#include "processor_v3.h"
#include "bigrt_executor.h"
#include "parse_graph.h" //TODO: remove
#include "supplier.h"

#include <ads/bsyeti/libs/backtrace/backtrace.h>

#include <bigrt/lib/consuming_system/consuming_system.h>
#include <bigrt/lib/processing/shard_processor/fallback/processor.h>
#include <bigrt/lib/processing/shard_processor/stateless/processor.h>
#include <bigrt/lib/writer/swift/factory.h>
#include <bigrt/lib/writer/yt_queue/factory.h>
#include <bigrt/lib/utility/profiling/vcpu_factor/vcpu_factor.h>
#include <bigrt/lib/utility/inflight/inflight.h>
#include <bigrt/lib/utility/logging/logging.h>  // MainLogger
#include <bigrt/lib/utility/profiling/safe_stats_over_yt.h>
#include <bigrt/lib/utility/throttler/throttler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NRoren::NPrivate {

// TODO: move to SyncExecutionContext
NPrivate::TStateManagerRegistryPtr MakeStateManagerRegistry(const ui64 shard, const NSFStats::TSolomonContext& sensorsContext, const THashMap<TString, TCreateBaseStateManagerFunction>& stateManagerFactoryFunctions)
{
    if (stateManagerFactoryFunctions.empty()) {
        return {};
    }

    NPrivate::TStateManagerRegistryPtr stateManagerRegistry = MakeIntrusive<NPrivate::TStateManagerRegistry>();
    for (const auto& [id, factory] : stateManagerFactoryFunctions) {
        auto stateManager = factory(shard, sensorsContext);
        stateManagerRegistry->Add(id, std::move(stateManager));
    }
    return stateManagerRegistry;
}

////////////////////////////////////////////////////////////////////////////////

TBigRtProcessorV3::TBigRtProcessorV3(
    TBase::TConstructionArgs args,
    int fibers,
    NYT::IInvokerPtr invoker,
    std::shared_ptr<NPrivate::TTimers> timers,
    NPrivate::TParsedPipelineV3& parsedPipeline,
    const TString& inputTag,
    const NYT::NProfiling::TProfiler& profiler,
    TSharedRorenProcessorStatePtr sharedState,
    std::optional<NBigRT::TThrottlerQuota> throttlerQuota
)
    : TBase(args)
    , Timers_(timers)
    , SharedState_(std::move(sharedState))
    , Throttler_(throttlerQuota ? std::optional(NBigRT::TAmortizedThrottler(*throttlerQuota, ShardsCount)) : std::nullopt)
    , PrepareDataProcessor_(invoker, fibers, inputTag, parsedPipeline, GetStopToken().GetCancelableContext())
    , ProcessPreparedDataProcessor_(invoker, fibers, inputTag, parsedPipeline, GetStopToken().GetCancelableContext())
    , CpuMetricsCollector_(NYT::New<NYT::NConcurrency::TPeriodicExecutor>(invoker, BIND([this]() { this->CollectCpuMetrics(); }), TDuration::Seconds(1)))
    , Metrics_{profiler.WithTags(ProfilerTags)}
{
    RorenContextArgsTemplate_.Shard = Shard;
    RorenContextArgsTemplate_.InputTag = inputTag;
    RorenContextArgsTemplate_.Timers = Timers_;
    RorenContextArgsTemplate_.TransactionKeeper = TransactionKeeper;
    RorenContextArgsTemplate_.TablePoller = SharedState_->TablePoller_;
    RorenContextArgsTemplate_.Profiler = profiler.WithTags(ProfilerTags);
    RorenContextArgsTemplate_.MainCluster = Cluster;
    RorenContextArgsTemplate_.StateManagerRegistry = MakeStateManagerRegistry(Shard, SensorsContext, ProcessPreparedDataProcessor_.GetStateManagerFunctions());
    RorenContextArgsTemplate_.ThrottlerReport = std::make_shared<std::atomic<ui64>>(0);

    if (Timers_) {
        Timers_->ReInit();
    }

    CpuMetricsCollector_->Start();
}

void TBigRtProcessorV3::Process(TString dataSource, TMessageBatch data)
{
    Y_UNUSED(dataSource, data);
    Y_ABORT("Process() is deprecated. Use PrepareData() & ProcessPreparedData() instead");
}

TBigRtProcessorV3::TPreparedData TBigRtProcessorV3::PrepareData(TMessageBatch& data)
{
    if (data.GetSingleDataSource() == NPrivate::SYSTEM_TIMERS_SUPPLIER_NAME) {
        return PrepareTimers(data);
    } else {
        return PrepareInputData(data);
    }
}

TBigRtProcessorV3::TPreparedData TBigRtProcessorV3::PrepareTimers(TMessageBatch& data)
{
    Y_ABORT_IF(data.GetSingleDataSource() != NPrivate::SYSTEM_TIMERS_SUPPLIER_NAME);
    TPreparedData result;
    for (auto& message: data.Messages) {
        NPrivate::TTimerProto timer;
        Y_ABORT_IF(!timer.ParseFromString(message.UnpackedData()));
        result.Data.push_back({});
        result.Data.back().emplace_back(std::move(timer));
    }
    return result;
}

TBigRtProcessorV3::TPreparedData TBigRtProcessorV3::PrepareInputData(TMessageBatch& messageBatch)
{
    return PrepareDataProcessor_.Process(RorenContextArgsTemplate_, std::move(messageBatch));
}

void TBigRtProcessorV3::ProcessPreparedData(const TString& dataSource, TPreparedData& preparedData)
try {
    Y_ABORT_IF(FatalError_);
    if (!EpochStarted_) {
        RorenContextArgsTemplate_.Writer = DynamicPointerCast<NPrivate::TCompositeBigRtWriter>(GetWriter());
        Y_ABORT_IF(RorenContextArgsTemplate_.Writer == nullptr);

        ProcessPreparedDataProcessor_.StartEpoch(RorenContextArgsTemplate_);
        EpochStarted_ = true;
    }

    ProcessPreparedDataProcessor_.ProcessPreparedData(dataSource, std::move(preparedData.Data));
    ThrottleIfNecessary();
} catch (...) {
    FatalError_ = true;
    throw;
}

NYT::TFuture<TBigRtProcessorV3::TPrepareForAsyncWriteResult> TBigRtProcessorV3::FinishEpoch(TCommitContext& commitContext)
try {
    Y_ABORT_IF(FatalError_);
    ThrottleIfNecessary();
    auto baseFinishFuture = TStatelessShardProcessor::FinishEpoch(commitContext);
    if (!EpochStarted_) {
        return baseFinishFuture;
    }

    auto finishFuture = ProcessPreparedDataProcessor_.FinishEpoch(std::move(baseFinishFuture));
    finishFuture = finishFuture.Apply(BIND(
        [this] (TPrepareForAsyncWriteResult prepareResult) {
            return TPrepareForAsyncWriteResult{
                .AsyncWriter = std::move(prepareResult.AsyncWriter),
                .OnCommitCallback = [
                    this,
                    onCommitCallback = std::move(prepareResult.OnCommitCallback)
                    ] (const TCommitContext& context) {
                        if (onCommitCallback) {
                            onCommitCallback(context);
                        }
                        if (context.Success && this->Timers_) {
                            this->Timers_->OnCommit();
                        }
                    },
                };
        }
    ));
    EpochStarted_ = false;
    return finishFuture;
} catch (...) {
    FatalError_ = true;
    throw;
}

TBigRtProcessorV3::~TBigRtProcessorV3()
{
    Y_ABORT_IF(!GetStopToken().IsSet() && !FatalError_ && EpochStarted_);
}

void TBigRtProcessorV3::CollectCpuMetrics()
{
    NBigRT::TConsumingSystem::TVCpu currentMetrics;
    currentMetrics.Prepare = PrepareDataProcessor_.GetCpuTime();
    currentMetrics.Process = ProcessPreparedDataProcessor_.GetCpuTime();
    currentMetrics.Total = currentMetrics.Load + currentMetrics.Prepare + currentMetrics.Process + currentMetrics.Commit + currentMetrics.Compress + currentMetrics.PrepareForAsyncWrite;
    CpuMetricsWindow_.Push(std::move(currentMetrics));
}

NBigRT::TConsumingSystem::TVCpu TBigRtProcessorV3::GetEstimatedVCpuMetrics() const
{
    auto cpuMetrics = CpuMetricsWindow_.GetAvg();
    cpuMetrics *= NBigRT::GetVCpuFactor();
    return cpuMetrics;
}

void TBigRtProcessorV3::ThrottleIfNecessary()
{
    if (!Throttler_) {
        return;
    }
    Throttler_->ReportProcessed(RorenContextArgsTemplate_.ThrottlerReport->exchange(0));
    TDuration sleepTime = Min(Throttler_->GetThrottleTime(), TDuration::Seconds(10));
    if (!sleepTime) {
        return;
    }
    Metrics_.ThrottleTotalTime.Add(sleepTime);
    GetStopToken().Wait(sleepTime);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
