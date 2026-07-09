#include "buffer_state_manager.h"
#include "job_spec.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/counter.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow::NWorker {

using namespace NThreading;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr i64 SideInflation(bool isInput)
{
    return isInput ? InputMessageExtraTechnicalMemoryCost : OutputMessageExtraTechnicalMemoryCost;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TBufferStateManager
    : public IBufferStateManager
{
private:
    struct TStreamData
    {
        TStreamLimitUsageStatePtr LimitUsageState;
        i64 InflationPerMessage = 0;
        TSimpleEmaCounter PushDemand;

        TStreamUsage LastReportedUsage;

        NProfiling::TGauge LimitBytesGauge;
        NProfiling::TGauge SizeGauge;
        NProfiling::TGauge PendingInflatedBytesGauge;
        NProfiling::TGauge MeasuredDemandGauge;
        NProfiling::TGauge BaselineDemandGauge;
        NProfiling::TCounter RegisteredBytesCounter;
        NProfiling::TCounter UnregisteredBytesCounter;
        NProfiling::TGauge MessageCountGauge;
        NProfiling::TCounter RegisteredCountCounter;
        NProfiling::TCounter UnregisteredCountCounter;
    };

    struct TSideState
    {
        THashMap<TStreamId, TStreamData> Streams;
    };

    struct TJobState
    {
        TComputationId ComputationId;
        TSideState Input;
        TSideState Output;
    };

public:
    TBufferStateManager(
        IInvokerPtr invoker,
        IJobDirectoryPtr jobDirectory,
        TDynamicBufferStateManagerSpecPtr dynamicSpec,
        std::function<TInstant()> timeProvider)
        : JobDirectory_(std::move(jobDirectory))
        , TimeProvider_(std::move(timeProvider))
        , DynamicSpec_(std::move(dynamicSpec))
        , BufferManagementExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TBufferStateManager::ManageBuffers, MakeWeak(this)),
            DynamicSpec_->ManagePeriod))
    { }

    void Initialize()
    {
        BufferManagementExecutor_->Start();
    }

    TJobStreamLimitUsageStates RegisterJob(TJobId jobId, const TJobSpecPtr& jobSpec) override
    {
        auto guard = Guard(Lock_);

        auto demandWindow = DynamicSpec_->DemandWindow;
        const auto& computationId = jobSpec->Partition->ComputationId;
        auto computationProfiler = WorkerProfiler()
            .WithTag("computation_id", computationId.Underlying())
            .WithPrefix("/buffer_state/computations");
        auto seedSide = [&] (const auto& specStreamIds, i64 guarantee, i64 inflation, TStringBuf sidePrefix) {
            TSideState side;
            for (const auto& streamId : specStreamIds) {
                auto& streamData = side.Streams[streamId];
                streamData.LimitUsageState = New<TStreamLimitUsageState>(inflation);
                streamData.LimitUsageState->SetLimitBytes(guarantee);
                streamData.InflationPerMessage = inflation;
                streamData.PushDemand.SetWindow(demandWindow);
                auto streamProfiler = computationProfiler
                    .WithPrefix(sidePrefix)
                    .WithTag("stream_id", streamId.Underlying());
                // TODO(pechatnov): rename /size -> /used_inflated_bytes, /limit -> /limit_inflated_bytes,
                // /pending_size -> /pending_inflated_bytes.
                streamData.LimitBytesGauge = streamProfiler.Gauge("/limit");
                streamData.SizeGauge = streamProfiler.Gauge("/size");
                streamData.PendingInflatedBytesGauge = streamProfiler.WithDefaultDisabled().Gauge("/pending_size");
                streamData.MeasuredDemandGauge = streamProfiler.Gauge("/measured_demand");
                streamData.BaselineDemandGauge = streamProfiler.Gauge("/baseline_demand");
                streamData.RegisteredBytesCounter = streamProfiler.Counter("/registered_bytes");
                streamData.UnregisteredBytesCounter = streamProfiler.Counter("/unregistered_bytes");
                streamData.MessageCountGauge = streamProfiler.Gauge("/message_count");
                streamData.RegisteredCountCounter = streamProfiler.Counter("/registered_count");
                streamData.UnregisteredCountCounter = streamProfiler.Counter("/unregistered_count");
            }
            return side;
        };
        auto jobState = TJobState{
            .ComputationId = computationId,
            .Input = seedSide(jobSpec->ComputationSpec->InputStreamIds, DynamicSpec_->InputBuffer->JobGuarantee, SideInflation(/*isInput*/ true), "/input"),
            .Output = seedSide(jobSpec->ComputationSpec->OutputStreamIds, DynamicSpec_->OutputBuffer->JobGuarantee, SideInflation(/*isInput*/ false), "/output"),
        };
        TJobStreamLimitUsageStates states;
        auto exportStates = [] (const TSideState& side) {
            TStreamLimitUsageStateMap map;
            map.reserve(side.Streams.size());
            for (const auto& [streamId, streamData] : side.Streams) {
                map.emplace(streamId, streamData.LimitUsageState);
            }
            return map;
        };
        states.Input = exportStates(jobState.Input);
        states.Output = exportStates(jobState.Output);
        EmplaceOrCrash(JobIdToState_, jobId, std::move(jobState));
        return states;
    }

    void RemoveJob(TJobId jobId) override
    {
        auto guard = Guard(Lock_);

        EraseOrCrash(JobIdToState_, jobId);
    }

    void Reconfigure(TDynamicBufferStateManagerSpecPtr dynamicSpec) override
    {
        auto guard = Guard(Lock_);

        if (AreNodesEqual(ConvertToNode(DynamicSpec_), ConvertToNode(dynamicSpec))) {
            return;
        }

        std::swap(DynamicSpec_, dynamicSpec);
        BufferManagementExecutor_->SetPeriod(DynamicSpec_->ManagePeriod);

        auto demandWindow = DynamicSpec_->DemandWindow;
        for (auto& [jobId, jobState] : JobIdToState_) {
            for (auto& [streamId, streamData] : jobState.Input.Streams) {
                streamData.PushDemand.SetWindow(demandWindow);
            }
            for (auto& [streamId, streamData] : jobState.Output.Streams) {
                streamData.PushDemand.SetWindow(demandWindow);
            }
        }
    }

    void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) override
    {
        auto guard = Guard(Lock_);
        MessageTransferingInfo_ = std::move(messageTransferingInfo);
    }

    void ManageBuffers() override
    {
        auto guard = Guard(Lock_);

        if (JobIdToState_.empty()) {
            return;
        }
        ApplyFairShareStrategyToBuffers(guard);
    }

private:
    std::optional<double> GetOverrideLimit(const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& parameters, const TComputationId& computationId, const TStreamId& streamId)
    {
        if (auto it = parameters->JobOverrides.find(computationId); it != parameters->JobOverrides.end()) {
            if (auto streamIt = it->second.find(streamId); streamIt != it->second.end()) {
                return streamIt->second;
            }
        }
        return std::nullopt;
    }

    double ComputeLimit(const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& parameters, double demand, double totalDemand, std::optional<double> overrideLimit)
    {
        if (overrideLimit.has_value()) {
            return *overrideLimit;
        }
        return std::min<double>(
            std::min<double>(
                std::max(parameters->JobGuarantee, parameters->JobLimit),
                parameters->JobGuarantee + parameters->MaxDuration.SecondsFloat() * demand),
            parameters->JobGuarantee + parameters->FairSharePool * demand / totalDemand);
    }

    void ApplyFairShareStrategyToBuffers(TGuard<TSpinLock>& /*guard*/)
    {
        auto now = TimeProvider_();
        auto baselineDemand = [&] (const TStreamId& streamId, const TComputationId& computationId, i64 inflationPerMessage) {
            if (!MessageTransferingInfo_) {
                return 0.0;
            }
            const auto& speeds = MessageTransferingInfo_->SpeedStatistics.StreamSpeed1d;
            auto it = speeds.find(streamId);
            if (it == speeds.end()) {
                return 0.0;
            }
            i64 partitionCount = JobDirectory_->GetPartitionCount(computationId);
            if (partitionCount <= 0) {
                return 0.0;
            }
            return (it->second.ProcessedBytesPerSecond + it->second.ProcessedMessagesPerSecond * static_cast<double>(inflationPerMessage)) / partitionCount;
        };

        struct TStreamPlan
        {
            TStreamData* StreamData = nullptr;
            const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr* Spec = nullptr;
            double Demand = 0;
            std::optional<double> OverrideLimit;
            double* TotalDemand = nullptr;
        };

        std::vector<TStreamPlan> plans;
        plans.reserve(JobIdToState_.size() * 4);

        double totalInputDemand = 1;
        double totalOutputDemand = 1;
        auto addPlan = [&] (TSideState& side,
            const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec,
            const TComputationId& computationId,
            i64 inflationPerMessage,
            double* totalDemand) {
            for (auto& [streamId, streamData] : side.Streams) {
                auto overrideLimit = GetOverrideLimit(spec, computationId, streamId);
                auto usage = streamData.LimitUsageState->Read();
                i64 inflatedTotal = usage.CumulativeByteOut + usage.CumulativeCountOut * inflationPerMessage;
                streamData.PushDemand.Update(static_cast<double>(inflatedTotal), now);
                double measured = streamData.PushDemand.GetRate(now).value_or(0.0);
                double baseline = baselineDemand(streamId, computationId, inflationPerMessage);
                double demand = std::max(measured, baseline);
                if (!overrideLimit.has_value()) {
                    *totalDemand += demand;
                }
                i64 inflightCount = usage.CumulativeCountIn - usage.CumulativeCountOut;
                i64 inflatedSize = usage.GetInflatedInflightBytes(inflationPerMessage);
                streamData.RegisteredBytesCounter.Increment(usage.CumulativeByteIn - streamData.LastReportedUsage.CumulativeByteIn);
                streamData.UnregisteredBytesCounter.Increment(usage.CumulativeByteOut - streamData.LastReportedUsage.CumulativeByteOut);
                streamData.RegisteredCountCounter.Increment(usage.CumulativeCountIn - streamData.LastReportedUsage.CumulativeCountIn);
                streamData.UnregisteredCountCounter.Increment(usage.CumulativeCountOut - streamData.LastReportedUsage.CumulativeCountOut);
                streamData.LastReportedUsage = usage;
                streamData.SizeGauge.Update(inflatedSize);
                streamData.MessageCountGauge.Update(inflightCount);
                streamData.PendingInflatedBytesGauge.Update(usage.PendingInflatedBytes);
                streamData.MeasuredDemandGauge.Update(measured);
                streamData.BaselineDemandGauge.Update(baseline);
                plans.push_back({&streamData, &spec, demand, overrideLimit, totalDemand});
            }
        };
        for (auto& [jobId, jobState] : JobIdToState_) {
            addPlan(jobState.Input, DynamicSpec_->InputBuffer, jobState.ComputationId, SideInflation(/*isInput*/ true), &totalInputDemand);
            addPlan(jobState.Output, DynamicSpec_->OutputBuffer, jobState.ComputationId, SideInflation(/*isInput*/ false), &totalOutputDemand);
        }

        for (const auto& entry : plans) {
            i64 newLimit = static_cast<i64>(ComputeLimit(*entry.Spec, entry.Demand, *entry.TotalDemand, entry.OverrideLimit));
            entry.StreamData->LimitUsageState->SetLimitBytes(newLimit);
            entry.StreamData->LimitBytesGauge.Update(newLimit);
        }
    }

private:
    const IJobDirectoryPtr JobDirectory_;
    const std::function<TInstant()> TimeProvider_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TDynamicBufferStateManagerSpecPtr DynamicSpec_;
    const TPeriodicExecutorPtr BufferManagementExecutor_;

    THashMap<TJobId, TJobState> JobIdToState_;
    TMessageTransferingInfoPtr MessageTransferingInfo_;
};

////////////////////////////////////////////////////////////////////////////////

IBufferStateManagerPtr CreateBufferStateManager(
    IInvokerPtr invoker,
    IJobDirectoryPtr jobDirectory,
    TDynamicBufferStateManagerSpecPtr dynamicSpec,
    std::function<TInstant()> timeProvider)
{
    auto manager = New<TBufferStateManager>(
        std::move(invoker),
        std::move(jobDirectory),
        std::move(dynamicSpec),
        std::move(timeProvider));
    manager->Initialize();
    return manager;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
