#include "buffer_state_manager.h"
#include "job_spec.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/buffers/epoch_cycle_tracker.h>
#include <yt/yt/flow/library/cpp/buffers/max_rate_estimator.h>
#include <yt/yt/flow/library/cpp/buffers/offered_rate_estimator.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/counter.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/new.h>

#include <cmath>

namespace NYT::NFlow::NWorker {

using namespace NThreading;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr i64 SideInflation(bool isInput)
{
    return isInput ? InputMessageExtraTechnicalMemoryCost : OutputMessageExtraTechnicalMemoryCost;
}

constexpr double MaxSaneIORatio = 1000.0;

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
        //! Input side only: fed by the input buffer, window managed here.
        TOfferedRateEstimatorPtr OfferedRateEstimator;
        TSimpleEmaCounter PushDemand;
        // Raw (non-inflated) bytes rate: drained bytes for input streams, produced
        // bytes for output streams; feeds the shadow I/O ratio.
        TSimpleEmaCounter RawBytesRate;
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
        NProfiling::TGauge UtilizationGauge;
        NProfiling::TGauge IORatioGauge;
    };

    struct TSideState
    {
        THashMap<TStreamId, TStreamData> Streams;
    };

    //! V2 issuance state of one stream-side limit entity.
    struct TIssuanceState
    {
        //! Windowed-max drain rate; the bucket adapts to the job's epoch cycle.
        TMaxRateEstimator MaxDrainRate;
        //! Headroom above the used peak; what probes beyond measured demand.
        i64 Headroom = 0;
        i64 IssuedLimit = -1;
        //! Demand seen at the last headroom growth; growth requires demand to keep
        //! rising (STARTUP-exit rule of TCP BBR, Bottleneck Bandwidth and
        //! Round-trip propagation time), so a saturated bottleneck stops probing.
        double LastProbeDemand = 0;
        //! Warm-start demand floor: keeps the seeded sizing (demandFloor and drain cap)
        //! effective until the max-rate estimator produces its first own value —
        //! otherwise a small instant estimate (e.g. the offered rate of an old
        //! backlog) would collapse the drain cap right after the seed.
        double SeedDemand = 0;
        //! Slow EMA of the planned demand, fed to the persisted warmup: the
        //! instantaneous demand is spiky, and every >25% wiggle would turn an
        //! otherwise-empty epoch transaction into a state write.
        double SmoothedDemand = 0;
    };

    struct TJobState
    {
        TComputationId ComputationId;
        TSideState Input;
        TSideState Output;
        THashMap<TStreamId, TIssuanceState> InputIssuance;
        THashMap<TStreamId, TIssuanceState> OutputIssuance;
        TEpochCycleTrackerPtr InputEpochCycleTracker;
        NProfiling::TGauge EpochCycleGauge;
        //! Slow EMA of the median cycle for the persisted warmup (see
        //! TIssuanceState::SmoothedDemand for the rationale).
        double SmoothedEpochCycleSeconds = 0;
        bool SeededFromWarmup = false;
    };

    struct TStreamPlan
    {
        TStreamData* StreamData = nullptr;
        TIssuanceState* Issuance = nullptr;
        const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr* Spec = nullptr;
        double Demand = 0;
        std::optional<double> OverrideLimit;
        double* TotalDemand = nullptr;
        bool IsInput = false;
        bool HasBacklog = false;
        double PeakInflightBytes = 0;
        double UsedInflatedBytes = 0;
        double PendingInflatedBytes = 0;
        double EpochCycleSeconds = 0;
    };

public:
    TBufferStateManager(
        IInvokerPtr invoker,
        IJobDirectoryPtr jobDirectory,
        TDynamicBufferStateManagerSpecPtr dynamicSpec,
        std::function<TInstant()> timeProvider,
        std::vector<TWorkerGroupId> workerGroups)
        : JobDirectory_(std::move(jobDirectory))
        , TimeProvider_(std::move(timeProvider))
        , WorkerGroups_(std::move(workerGroups))
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
        auto seedSide = [&] (const auto& specStreamIds, i64 guarantee, bool isInput, TStringBuf sidePrefix) {
            TSideState side;
            i64 inflation = SideInflation(isInput);
            for (const auto& streamId : specStreamIds) {
                auto& streamData = side.Streams[streamId];
                streamData.LimitUsageState = New<TStreamLimitUsageState>(inflation);
                streamData.LimitUsageState->SetLimitBytes(guarantee);
                streamData.InflationPerMessage = inflation;
                if (isInput) {
                    streamData.OfferedRateEstimator = New<TOfferedRateEstimator>(demandWindow);
                }
                streamData.PushDemand.SetWindow(demandWindow);
                streamData.RawBytesRate.SetWindow(demandWindow);
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
                streamData.UtilizationGauge = streamProfiler.Gauge("/utilization");
                if (!isInput) {
                    streamData.IORatioGauge = streamProfiler.Gauge("/io_ratio");
                }
            }
            return side;
        };
        auto jobState = TJobState{
            .ComputationId = computationId,
            .Input = seedSide(jobSpec->ComputationSpec->InputStreamIds, DynamicSpec_->InputBuffer->JobGuarantee, /*isInput*/ true, "/input"),
            .Output = seedSide(jobSpec->ComputationSpec->OutputStreamIds, DynamicSpec_->OutputBuffer->JobGuarantee, /*isInput*/ false, "/output"),
            .InputEpochCycleTracker = New<TEpochCycleTracker>(),
            .EpochCycleGauge = computationProfiler.Gauge("/input_epoch_cycle_median"),
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
        states.InputEpochCycleTracker = jobState.InputEpochCycleTracker;
        for (const auto& [streamId, streamData] : jobState.Input.Streams) {
            states.InputOfferedRateEstimators.emplace(streamId, streamData.OfferedRateEstimator);
        }

        if (DynamicSpec_->EnableV2) {
            for (const auto& streamId : jobSpec->ComputationSpec->InputStreamIds) {
                jobState.InputIssuance.try_emplace(streamId);
            }
            for (const auto& streamId : jobSpec->ComputationSpec->OutputStreamIds) {
                jobState.OutputIssuance.try_emplace(streamId);
            }
        }

        EmplaceOrCrash(JobIdToState_, jobId, std::move(jobState));
        return states;
    }

    bool IsV2Enabled() override
    {
        auto guard = Guard(Lock_);
        return DynamicSpec_->EnableV2;
    }

    TDuration GetWarmupRefreshPeriod() override
    {
        auto guard = Guard(Lock_);
        return DynamicSpec_->WarmupRefreshPeriod;
    }

    TPartitionBufferWarmup GetJobWarmup(TJobId jobId) override
    {
        auto guard = Guard(Lock_);

        TPartitionBufferWarmup warmup;
        auto it = JobIdToState_.find(jobId);
        if (it == JobIdToState_.end() || !DynamicSpec_->EnableV2) {
            return warmup;
        }
        auto& jobState = it->second;
        auto fillDemands = [] (const TSideState& side, THashMap<TStreamId, double>& demands) {
            for (const auto& [streamId, streamData] : side.Streams) {
                if (double demand = streamData.LimitUsageState->GetEstimatedInflatedSpeed(); demand > 0) {
                    demands[streamId] = demand;
                }
            }
        };
        fillDemands(jobState.Input, warmup.InputSpeeds);
        fillDemands(jobState.Output, warmup.OutputSpeeds);
        if (jobState.SmoothedEpochCycleSeconds > 0) {
            warmup.EpochCycleSeconds = jobState.SmoothedEpochCycleSeconds;
        }
        return warmup;
    }

    void SeedJob(TJobId jobId, const TPartitionBufferWarmup& bufferWarmup) override
    {
        auto guard = Guard(Lock_);

        auto it = JobIdToState_.find(jobId);
        if (it == JobIdToState_.end() || it->second.SeededFromWarmup) {
            return;
        }
        SeedFromWarmup(it->second, bufferWarmup);
    }

    //! Warm start: seed headroom (and the epoch cycle) from the persisted converged
    //! sizing of this partition (delivered with the partition spec), so the first
    //! manage tick issues the previous steady-state limits instead of ramping from
    //! the floor.
    void SeedFromWarmup(TJobState& jobState, const TPartitionBufferWarmup& warmup)
    {
        if (!DynamicSpec_->EnableV2 ||
            (warmup.InputSpeeds.empty() && warmup.OutputSpeeds.empty()))
        {
            return;
        }
        jobState.SeededFromWarmup = true;
        // Sanitize the persisted cycle: a hand-corrupted YSON could carry NaN or a
        // huge value that would poison the sizing math or overflow TDuration.
        const double epochCycleSeconds =
            std::isfinite(warmup.EpochCycleSeconds) && warmup.EpochCycleSeconds > 0
            ? std::min(warmup.EpochCycleSeconds, DynamicSpec_->InputBuffer->MaxDuration.SecondsFloat())
            : 0.0;
        const double epochSeconds = std::max(epochCycleSeconds, DynamicSpec_->ManagePeriod.SecondsFloat());
        if (epochCycleSeconds > 0) {
            jobState.InputEpochCycleTracker->RecordCycle(TDuration::Seconds(epochCycleSeconds));
            jobState.SmoothedEpochCycleSeconds = epochCycleSeconds;
        }
        auto seedIssuance = [&] (
            TIssuanceState& issuance,
            TSideState& sideState,
            const TStreamId& streamId,
            double demand,
            const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec) {
            // The guard also rejects NaN from a hand-corrupted persisted warmup.
            if (!(demand > 0) || !std::isfinite(demand)) {
                return;
            }
            const i64 jobLimit = std::max<i64>(spec->JobGuarantee, spec->JobLimit);
            issuance.Headroom = static_cast<i64>(std::min<double>(
                DynamicSpec_->V2GainEpochs * demand * std::min(epochSeconds, spec->MaxDuration.SecondsFloat()),
                static_cast<double>(jobLimit)));
            issuance.LastProbeDemand = demand;
            issuance.SeedDemand = demand;
            issuance.SmoothedDemand = demand;
            // Publish into the slot GetJobWarmup reads: a commit landing before
            // the first manage tick must not see an empty warmup and overwrite
            // the persisted one.
            if (auto* streamData = sideState.Streams.FindPtr(streamId)) {
                streamData->LimitUsageState->SetEstimatedInflatedSpeed(demand);
            }
        };
        for (auto& [streamId, issuance] : jobState.InputIssuance) {
            seedIssuance(issuance, jobState.Input, streamId, GetOrDefault(warmup.InputSpeeds, streamId, 0.0), DynamicSpec_->InputBuffer);
        }
        for (auto& [streamId, issuance] : jobState.OutputIssuance) {
            seedIssuance(issuance, jobState.Output, streamId, GetOrDefault(warmup.OutputSpeeds, streamId, 0.0), DynamicSpec_->OutputBuffer);
        }
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

        if (DynamicSpec_->EnableV2 != dynamicSpec->EnableV2) {
            // A stale IssuedLimit from a previous v2 period would defeat the
            // publication hysteresis after re-enabling; start the issuance fresh.
            for (auto& [jobId, jobState] : JobIdToState_) {
                jobState.InputIssuance.clear();
                jobState.OutputIssuance.clear();
            }
        }

        auto demandWindow = DynamicSpec_->DemandWindow;
        for (auto& [jobId, jobState] : JobIdToState_) {
            for (auto& [streamId, streamData] : jobState.Input.Streams) {
                streamData.OfferedRateEstimator->SetWindow(demandWindow);
                streamData.PushDemand.SetWindow(demandWindow);
                streamData.RawBytesRate.SetWindow(demandWindow);
            }
            for (auto& [streamId, streamData] : jobState.Output.Streams) {
                streamData.PushDemand.SetWindow(demandWindow);
                streamData.RawBytesRate.SetWindow(demandWindow);
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
        // The tick is not idempotent (peaks are read-and-reset), so a second run
        // at the same instant would zero the usage the first one consumed.
        auto now = TimeProvider_();
        if (LastManageInstant_ && now <= *LastManageInstant_) {
            return;
        }
        LastManageInstant_ = now;
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

    //! The side's base pool, replaced by the worker-group overrides: a worker in
    //! several listed groups takes the max (the pool reflects the memory this
    //! worker actually has). Requires #Lock_.
    i64 EffectiveFairSharePool(const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& side)
    {
        std::optional<i64> best;
        for (const auto& group : WorkerGroups_) {
            if (auto it = side->WorkerGroupFairSharePoolOverrides.find(group); it != side->WorkerGroupFairSharePoolOverrides.end()) {
                best = std::max<i64>(best.value_or(it->second), it->second);
            }
        }
        return best.value_or(side->FairSharePool);
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
            parameters->JobGuarantee + static_cast<double>(EffectiveFairSharePool(parameters)) * demand / totalDemand);
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

        struct TStreamMeasurements
        {
            TStreamData* StreamData = nullptr;
            std::optional<double> OverrideLimit;
            double InflatedDrainedTotal = 0;
            double Baseline = 0;
            double OfferedRate = 0;
            double V1Demand = 0;
            double PeakInflightBytes = 0;
            //! Current in-flight snapshot, NOT the peak: peaks of different
            //! streams are not simultaneous, so summing them over-counts the pool
            //! (a burst that filled and drained within one tick would keep its
            //! peak bytes charged) and starves small streams; the snapshot also
            //! stays visible for a stream whose buffer saw no Update() at all.
            double UsedInflatedBytes = 0;
            double PendingInflatedBytes = 0;
            bool HasBacklog = false;
        };

        std::vector<TStreamPlan> plans;
        plans.reserve(JobIdToState_.size() * 4);

        double totalInputDemand = 1;
        double totalOutputDemand = 1;

        // Phase A: per-stream accounting and sensors (identical for v1 and v2).
        auto computeMeasurements = [&] (TSideState& side,
            const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec,
            const TComputationId& computationId,
            bool isInput) {
            i64 inflationPerMessage = SideInflation(isInput);
            THashMap<TStreamId, TStreamMeasurements> measurements;
            for (auto& [streamId, streamData] : side.Streams) {
                auto usage = streamData.LimitUsageState->Read();
                i64 inflatedTotal = usage.CumulativeByteOut + usage.CumulativeCountOut * inflationPerMessage;
                streamData.PushDemand.Update(static_cast<double>(inflatedTotal), now);
                streamData.RawBytesRate.Update(
                    static_cast<double>(isInput ? usage.CumulativeByteOut : usage.CumulativeByteIn),
                    now);
                double measured = streamData.PushDemand.GetRate(now).value_or(0.0);
                double baseline = baselineDemand(streamId, computationId, inflationPerMessage);
                double demand = std::max(measured, baseline);
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
                streamData.LimitUsageState->SetEstimatedInflatedSpeed(demand);
                i64 maxInflightBytes = streamData.LimitUsageState->ReadAndResetMaxInflatedInflightBytes();
                i64 currentLimit = streamData.LimitUsageState->GetLimitBytes();
                streamData.UtilizationGauge.Update(
                    currentLimit > 0 ? static_cast<double>(maxInflightBytes) / static_cast<double>(currentLimit) : 0.0);
                measurements[streamId] = TStreamMeasurements{
                    .StreamData = &streamData,
                    .OverrideLimit = GetOverrideLimit(spec, computationId, streamId),
                    .InflatedDrainedTotal = static_cast<double>(inflatedTotal),
                    .Baseline = baseline,
                    .OfferedRate = streamData.LimitUsageState->GetOfferedInflatedBytesPerSecond(),
                    .V1Demand = demand,
                    .PeakInflightBytes = static_cast<double>(maxInflightBytes),
                    .UsedInflatedBytes = static_cast<double>(inflatedSize),
                    .PendingInflatedBytes = static_cast<double>(usage.PendingInflatedBytes),
                    // Output buffers always count as backlogged: the producer side
                    // cannot see the store, releasing to zero would stall the epoch.
                    .HasBacklog = !isInput || usage.PendingInflatedBytes > 0 || inflatedSize > 0,
                };
            }
            return measurements;
        };

        // Phase B: assemble per-stream limit plans.
        //! Returns the planned demand of the stream.
        auto planStream = [&] (TStreamMeasurements& streamMeasurements,
            TIssuanceState* issuance,
            const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec,
            bool isInput,
            double epochCycleSeconds,
            double* totalDemand) -> double {
            // Distrust guard: the measured cycle never acts beyond the configured
            // buffered-time cap of this side.
            epochCycleSeconds = std::min(epochCycleSeconds, spec->MaxDuration.SecondsFloat());
            double demand = streamMeasurements.V1Demand;
            if (issuance) {
                issuance->MaxDrainRate.SetBucketCount(DynamicSpec_->MaxRateEstimatorBuckets);
                issuance->MaxDrainRate.Update(streamMeasurements.InflatedDrainedTotal, now, TDuration::Seconds(epochCycleSeconds));
                if (issuance->MaxDrainRate.GetMaxRate().value_or(0.0) > 0) {
                    issuance->SeedDemand = 0;
                }
                demand = std::max({
                    issuance->MaxDrainRate.GetMaxRate().value_or(0.0),
                    streamMeasurements.Baseline,
                    DynamicSpec_->V2UseOfferedRate ? streamMeasurements.OfferedRate : 0.0,
                    issuance->SeedDemand,
                });
            }
            if (issuance) {
                // The persisted warmup must carry the sizing v2 actually uses,
                // not the phase-A EMA (which decays between bursty drains) — but
                // smoothed, so a converged pipeline stops producing state writes.
                const double alpha = std::min(
                    1.0,
                    DynamicSpec_->ManagePeriod.SecondsFloat() / std::max(DynamicSpec_->DemandWindow.SecondsFloat(), 1.0));
                issuance->SmoothedDemand += (demand - issuance->SmoothedDemand) * alpha;
                streamMeasurements.StreamData->LimitUsageState->SetEstimatedInflatedSpeed(issuance->SmoothedDemand);
            }
            if (!streamMeasurements.OverrideLimit.has_value()) {
                *totalDemand += demand;
            }
            plans.push_back({
                .StreamData = streamMeasurements.StreamData,
                .Issuance = issuance,
                .Spec = &spec,
                .Demand = demand,
                .OverrideLimit = streamMeasurements.OverrideLimit,
                .TotalDemand = totalDemand,
                .IsInput = isInput,
                .HasBacklog = streamMeasurements.HasBacklog,
                .PeakInflightBytes = streamMeasurements.PeakInflightBytes,
                .UsedInflatedBytes = streamMeasurements.UsedInflatedBytes,
                .PendingInflatedBytes = streamMeasurements.PendingInflatedBytes,
                .EpochCycleSeconds = epochCycleSeconds,
            });
            return demand;
        };

        const double managePeriodSeconds = DynamicSpec_->ManagePeriod.SecondsFloat();
        for (auto& [jobId, jobState] : JobIdToState_) {
            jobState.InputEpochCycleTracker->SetWindow(DynamicSpec_->EpochCycleWindowSamples);
            double epochCycleSeconds = managePeriodSeconds;
            if (auto medianCycle = jobState.InputEpochCycleTracker->GetMedianCycle()) {
                epochCycleSeconds = std::max(epochCycleSeconds, medianCycle->SecondsFloat());
                jobState.EpochCycleGauge.Update(medianCycle->SecondsFloat());
                const double alpha = std::min(
                    1.0,
                    DynamicSpec_->ManagePeriod.SecondsFloat() / std::max(DynamicSpec_->DemandWindow.SecondsFloat(), 1.0));
                jobState.SmoothedEpochCycleSeconds = jobState.SmoothedEpochCycleSeconds > 0
                    ? jobState.SmoothedEpochCycleSeconds + (medianCycle->SecondsFloat() - jobState.SmoothedEpochCycleSeconds) * alpha
                    : medianCycle->SecondsFloat();
            }

            auto inputMeasurements = computeMeasurements(jobState.Input, DynamicSpec_->InputBuffer, jobState.ComputationId, /*isInput*/ true);
            auto outputMeasurements = computeMeasurements(jobState.Output, DynamicSpec_->OutputBuffer, jobState.ComputationId, /*isInput*/ false);

            double jobInputDemand = 0;
            for (auto& [streamId, streamMeasurements] : inputMeasurements) {
                auto* issuance = DynamicSpec_->EnableV2 ? &jobState.InputIssuance[streamId] : nullptr;
                jobInputDemand += planStream(streamMeasurements, issuance, DynamicSpec_->InputBuffer, /*isInput*/ true, epochCycleSeconds, &totalInputDemand);
            }

            double totalInputRawRate = 0;
            for (auto& [streamId, streamData] : jobState.Input.Streams) {
                totalInputRawRate += streamData.RawBytesRate.GetRate(now).value_or(0.0);
            }

            for (auto& [streamId, streamMeasurements] : outputMeasurements) {
                auto* issuance = DynamicSpec_->EnableV2 ? &jobState.OutputIssuance[streamId] : nullptr;
                if (issuance) {
                    // Local fast path: the stream's demand follows the job's CURRENT
                    // input demand through its measured production ratio, so an input
                    // speedup (or a cold input backlog) opens the output budget in
                    // the same tick, one epoch before the drain can show it.
                    double producedRawRate = streamMeasurements.StreamData->RawBytesRate.GetRate(now).value_or(0.0);
                    // The cap only guards against a degenerate ratio when the input
                    // rate measurement is vanishingly small.
                    double ratio = totalInputRawRate > 0
                        ? std::min(producedRawRate / totalInputRawRate, MaxSaneIORatio)
                        : 0.0;
                    streamMeasurements.Baseline = std::max(streamMeasurements.Baseline, ratio * jobInputDemand);
                }
                // The output side reuses the INPUT epoch cycle as a proxy (the
                // output drain cycle is a different downstream process): it only
                // raises the BDP floor, boxed by the used peak and max_duration,
                // so precision does not matter.
                planStream(streamMeasurements, issuance, DynamicSpec_->OutputBuffer, /*isInput*/ false, epochCycleSeconds, &totalOutputDemand);
            }

            for (auto& [streamId, streamData] : jobState.Output.Streams) {
                double producedRawRate = streamData.RawBytesRate.GetRate(now).value_or(0.0);
                streamData.IORatioGauge.Update(totalInputRawRate > 0 ? producedRawRate / totalInputRawRate : 0.0);
            }
        }

        if (!DynamicSpec_->EnableV2) {
            for (const auto& entry : plans) {
                i64 newLimit = static_cast<i64>(ComputeLimit(*entry.Spec, entry.Demand, *entry.TotalDemand, entry.OverrideLimit));
                entry.StreamData->LimitUsageState->SetLimitBytes(newLimit);
                entry.StreamData->LimitBytesGauge.Update(newLimit);
            }
            return;
        }
        ApplyV2Issuance(plans);
    }

    void ApplyV2Issuance(std::vector<TStreamPlan>& plans)
    {
        const i64 floor = DynamicSpec_->V2Floor;
        const double gainEpochs = DynamicSpec_->V2GainEpochs;
        const double growth = DynamicSpec_->V2HeadroomGrowthFactor;
        const double highUtilization = DynamicSpec_->V2HighUtilizationThreshold;
        const double publishThreshold = DynamicSpec_->V2PublishThreshold;

        // Pass 1: per-stream wanted limit = max(used peak + headroom, demandFloor)
        // clamped to [streamFloor, min(jobLimit, drainCap)].
        std::array<double, 2> wantedTotalBySide = {0.0, 0.0};
        // In-flight bytes already committed per side: non-evictable, so a stream's
        // limit may not exceed the pool room the other streams' in-flight leaves.
        std::array<double, 2> usedBySide = {0.0, 0.0};
        std::vector<i64> wantedLimits;
        wantedLimits.reserve(plans.size());
        for (auto& entry : plans) {
            auto& issuance = *entry.Issuance;
            const i64 jobLimit = std::max<i64>((*entry.Spec)->JobGuarantee, (*entry.Spec)->JobLimit);

            i64 demandFloor = static_cast<i64>(std::min<double>(
                gainEpochs * entry.Demand * entry.EpochCycleSeconds,
                static_cast<double>(jobLimit)));
            // Drain-time budget: never buffer more than |max_duration| seconds of
            // demand. Raised by the announced backlog so a cold stream can admit
            // its pending offers to bootstrap.
            const i64 drainCap = static_cast<i64>(std::min<double>(
                static_cast<double>(jobLimit),
                std::max(
                    entry.Demand * (*entry.Spec)->MaxDuration.SecondsFloat(),
                    entry.PendingInflatedBytes)));
            i64 streamFloor = entry.HasBacklog ? std::min<i64>(floor, jobLimit) : 0;

            const i64 issuedPrev = issuance.IssuedLimit >= 0
                ? issuance.IssuedLimit
                : entry.StreamData->LimitUsageState->GetLimitBytes();
            // The peak resets when read: a stream whose job produced no usage
            // updates this tick (e.g. blocked on a stalled downstream) reads as
            // 0 while its bytes are still resident — the sizing must not
            // collapse below them.
            const double activeBytes = std::max(entry.PeakInflightBytes, entry.UsedInflatedBytes);
            const double utilization = activeBytes / std::max<double>(issuedPrev, 1.0);
            if (utilization > highUtilization) {
                // Grow while the sender announces more backlog than the current
                // headroom (credit-by-announced-demand; only input streams carry an
                // announced backlog, so output streams grow on the demand rule
                // alone) or while grants demonstrably convert into drain-rate
                // growth. The drain cap below bounds how far a standing bottleneck
                // can inflate the buffer.
                if (entry.PendingInflatedBytes > static_cast<double>(issuance.Headroom) ||
                    entry.Demand > issuance.LastProbeDemand * 1.25 ||
                    issuance.LastProbeDemand == 0)
                {
                    issuance.Headroom = static_cast<i64>(std::max<double>(issuance.Headroom, floor) * growth);
                    issuance.LastProbeDemand = std::max(entry.Demand, 1.0);
                }
            } else if (utilization < highUtilization / 2) {
                issuance.Headroom = static_cast<i64>(issuance.Headroom / growth);
                issuance.LastProbeDemand = std::min(issuance.LastProbeDemand, entry.Demand);
            }
            // std::min guards std::clamp against lo > hi when a misconfigured
            // V2Floor exceeds this side's jobLimit (as streamFloor does below).
            issuance.Headroom = std::clamp<i64>(issuance.Headroom, std::min<i64>(floor, jobLimit), jobLimit);

            // The measured-demand demandFloor is a floor-raiser, not a cap: capping by it
            // would be self-confirming (a limit set from measurements taken under
            // that same limit can never discover growth) — headroom above the used
            // peak is what probes beyond it.
            i64 wanted;
            if (entry.OverrideLimit.has_value()) {
                // Overrides win outright, as in the v1 formula: no clamps, no probing.
                wanted = static_cast<i64>(*entry.OverrideLimit);
            } else if (!entry.HasBacklog) {
                // No pending data and nothing in flight: hold no reservation at all.
                wanted = 0;
            } else {
                wanted = std::clamp<i64>(
                    std::max<i64>(static_cast<i64>(activeBytes) + issuance.Headroom, demandFloor),
                    streamFloor,
                    std::min(jobLimit, std::max(drainCap, streamFloor)));
            }
            wantedLimits.push_back(wanted);
            // Overrides live entirely outside the pool budget (v1 semantics):
            // neither their paper-sized limits nor their resident in-flight count
            // against the fair share — an overridden latency-bound writer
            // legitimately holds gigabytes, and charging that against the pool
            // starves every fair-share neighbour on the worker. The operator
            // budgets override memory on top of the pool, as v1 always did.
            if (!entry.OverrideLimit.has_value()) {
                usedBySide[entry.IsInput ? 0 : 1] += entry.UsedInflatedBytes;
                wantedTotalBySide[entry.IsInput ? 0 : 1] += static_cast<double>(wanted);
            }
        }

        // Pass 2: enforce Σ(fair-share issued) ≤ pool per side by proportional
        // trimming, then publish with hysteresis.
        const std::array<double, 2> fullPoolBySide = {
            static_cast<double>(EffectiveFairSharePool(DynamicSpec_->InputBuffer)),
            static_cast<double>(EffectiveFairSharePool(DynamicSpec_->OutputBuffer)),
        };
        for (size_t i = 0; i < plans.size(); ++i) {
            auto& entry = plans[i];
            auto& issuance = *entry.Issuance;
            const size_t side = entry.IsInput ? 0 : 1;
            i64 wanted = wantedLimits[i];
            if (!entry.OverrideLimit.has_value() && wantedTotalBySide[side] > fullPoolBySide[side]) {
                // The trim may go below the per-stream floor: on an oversubscribed
                // pool admission degrades to fresh-offer-at-a-time rather than
                // letting the sum breach the pool.
                wanted = static_cast<i64>(wanted * (fullPoolBySide[side] / wantedTotalBySide[side]));
            }
            if (!entry.OverrideLimit.has_value()) {
                // Bound Σused (not just Σissued) by the pool: a stream stuck holding
                // in-flight it cannot drain must not let its neighbours admit on top
                // of it. This caps growth to the room the others' in-flight leaves;
                // the transient overshoot is then one admission round, not a pool
                // share.
                const double poolRoom = fullPoolBySide[side] - (usedBySide[side] - entry.UsedInflatedBytes);
                wanted = std::min<i64>(wanted, std::max<i64>(static_cast<i64>(poolRoom), 0));
            }

            const i64 issuedPrev = issuance.IssuedLimit >= 0
                ? issuance.IssuedLimit
                : entry.StreamData->LimitUsageState->GetLimitBytes();
            // Shrinks always publish — the Σ ≤ pool invariant must not be deferred
            // by the anti-oscillation threshold; growth is hysteresed.
            if (issuance.IssuedLimit < 0 ||
                wanted < issuedPrev ||
                wanted - issuedPrev > publishThreshold * static_cast<double>(std::max<i64>(issuedPrev, 1)))
            {
                issuance.IssuedLimit = wanted;
                entry.StreamData->LimitUsageState->SetLimitBytes(wanted);
            }
            entry.StreamData->LimitBytesGauge.Update(issuance.IssuedLimit);
        }
    }

private:
    const IJobDirectoryPtr JobDirectory_;
    const std::function<TInstant()> TimeProvider_;
    const std::vector<TWorkerGroupId> WorkerGroups_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TDynamicBufferStateManagerSpecPtr DynamicSpec_;
    std::optional<TInstant> LastManageInstant_;
    const TPeriodicExecutorPtr BufferManagementExecutor_;

    THashMap<TJobId, TJobState> JobIdToState_;
    TMessageTransferingInfoPtr MessageTransferingInfo_;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionBufferState
    : public IPartitionBufferState
{
public:
    TPartitionBufferState(
        IBufferStateManagerPtr manager,
        TJobId jobId,
        TStreamLimitUsageStateMap outputStreamLimitUsageStates)
        : Manager_(std::move(manager))
        , JobId_(jobId)
        , OutputStreamLimitUsageStates_(std::move(outputStreamLimitUsageStates))
    { }

    void SeedWarmup(const TPartitionBufferWarmup& warmup) override
    {
        Manager_->SeedJob(JobId_, warmup);
    }

    TPartitionBufferWarmup GetWarmup() override
    {
        return Manager_->GetJobWarmup(JobId_);
    }

    bool IsWarmupEnabled() override
    {
        return Manager_->IsV2Enabled();
    }

    TDuration GetWarmupRefreshPeriod() override
    {
        return Manager_->GetWarmupRefreshPeriod();
    }

    const TStreamLimitUsageStateMap& GetOutputStreamLimitUsageStates() const override
    {
        return OutputStreamLimitUsageStates_;
    }

private:
    const IBufferStateManagerPtr Manager_;
    const TJobId JobId_;
    const TStreamLimitUsageStateMap OutputStreamLimitUsageStates_;
};

IPartitionBufferStatePtr CreatePartitionBufferState(
    IBufferStateManagerPtr manager,
    TJobId jobId,
    TStreamLimitUsageStateMap outputStreamLimitUsageStates)
{
    return New<TPartitionBufferState>(std::move(manager), jobId, std::move(outputStreamLimitUsageStates));
}

IBufferStateManagerPtr CreateBufferStateManager(
    IInvokerPtr invoker,
    IJobDirectoryPtr jobDirectory,
    TDynamicBufferStateManagerSpecPtr dynamicSpec,
    std::function<TInstant()> timeProvider,
    std::vector<TWorkerGroupId> workerGroups,
    bool enablePeriodicManagement)
{
    auto manager = New<TBufferStateManager>(
        std::move(invoker),
        std::move(jobDirectory),
        std::move(dynamicSpec),
        std::move(timeProvider),
        std::move(workerGroups));
    if (enablePeriodicManagement) {
        manager->Initialize();
    }
    return manager;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
