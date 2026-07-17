#include "controller_base.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <library/cpp/iterator/concatenate.h>

namespace NYT::NFlow {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetPartitionEventWatermark(
    const TNodeTraverseDataPtr& node,
    const TComputationSpecPtr& spec)
{
    TSystemTimestamp partitionWatermark = InfinitySystemTimestamp;
    for (const auto& [streamId, stream] : node->Streams) {
        if (!spec->InputStreamIds.contains(streamId)) {
            partitionWatermark = std::min(partitionWatermark, stream->EventWatermark);
        }
    }
    return partitionWatermark;
}

void HideEventWatermarkInplace(
    const TNodeTraverseDataPtr& node,
    const TSystemTimestamp& updateTime,
    const TComputationSpecPtr& spec)
{
    for (const auto& [streamId, stream] : node->Streams) {
        if (spec->InputStreamIds.contains(streamId)) {
            continue;
        }
        const ui64 delay = spec->WatermarkStrategy->WatermarkGenerator->OutOfOrdernessBound.Seconds();
        stream->EventWatermark = std::max(
            stream->EventWatermark,
            TSystemTimestamp(std::max<i64>(updateTime.Underlying(), delay) - delay));
    }
}

std::vector<TNodeTraverseDataPtr> ApplyAvailabilityGroupsEventWatermarkComputeRule(
    const THashMap<std::string, std::vector<TNodeTraverseDataPtr>>& nodesByAvailabilityGroup,
    const TComputationSpecPtr& spec,
    const TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger)
{
    struct TAvailabilityGroupSensors
    {
        TProfiler Profiler;
        std::string Key; // Availability group.

        TProfiler LocalProfiler = Profiler.WithTag("availability_group", Key);
        TGauge UnavailablePartitions = LocalProfiler.Gauge("/unavailable_partitions");
        TGauge ConfirmedUnavailablePartitions = LocalProfiler.Gauge("/confirmed_unavailable_partitions");
    };

    const auto& Logger = logger;

    if (!spec->WatermarkStrategy->WatermarkGenerator->UnavailablePartitionGroups) {
        std::vector<TNodeTraverseDataPtr> nodes;
        for (const auto& [_, group] : nodesByAvailabilityGroup) {
            for (const auto& node : group) {
                nodes.push_back(node);
            }
        }
        return nodes;
    }

    const auto& unavailableSpec = spec->WatermarkStrategy->WatermarkGenerator->UnavailablePartitionGroups;

    struct TAvailabilityGroupStatistics
    {
        i64 TotalPartitions = 0;
        i64 UnavailablePartitions = 0;
    };

    THashMap<std::string, TAvailabilityGroupStatistics> availabilityGroupStatistics;
    THashMap<std::string, TSystemTimestamp> availabilityGroupMinUpdateTimestamp;
    for (const auto& [availabilityGroup, nodes] : nodesByAvailabilityGroup) {
        for (const auto& traverseData : nodes) {
            auto& statistics = availabilityGroupStatistics[availabilityGroup];
            statistics.TotalPartitions += 1;
            const auto lastUnavailableTimestamp = GetPartitionLastUnavailableTimestamp(traverseData, spec, logger);
            if (lastUnavailableTimestamp.has_value()) {
                statistics.UnavailablePartitions += 1;

                auto& minTimestamp = availabilityGroupMinUpdateTimestamp.emplace(availabilityGroup, *lastUnavailableTimestamp).first->second;
                minTimestamp = std::min(minTimestamp, *lastUnavailableTimestamp);
            }
        }
    }

    std::vector<TNodeTraverseDataPtr> allNodes;
    THashMap<std::string, std::vector<TNodeTraverseDataPtr>> nodesToHideWatermarks; // {availabilityGroup: [nodes...], ...}
    int unavailableGroups = 0;
    for (const auto& [availabilityGroup, statistics] : availabilityGroupStatistics) {
        auto& sensors = sensorsOwner.Get<TAvailabilityGroupSensors>(availabilityGroup);
        sensors.UnavailablePartitions.Update(statistics.UnavailablePartitions);

        bool hideUnavailableWatermarks = false;
        if (statistics.UnavailablePartitions == statistics.TotalPartitions) {
            YT_TLOG_WARNING("Availability group is unavailable (all partitions of group are unavailable)")
                .With("AvailabilityGroup", availabilityGroup)
                .With("TotalPartitions", statistics.TotalPartitions);
            hideUnavailableWatermarks = true;
            unavailableGroups += 1;
        }

        const auto& nodes = nodesByAvailabilityGroup.at(availabilityGroup);
        if (!hideUnavailableWatermarks) {
            sensors.ConfirmedUnavailablePartitions.Update(0);
            allNodes.insert(allNodes.end(), nodes.begin(), nodes.end());
        } else {
            sensors.ConfirmedUnavailablePartitions.Update(statistics.TotalPartitions);
            for (const auto& node : nodes) {
                auto nodeCopy = NYTree::CloneYsonStruct(node);
                allNodes.push_back(nodeCopy);
                nodesToHideWatermarks[availabilityGroup].push_back(nodeCopy);
            }
        }
    }

    if (unavailableGroups == 0) {
        return allNodes;
    }

    const int totalGroups = availabilityGroupStatistics.size();
    const int availableGroups = totalGroups - unavailableGroups;
    if (unavailableGroups > unavailableSpec->MaxUnavailableGroups || availableGroups < unavailableSpec->MinAvailableGroups) {
        YT_TLOG_ERROR("Cannot advance the watermark past unavailable availability groups")
            .With("UnavailableGroups", unavailableGroups)
            .With("AvailableGroups", availableGroups)
            .With("TotalGroups", totalGroups)
            .With("MaxUnavailableGroups", unavailableSpec->MaxUnavailableGroups)
            .With("MinAvailableGroups", unavailableSpec->MinAvailableGroups);
        return allNodes;
    }

    for (const auto& [availabilityGroup, nodes] : nodesToHideWatermarks) {
        const auto updateTimestamp = availabilityGroupMinUpdateTimestamp.at(availabilityGroup);
        for (const auto& node : nodes) {
            HideEventWatermarkInplace(node, updateTimestamp, spec);
        }
    }
    return allNodes;
}

std::vector<TNodeTraverseDataPtr> ApplyIdlePartitionsRule(
    const std::vector<TNodeTraverseDataPtr>& nodes,
    const TComputationSpecPtr& spec,
    const TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger,
    const IStatusErrorStatePtr& watermarkStallErrorState)
{
    struct TIdlePartitionsSensors
    {
        TProfiler Profiler;
        TGauge Detected = Profiler.Gauge("/idle_partitions_detected");
        TGauge Ignored = Profiler.Gauge("/idle_partitions_ignored");
    };

    struct TIdlePartitionsStatistics
    {
        i64 TotalPartitions = 0;
        i64 Detected = 0;
        i64 Ignored = 0;
        i64 IgnoreLimit = 0;
    };

    const auto& Logger = logger;
    const auto& idleSpec = spec->WatermarkStrategy->WatermarkGenerator->IdlePartitions;
    auto& sensors = sensorsOwner.Get<TIdlePartitionsSensors>();

    if (!idleSpec) {
        sensors.Detected.Update(0);
        sensors.Ignored.Update(0);
        watermarkStallErrorState->ClearError();
        return nodes;
    }

    TIdlePartitionsStatistics statistics;
    statistics.TotalPartitions = std::ssize(nodes);
    statistics.IgnoreLimit = static_cast<i64>(idleSpec->MaxRatio * statistics.TotalPartitions);

    std::vector<TNodeTraverseDataPtr> preparedNodes;

    for (const auto& node : nodes) {
        auto lastIdleTimestamp = GetPartitionLastIdleTimestamp(node, spec);
        if (lastIdleTimestamp.has_value()) {
            statistics.Detected++;
            if (statistics.Ignored < statistics.IgnoreLimit) {
                statistics.Ignored++;
                auto preparedNode = NYTree::CloneYsonStruct(node);
                HideEventWatermarkInplace(preparedNode, *lastIdleTimestamp, spec);
                preparedNodes.push_back(preparedNode);
            } else {
                preparedNodes.push_back(node);
            }
        } else {
            preparedNodes.push_back(node);
        }
    }

    sensors.Detected.Update(statistics.Detected);
    sensors.Ignored.Update(statistics.Ignored);

    std::string result;
    if (statistics.Detected == 0) {
        result = "no_idle_partitions";
    } else if (statistics.Ignored == statistics.Detected) {
        result = "applied";
    } else if (statistics.Ignored > 0) {
        result = "partially_applied";
    } else {
        result = "zero_limit_and_not_applied";
    }

    if (statistics.Detected != 0) {
        YT_TLOG_DEBUG("IdlePartitions rule summary")
            .With("Enabled", true)
            .With("TotalPartitions", statistics.TotalPartitions)
            .With("Detected", statistics.Detected)
            .With("Ignored", statistics.Ignored)
            .With("Limit", statistics.IgnoreLimit)
            .With("MaxRatio", idleSpec->MaxRatio)
            .With("IdleDuration", idleSpec->Duration)
            .With("Result", result);
    }

    // When the idle fraction exceeds |MaxRatio| (so some idle partitions cannot be ignored) but stays
    // below 100%, the remaining idle partitions gate the watermark. The all-idle case is handled
    // elsewhere, so warn the pipeline owner only about this partial-idle stall. The status-profiler
    // error state is a persistent leaf: it is raised while the stall holds and cleared otherwise, and
    // its own break/recover logging (wired to the public logger) surfaces the message to the owner.
    if (statistics.Detected > statistics.IgnoreLimit && statistics.Detected < statistics.TotalPartitions) {
        watermarkStallErrorState->SetError(TError(
            "Watermark cannot advance because too many source partitions are idle")
            << TErrorAttribute("idle_partitions", statistics.Detected)
            << TErrorAttribute("total_partitions", statistics.TotalPartitions)
            << TErrorAttribute("watermark_gating_partitions", statistics.Detected - statistics.Ignored)
            << TErrorAttribute("max_ratio", idleSpec->MaxRatio));
    } else {
        watermarkStallErrorState->ClearError();
    }

    return preparedNodes;
}

std::vector<TNodeTraverseDataPtr> ApplyLateDataPartitionsRule(
    const std::vector<TNodeTraverseDataPtr>& nodes,
    const TComputationSpecPtr& spec,
    const TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger)
{
    struct TLateDataPartitionsSensors
    {
        TProfiler Profiler;
        TGauge Detected = Profiler.Gauge("/late_data_partitions_detected");
        TGauge Ignored = Profiler.Gauge("/late_data_partitions_ignored");
    };

    struct TLateDataPartitionsStatistics
    {
        i64 TotalPartitions = 0;
        i64 Detected = 0;
        i64 Ignored = 0;
        int PercentileIndex = 0;
        TSystemTimestamp MinWatermark = InfinitySystemTimestamp;
        TSystemTimestamp MaxWatermark = ZeroSystemTimestamp;
        TSystemTimestamp PercentileWatermark = ZeroSystemTimestamp;
        TSystemTimestamp ThresholdWatermark = ZeroSystemTimestamp;
        TSystemTimestamp EffectiveWatermark = ZeroSystemTimestamp;
        TDuration MaxDelay = TDuration::Zero();
    };

    const auto& Logger = logger;
    const auto& lateDataSpec = spec->WatermarkStrategy->WatermarkGenerator->LateDataPartitions;
    auto& sensors = sensorsOwner.Get<TLateDataPartitionsSensors>();

    if ((spec->OutputStreamIds.empty() && spec->SourceStreams.empty()) || !lateDataSpec || nodes.empty())
    {
        sensors.Detected.Update(0);
        sensors.Ignored.Update(0);
        return nodes;
    }

    std::vector<std::pair<TSystemTimestamp, TNodeTraverseDataPtr>> partitionWatermarks;
    for (const auto& node : nodes) {
        auto partitionWatermark = GetPartitionEventWatermark(node, spec);
        partitionWatermarks.push_back({partitionWatermark, node});
    }

    std::sort(partitionWatermarks.begin(), partitionWatermarks.end(), [] (const auto& a, const auto& b) {
        return a.first < b.first;
    });

    TLateDataPartitionsStatistics statistics;
    statistics.TotalPartitions = std::ssize(partitionWatermarks);
    statistics.MinWatermark = partitionWatermarks.front().first;
    statistics.MaxWatermark = partitionWatermarks.back().first;

    statistics.PercentileIndex = statistics.TotalPartitions *
        (PreciseWatermarkPercentile.Underlying() - lateDataSpec->Value.Underlying()) /
        PreciseWatermarkPercentile.Underlying();
    YT_VERIFY(statistics.PercentileIndex >= 0);
    YT_VERIFY(statistics.PercentileIndex <= statistics.TotalPartitions);
    YT_VERIFY(lateDataSpec->Value < PreciseWatermarkPercentile || statistics.PercentileIndex == 0);
    YT_VERIFY(lateDataSpec->Value > IgnoreInflightWatermarkPercentile || statistics.PercentileIndex == statistics.TotalPartitions);

    if (statistics.PercentileIndex == statistics.TotalPartitions) {
        sensors.Detected.Update(0);
        sensors.Ignored.Update(0);
        YT_TLOG_DEBUG("LateDataPartitions rule summary")
            .With("Enabled", true)
            .With("TotalPartitions", statistics.TotalPartitions)
            .With("Detected", 0)
            .With("Ignored", 0)
            .With("Result", "all_ignored_by_percentile");
        return nodes;
    }

    statistics.PercentileWatermark = partitionWatermarks[statistics.PercentileIndex].first;
    statistics.ThresholdWatermark = TSystemTimestamp(
        std::max<i64>(statistics.PercentileWatermark.Underlying(), lateDataSpec->Delay.Seconds()) -
        lateDataSpec->Delay.Seconds());
    statistics.EffectiveWatermark = std::max(statistics.MinWatermark, statistics.ThresholdWatermark);

    std::vector<TNodeTraverseDataPtr> preparedNodes;
    for (const auto& [watermark, node] : partitionWatermarks) {
        if (watermark < statistics.ThresholdWatermark) {
            statistics.Detected++;
            auto preparedNode = NYTree::CloneYsonStruct(node);
            HideEventWatermarkInplace(preparedNode, statistics.EffectiveWatermark, spec);
            preparedNodes.push_back(preparedNode);
            statistics.Ignored++;
            auto delay = TDuration::Seconds(statistics.PercentileWatermark.Underlying() - watermark.Underlying());
            statistics.MaxDelay = std::max(statistics.MaxDelay, delay);
        } else {
            preparedNodes.push_back(node);
        }
    }

    sensors.Detected.Update(statistics.Detected);
    sensors.Ignored.Update(statistics.Ignored);
    std::string result = statistics.Ignored > 0 ? "applied" : "no_late_partitions";

    if (statistics.Detected != 0) {
        YT_TLOG_DEBUG("LateDataPartitions rule summary")
            .With("TotalPartitions", statistics.TotalPartitions)
            .With("Detected", statistics.Detected)
            .With("Ignored", statistics.Ignored)
            .With("Value", lateDataSpec->Value)
            .With("Delay", lateDataSpec->Delay)
            .With("PercentileIndex", statistics.PercentileIndex)
            .With("MinWatermark", statistics.MinWatermark)
            .With("PercentileWatermark", statistics.PercentileWatermark)
            .With("ThresholdWatermark", statistics.ThresholdWatermark)
            .With("EffectiveWatermark", statistics.EffectiveWatermark)
            .With("MaxWatermark", statistics.MaxWatermark)
            .With("MaxDelay", statistics.MaxDelay)
            .With("Result", result);
    }

    return preparedNodes;
}

std::vector<TNodeTraverseDataPtr> ApplyEventWatermarkComputeRule(
    const THashMap<std::string, std::vector<TNodeTraverseDataPtr>>& nodesByAvailabilityGroup,
    const TComputationSpecPtr& spec,
    const TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger,
    const IStatusErrorStatePtr& watermarkStallErrorState)
{
    if (!spec->WatermarkStrategy->WatermarkGenerator) {
        std::vector<TNodeTraverseDataPtr> allNodes;
        for (const auto& [_, nodes] : nodesByAvailabilityGroup) {
            allNodes.insert(allNodes.end(), nodes.begin(), nodes.end());
        }
        return allNodes;
    }

    const auto nodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(nodesByAvailabilityGroup, spec, sensorsOwner, logger);
    const auto afterIdle = ApplyIdlePartitionsRule(nodes, spec, sensorsOwner, logger, watermarkStallErrorState);
    auto preparedNodes = ApplyLateDataPartitionsRule(afterIdle, spec, sensorsOwner, logger);

    return preparedNodes;
}

std::optional<TSystemTimestamp> GetPartitionLastIdleTimestamp(
    const TNodeTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    bool relaxed)
{
    const auto& watermarkSpec = spec->WatermarkStrategy->WatermarkGenerator;

    // 1) all streams should have zero inflight.
    // 2) Input streams should be completed.
    // 3) Source streams should have zero inflight for some time.
    for (const auto& [streamId, stream] : traverseData->Streams) {
        if (stream->InflightMetrics->Count != 0) {
            return std::nullopt;
        }
    }

    for (const auto& streamId : spec->InputStreamIds) {
        auto stream = traverseData->Streams.at(streamId);
        if (stream->State != EStreamState::Completed) {
            return std::nullopt;
        }
    }

    TSystemTimestamp idleTimestamp = traverseData->ReportTime;

    for (const auto& [streamId, source] : spec->SourceStreams) {
        auto stream = traverseData->Streams.at(streamId);
        auto inflightMetrics = stream->InflightMetrics;

        if (inflightMetrics->IdleDuration && watermarkSpec->IdlePartitions) {
            THROW_ERROR_EXCEPTION_UNLESS(inflightMetrics->LastIdleTimestamp, "LastIdleTimestamp must not be nullopt if IdleDuration is set");
            idleTimestamp = std::min(idleTimestamp, *inflightMetrics->LastIdleTimestamp);
            if (!relaxed && *inflightMetrics->IdleDuration < watermarkSpec->IdlePartitions->Duration) {
                return std::nullopt;
            }
        }
    }

    return idleTimestamp;
}

std::optional<TSystemTimestamp> GetPartitionLastUnavailableTimestamp(
    const TNodeTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    const NLogging::TLogger& logger)
{
    if (spec->SourceStreams.size() == 0) {
        return std::nullopt;
    }

    for (const auto& [streamId, source] : spec->SourceStreams) {
        auto stream = traverseData->Streams.at(streamId);
        auto inflightMetrics = stream->InflightMetrics;

        if (!inflightMetrics->UnavailableTimestamp) {
            return std::nullopt;
        }

        YT_VERIFY(spec->SourceStreams.size() == 1);
        return *inflightMetrics->UnavailableTimestamp;
    }

    const auto& Logger = logger;
    YT_TLOG_FATAL("Unreachable point reached in GetPartitionLastUnavailableTimestamp()");
    Y_UNREACHABLE();
}

THashMap<TStreamId, TStreamTraverseDataMetricsPtr> ComputeStreamMetrics(
    const std::vector<TNodeTraverseDataPtr>& traverseData,
    const TComputationSpecPtr& /*spec*/)
{
    struct TMinMaxTimestamps
    {
        TSystemTimestamp Min = InfinitySystemTimestamp;
        TSystemTimestamp Max = ZeroSystemTimestamp;
    };

    THashMap<TStreamId, TStreamTraverseDataMetricsPtr> metrics;

    auto computeMinMaxDifference = [&] (auto&& getter, auto&& setter) {
        THashMap<TStreamId, TMinMaxTimestamps> streamTimestamps;
        for (const auto& nodeTraverseData : traverseData) {
            for (const auto& [streamId, streamTraverseData] : nodeTraverseData->Streams) {
                auto& timestamps = streamTimestamps[streamId];
                timestamps.Min = std::min(timestamps.Min, getter(streamTraverseData));
                timestamps.Max = std::max(timestamps.Max, getter(streamTraverseData));
            }
        }
        for (const auto& [streamId, timestamps] : streamTimestamps) {
            auto& streamMetrics = metrics[streamId];
            if (!streamMetrics) {
                streamMetrics = New<TStreamTraverseDataMetrics>();
            }
            if (timestamps.Min == InfinitySystemTimestamp || timestamps.Min == ZeroSystemTimestamp) {
                // If there is no partitions or some partitions has uninitialized watermark.
                setter(streamMetrics, -1);
            } else {
                setter(streamMetrics, timestamps.Max.Underlying() - timestamps.Min.Underlying());
            }
        }
    };
    computeMinMaxDifference(
        [] (const auto& streamTraverseData) {
            return streamTraverseData->SystemWatermark;
        },
        [] (auto& metrics, double value) {
            metrics->SystemWatermarkMinMaxDifference = value;
        });
    computeMinMaxDifference(
        [] (const auto& streamTraverseData) {
            return streamTraverseData->EventWatermark;
        },
        [] (auto& metrics, double value) {
            metrics->EventWatermarkMinMaxDifference = value;
        });

    return metrics;
}

////////////////////////////////////////////////////////////////////////////////

void TComputationControllerBase::TExtendedParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("weight_multiplier", &TThis::WeightMultiplier)
        .Default(1.0)
        .GreaterThan(0);

    registrar.Parameter("interrupting_weight_multiplier", &TThis::InterruptingWeightMultiplier)
        .Default(0.1)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

TComputationControllerBase::TComputationControllerBase(
    TComputationControllerContextPtr context,
    TDynamicComputationControllerContextPtr dynamicContext)
    : Logger(context->Logger)
    , Context_(std::move(context))
    , Parameters_(DynamicPointerCast<IComputationController::TParameters>(TRegistry::Get()->ParseComputationParameters(Context_->ComputationSpec)))
    , SensorsOwner_(Context_->Profiler)
    // On a controller node the root status profiler is wired to the public logger, so raising or
    // clearing this error state also emits a public break/recover log line for the pipeline owner.
    , IdlePartitionsWatermarkStallErrorState_(Context_->StatusProfiler->ErrorState("/idle_partitions_watermark_stall"))
    , DynamicContext_(std::move(dynamicContext))
    , DynamicParameters_(DynamicPointerCast<IComputationController::TDynamicParameters>(
        TRegistry::Get()->ParseDynamicComputationParameters(Context_->ComputationSpec, DynamicContext_.Acquire()->DynamicComputationSpec)))
{
    YT_VERIFY(Parameters_);
    YT_VERIFY(DynamicParameters_);
    SubscribeReconfigured(BIND([this] (const TDynamicComputationControllerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = DynamicPointerCast<IComputationController::TDynamicParameters>(
            TRegistry::Get()->ParseDynamicComputationParameters(Context_->ComputationSpec, dynamicContext->DynamicComputationSpec));
    }));
}

TComputationControllerContextPtr TComputationControllerBase::GetContext() const
{
    return Context_;
}

TDynamicComputationControllerContextPtr TComputationControllerBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TComputationSpecPtr TComputationControllerBase::GetSpec() const
{
    return Context_->ComputationSpec;
}

TDynamicComputationSpecPtr TComputationControllerBase::GetDynamicSpec() const
{
    return DynamicContext_.Acquire()->DynamicComputationSpec;
}

IComputationController::TParametersPtr TComputationControllerBase::GetParametersBase() const
{
    return Parameters_;
}

IComputationController::TDynamicParametersPtr TComputationControllerBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

const TComputationId& TComputationControllerBase::GetComputationId() const
{
    return Context_->ComputationId;
}

void TComputationControllerBase::InterruptPartition(const TFlowViewPtr& flowView, const TPartitionId& partitionId)
{
    flowView->State->ExecutionSpec->Layout->UpdatePartition(partitionId, EPartitionState::Interrupting, flowView->State->ExecutionSpec->GetEpoch(), TInstant::Now());
}

void TComputationControllerBase::CompletePartition(const TFlowViewPtr& flowView, const TPartitionId& partitionId)
{
    flowView->State->ExecutionSpec->Layout->UpdatePartition(partitionId, EPartitionState::Completing, flowView->State->ExecutionSpec->GetEpoch(), TInstant::Now());
}

void TComputationControllerBase::CreateSourcePartition(
    const TFlowViewPtr& flowView,
    const TKey& sourceKey,
    const NYTree::IMapNodePtr& dynamicPartitionSpec)
{
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    partition->ComputationId = GetComputationId();
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = flowView->State->ExecutionSpec->GetEpoch();
    partition->StateTimestamp = TInstant::Now();
    partition->SourceKey = sourceKey;
    flowView->State->ExecutionSpec->Layout->CreatePartition(partition);
    UpdateDynamicPartitionSpec(flowView, partition->PartitionId, dynamicPartitionSpec);
}

void TComputationControllerBase::CreateRangePartition(
    const TFlowViewPtr& flowView,
    const TKey& lowerKey,
    const TKey& upperKey,
    const NYTree::IMapNodePtr& dynamicPartitionSpec)
{
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    partition->ComputationId = GetComputationId();
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = flowView->State->ExecutionSpec->GetEpoch();
    partition->StateTimestamp = TInstant::Now();
    partition->LowerKey = lowerKey;
    partition->UpperKey = upperKey;
    flowView->State->ExecutionSpec->Layout->CreatePartition(partition);
    UpdateDynamicPartitionSpec(flowView, partition->PartitionId, dynamicPartitionSpec);
}

void TComputationControllerBase::UpdateDynamicPartitionSpec(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    const NYTree::IMapNodePtr& dynamicComputationPartitionSpec)
{
    // The iteration owns a deep clone of the ephemeral state, so in-place
    // mutation is safe; the job-manager-owned fields are left untouched.
    const auto& spec = flowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    // TODO: Improve perf.
    if (spec->ComputationPartitionSpec &&
        (spec->ComputationPartitionSpec == dynamicComputationPartitionSpec ||
            AreNodesEqual(spec->ComputationPartitionSpec, dynamicComputationPartitionSpec)))
    {
        return;
    }
    spec->ComputationPartitionSpec = dynamicComputationPartitionSpec;
    YT_TLOG_INFO("Update dynamic partition spec")
        .With("PartitionId", partitionId)
        .With("NewDynamicPartitionSpec", ConvertToYsonString(spec, NYson::EYsonFormat::Text));
}

TProcessPartitionTraverseDataResultPtr TComputationControllerBase::ProcessPartitionTraverseData(
    const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
    const TFlowViewPtr& flowView)
{
    auto futurePartitionsTraverse = GetFuturePartitionsNodeTraverseData(flowView);
    THROW_ERROR_EXCEPTION_IF(
        traverseData.empty() && !futurePartitionsTraverse,
        "Computation %Qv has no partitions to process (neither current nor future). "
        "Check that source partition filters are well-formed and not over-restrictive",
        GetComputationId());

    auto nodesByAvailabilityGroup = GetNodesByAvailabilityGroup(traverseData, flowView);
    auto preparedTraverseData = ApplyEventWatermarkComputeRule(
        nodesByAvailabilityGroup,
        GetSpec(),
        SensorsOwner_,
        Logger,
        IdlePartitionsWatermarkStallErrorState_);
    if (futurePartitionsTraverse.has_value()) {
        preparedTraverseData.push_back(*futurePartitionsTraverse);
    }
    auto result = New<TProcessPartitionTraverseDataResult>();
    result->StreamMetrics = ComputeStreamMetrics(preparedTraverseData, GetSpec());
    result->MergedTraverseData = MergeNodeTraverseData(preparedTraverseData, GetSpec());
    return result;
}

std::optional<TNodeTraverseDataPtr> TComputationControllerBase::GetFuturePartitionsNodeTraverseData(
    const TFlowViewPtr& /*flowView*/)
{
    return std::nullopt;
}

double TComputationControllerBase::ComputePartitionWeight(const TPartitionId& partitionId, const TFlowViewPtr& flowView)
{
    auto partition = GetOrCrash(flowView->State->ExecutionSpec->Layout->Partitions, partitionId);
    if (partition->State == EPartitionState::Executing) {
        return GetDynamicParameters()->WeightMultiplier;
    } else if (partition->State == EPartitionState::Interrupting || partition->State == EPartitionState::Completing) {
        return GetDynamicParameters()->InterruptingWeightMultiplier;
    } else {
        return 0.0;
    }
}

void TComputationControllerBase::Init(IInitContextPtr /*initContext*/)
{ }

void TComputationControllerBase::Sync()
{ }

void TComputationControllerBase::Commit()
{ }

THashMap<std::string, std::vector<TNodeTraverseDataPtr>> TComputationControllerBase::GetNodesByAvailabilityGroup(
    const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
    const TFlowViewPtr& /*flowView*/)
{
    return {{"default", GetValues(traverseData)}};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
