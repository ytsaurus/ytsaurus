#include "universal_controller.h"

#include "computation_base.h"
#include "universal_controller_helpers.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/sink_controller.h>
#include <yt/yt/flow/library/cpp/common/source_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NFlow {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TKey MakeUniversalPartitionKey(const TStreamId& streamId, const TKey& sourceKey)
{
    NTableClient::TUnversionedOwningRowBuilder builder;
    builder.AddValue(NTableClient::MakeUnversionedStringValue(streamId.Underlying(), 0));
    for (auto value : sourceKey.Underlying()) {
        value.Id += 1;
        builder.AddValue(value);
    }
    return TKey(TKey::TUnderlying(builder.FinishRow()));
}

std::pair<TStreamId, TKey> SplitUniversalPartitionKey(const TKey& partitionKey)
{
    auto streamId = NTableClient::FromUnversionedValue<TStreamId>(partitionKey.Underlying()[0]);
    NTableClient::TUnversionedOwningRowBuilder builder;
    for (int i = 1; i < partitionKey.Underlying().GetCount(); ++i) {
        auto value = partitionKey.Underlying()[i];
        value.Id -= 1;
        builder.AddValue(value);
    }
    return {streamId, TKey(TKey::TUnderlying(builder.FinishRow()))};
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("sources", &TThis::Sources)
        .Default();
    registrar.Parameter("sinks", &TThis::Sinks)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationControllerPartitioningState::Register(TRegistrar registrar)
{
    registrar.Parameter("last_sink_channel_counts", &TThis::LastSinkChannelCounts)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationController::TExtendedParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationController::TExtendedDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_partition_count", &TThis::DesiredPartitionCount)
        .Default();
    registrar.Parameter("min_partition_count", &TThis::MinPartitionCount)
        .Default();
    registrar.Parameter("max_partition_count", &TThis::MaxPartitionCount)
        .Default();
    registrar.Parameter("sink_channel_multiplier", &TThis::SinkChannelMultiplier)
        .Default();
    registrar.Parameter("desired_average_partition_cpu_load", &TThis::DesiredAveragePartitionCpuLoad)
        .Default();
    registrar.Parameter("desired_average_partition_memory_used", &TThis::DesiredAveragePartitionMemoryUsed)
        .Default();
    registrar.Parameter("desired_average_partition_messages_per_second", &TThis::DesiredAveragePartitionMessagesPerSecond)
        .Default();
    registrar.Parameter("desired_average_partition_bytes_per_second", &TThis::DesiredAveragePartitionBytesPerSecond)
        .Default();
    registrar.Parameter("desired_average_partition_timer_count", &TThis::DesiredAveragePartitionTimerCount)
        .Default();
    registrar.Parameter("allowed_partition_count_deviation", &TThis::AllowedPartitionCountDeviation)
        .InRange(1.01, 100)
        .Default();
    registrar.Parameter("partition_count_double_delay", &TThis::PartitionCountDoubleDelay)
        .Default();
    registrar.Parameter("partition_count_half_delay", &TThis::PartitionCountHalfDelay)
        .Default();
    registrar.Postprocessor([] (TThis* arg) {
        if (arg->MinPartitionCount && arg->MaxPartitionCount && *arg->MinPartitionCount > *arg->MaxPartitionCount) {
            THROW_ERROR_EXCEPTION("min_partition_count must be less than or equal to max_partition_count (MinPartitionCount: %v, MaxPartitionCount: %v)",
                *arg->MinPartitionCount,
                *arg->MaxPartitionCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TUniversalComputationController::TUniversalComputationController(
    TComputationControllerContextPtr context,
    TDynamicComputationControllerContextPtr dynamicContext)
    : TComputationControllerBase(std::move(context), std::move(dynamicContext))
    , Sources_(CreateSources(GetContext(), GetSpec(), GetDynamicSpec()))
    , Sinks_(CreateSinks(GetContext(), GetSpec(), GetDynamicSpec()))
{
    if (GetSpec()->InputStreamIds.size() > 0 && GetSpec()->SourceStreams.size() > 0) {
        THROW_ERROR_EXCEPTION("Computation with inputs and sources is not supported");
    }

    if (UsesRangePartitioning()) {
        if (GetSpec()->GroupBySchema->GetColumnCount() < 1) {
            THROW_ERROR_EXCEPTION("Range-partitioned computation should have non-empty group-by schema");
        }
        const auto& firstColumn = GetSpec()->GroupBySchema->Columns()[0];
        if (!GetSpec()->ExperimentalEnableNonUintKey.value_or(TComputationSpec::ExperimentalEnableNonUintKeyDefault)) {
            const auto requiredType = NTableClient::SimpleLogicalType(NTableClient::ESimpleLogicalValueType::Uint64);
            if (*firstColumn.LogicalType() != *requiredType) {
                THROW_ERROR_EXCEPTION("First column in GroupBySchema is expected to have type %Qv but it has %Qv",
                    *requiredType,
                    *firstColumn.LogicalType());
            }
        }
        if (const auto& expression = firstColumn.Expression()) {
            static const THashSet<std::string> forbiddenTokens = {
                "bigb_hash",
                "%",
            };
            for (const auto& token : forbiddenTokens) {
                if (expression->find(token) != std::string::npos) {
                    THROW_ERROR_EXCEPTION("%Qv is not allowed for usage in expression of the first column", token);
                }
            }
        }
    }

    SubscribeReconfigured(BIND([this] (const TDynamicComputationControllerContextPtr& /*dynamicContext*/) {
        for (const auto& [streamId, source] : Sources_) {
            auto dynamicSourceContext = New<TDynamicSourceControllerContext>();
            dynamicSourceContext->DynamicSourceSpec = GetOrDefault(GetDynamicSpec()->SourceStreams, streamId, New<TDynamicSourceSpec>());
            source->Reconfigure(dynamicSourceContext);
        }
        for (const auto& [sinkId, sink] : Sinks_) {
            auto dynamicSinkContext = New<TDynamicSinkControllerContext>();
            dynamicSinkContext->DynamicSinkSpec = GetOrDefault(GetDynamicSpec()->Sinks, sinkId, New<TDynamicSinkSpec>());
            sink->Reconfigure(dynamicSinkContext);
        }
    }));
}

void TUniversalComputationController::Init(IInitContextPtr initContext)
{
    initContext->InitClient(PartitioningState_, "partitioning/v0");
    for (const auto& [streamId, source] : Sources_) {
        auto sourceInitContext = initContext->WithPrefix(Format("sources/%v", streamId));
        // Nest the source state under its identity - the same string that versions its partition
        // keys - so changing the identifying params orphans the previous state (it is then reclaimed
        // by the state manager) and the source starts fresh.
        if (auto identity = source->GetSourceIdentity(); !identity.empty()) {
            sourceInitContext = sourceInitContext->WithPrefix(identity);
        }
        source->Init(std::move(sourceInitContext));
    }
    for (const auto& [sinkId, sink] : Sinks_) {
        sink->Init(initContext->WithPrefix(Format("sinks/%v", sinkId)));
    }
}

void TUniversalComputationController::Sync()
{
    for (const auto& [streamId, source] : Sources_) {
        source->Sync();
    }
    for (const auto& [sinkId, sink] : Sinks_) {
        sink->Sync();
    }
}

void TUniversalComputationController::Commit()
{
    for (const auto& [streamId, source] : Sources_) {
        source->Commit();
    }
    for (const auto& [sinkId, sink] : Sinks_) {
        sink->Commit();
    }
}

void TUniversalComputationController::UpdateWatermarkState(TWatermarkStatePtr watermarkState)
{
    WatermarkState_ = watermarkState;
    for (const auto& sink : GetValues(Sinks_)) {
        sink->UpdateWatermarkState(watermarkState);
    }
}

TWatermarkStatePtr TUniversalComputationController::GetWatermarkState()
{
    return WatermarkState_.Acquire();
}

bool TUniversalComputationController::UsesRangePartitioning() const
{
    const auto& spec = GetSpec();
    const bool hasInputStreams = !spec->InputStreamIds.empty();
    const bool keyVisitorOnly = !spec->KeyVisitorStreams.empty() && spec->SourceStreams.empty();
    return hasInputStreams || keyVisitorOnly;
}

bool TUniversalComputationController::IsFullCoverage(
    const std::vector<TPartitionId>& computationPartitions,
    const TFlowViewPtr& flowView)
{
    const auto grouped = GroupPartitions(computationPartitions, flowView);

    if (!grouped.BadPartitions.empty()) {
        return false;
    }

    if (UsesRangePartitioning()) {
        if (!grouped.KeyPartitions.empty()) {
            return false;
        }
        return !TestRangeOverlaps(GetValues(grouped.RangePartitions)) && UniteRanges(GetValues(grouped.RangePartitions)) == std::vector{UniversalKeyRange()};
    } else if (!GetSpec()->SourceStreams.empty()) {
        if (!grouped.RangePartitions.empty()) {
            return false;
        }
        auto expectedKeys = GetSourcePartitionKeys();
        if (!expectedKeys) {
            return false;
        }
        const auto actualKeys = GetValues(grouped.KeyPartitions);
        const auto actualKeysSet = THashSet<TKey>(actualKeys.begin(), actualKeys.end());
        const auto expectedKeysList = GetKeys(*expectedKeys);
        const auto expectedKeysSet = THashSet<TKey>(expectedKeysList.begin(), expectedKeysList.end());
        return actualKeys.size() == expectedKeys->size() && actualKeysSet == expectedKeysSet;
    } else {
        THROW_ERROR_EXCEPTION("Computation with no inputs and no sources is not supported");
    }
}

struct TUniversalComputationController::TInputAutoPartitioningContext
{
    const TFlowViewPtr& FlowView;
    const THashMap<TPartitionId, TKeyRange>& PartitionRanges;

    // Filled by InputAutoPartitioningCollectData().
    bool AllPartitionsHaveStatuses = false;
    bool AllPartitionsHavePivots = false;
    bool AnotherComputationRecentlyRepartitioned = false;
    //! Time span when not repartitioning and no start/stop happened.
    TDuration NormalFlightDuration;

    // Filled by InputAutoPartitioningCalculateOptimalCount().
    i64 ProposedCount = 0;
    bool RecreateNow = false;
    THashMap<TPartitionId, double> Weights;
    //! Current target-queue partition (channel) count per sink that reports one, keyed by sink id.
    THashMap<TSinkId, i64> SinkChannelCounts;

    // Filled by InputAutoPartitioningBuildRanges().
    std::vector<TKeyRange> NewRanges;
    std::vector<double> NewWeights;

    TInputAutoPartitioningContext(const TFlowViewPtr& flowView, const THashMap<TPartitionId, TKeyRange>& partitionRanges)
        : FlowView(flowView)
        , PartitionRanges(partitionRanges)
    { }
};

void TUniversalComputationController::DoPartitioning(
    const std::vector<TPartitionId>& computationPartitions,
    const TFlowViewPtr& flowView)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;

    auto grouped = GroupPartitions(computationPartitions, flowView);
    auto blockedStreamComputer = New<TBlockedStreamComputer>(flowView, grouped.InterruptingPartitions, Logger);

    for (const auto& partitionId : grouped.BadPartitions) {
        InterruptPartition(flowView, partitionId);
    }

    static const auto trivialDynamicPartitionSpec = GetEphemeralNodeFactory()->CreateMap();

    for (const auto& partitionId : grouped.InterruptingPartitions) {
        UpdateDynamicPartitionSpec(flowView, partitionId, trivialDynamicPartitionSpec);
    }

    if (UsesRangePartitioning()) {
        for (const auto& partitionId : GetKeys(grouped.KeyPartitions)) {
            InterruptPartition(flowView, partitionId); // Invalid partition, just interrupt and forget.
        }

        auto makeDynamicPartitionSpec = [&] (const TKey& lower, const TKey& upper) {
            auto blockedStreams = blockedStreamComputer->GetBlockedStreams(lower, upper);
            if (blockedStreams.empty()) {
                return trivialDynamicPartitionSpec;
            }
            auto dynamicComputationPartitionSpec = New<TUniversalComputationDynamicPartitionSpec>();
            dynamicComputationPartitionSpec->BlockedOutputStreams = std::move(blockedStreams);
            return ConvertTo<IMapNodePtr>(dynamicComputationPartitionSpec);
        };

        // If current ranges are invalid, interrupt and clear the list. After that grouped.RangePartitions will be correct in any case.
        if (TestRangeOverlaps(GetValues(grouped.RangePartitions)) || UniteRanges(GetValues(grouped.RangePartitions)) != std::vector{UniversalKeyRange()}) {
            for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
                blockedStreamComputer->AddInterruptingPartition(partitionId);
                InterruptPartition(flowView, partitionId);
            }
            grouped.RangePartitions.clear();
        }

        TInputAutoPartitioningContext context(flowView, grouped.RangePartitions);
        InputAutoPartitioningCollectData(context);
        InputAutoPartitioningCalculateOptimalCount(context);
        // A change in any sink's target-queue partition count invalidates that sink's persisted
        // per-partition producer ids (a queue producer session is bound to the queue's partitioning).
        // Force a recreation so fresh producer ids are generated (YTFLOW-572). Doing it here, before
        // BuildRanges, lets the usual pivot-readiness gate defer the recreation when needed;
        // LastSinkChannelCounts is updated only once a recreation actually runs, so the change is not
        // lost while deferred. Only sinks known at both points are compared, which ignores transient
        // unknown counts.
        for (const auto& [sinkId, channelCount] : context.SinkChannelCounts) {
            auto it = PartitioningState_->LastSinkChannelCounts.find(sinkId);
            if (it != PartitioningState_->LastSinkChannelCounts.end() && it->second != channelCount) {
                YT_TLOG_INFO("Partitioning: sink target-queue partition count changed, forcing recreation to regenerate producer ids")
                    .With("SinkId", sinkId)
                    .With("PreviousChannelCount", it->second)
                    .With("NewChannelCount", channelCount);
                context.RecreateNow = true;
            }
        }
        InputAutoPartitioningBuildRanges(context);
        InputAutoPartitioningTryRebalance(context);
        LastCommonRepartitioningInstant_ = GetContext()->CommonContext->LastRepartitioningInstant;

        if (context.RecreateNow) {
            for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
                blockedStreamComputer->AddInterruptingPartition(partitionId);
                InterruptPartition(flowView, partitionId);
            }
            for (const auto& [lower, upper] : context.NewRanges) {
                CreateRangePartition(flowView, lower, upper, makeDynamicPartitionSpec(lower, upper));
            }
            LastRepartitionTime_ = TInstant::Now();
            GetContext()->CommonContext->LastRepartitioningInstant = LastRepartitionTime_;
            LastCommonRepartitioningInstant_ = LastRepartitionTime_;
            // Merge (not replace) so a sink whose count is transiently unknown keeps its last-known
            // value for future comparisons.
            for (const auto& [sinkId, channelCount] : context.SinkChannelCounts) {
                PartitioningState_->LastSinkChannelCounts[sinkId] = channelCount;
            }
        } else {
            for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
                auto partition = GetOrCrash(layout->Partitions, partitionId);
                UpdateDynamicPartitionSpec(flowView, partitionId, makeDynamicPartitionSpec(*partition->LowerKey, *partition->UpperKey));
            }
        }
    } else if (GetSpec()->SourceStreams.size() > 0) {
        for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
            InterruptPartition(flowView, partitionId); // Invalid partition, just interrupt and forget.
        }

        ProcessSourcePartitionStatuses(grouped.KeyPartitions, flowView);

        auto expectedKeys = GetSourcePartitionKeys();
        if (expectedKeys) {
            auto makeDynamicPartitionSpec = [&] (const TKey& sourceKey, const NYTree::IMapNodePtr& dynamicSourcePartitionSpec) {
                auto dynamicComputationPartitionSpec = New<TUniversalComputationDynamicPartitionSpec>();
                dynamicComputationPartitionSpec->ActiveSource = dynamicSourcePartitionSpec;
                YT_VERIFY(dynamicComputationPartitionSpec->ActiveSource);
                dynamicComputationPartitionSpec->BlockedOutputStreams = blockedStreamComputer->GetBlockedStreams(sourceKey);
                return ConvertTo<IMapNodePtr>(dynamicComputationPartitionSpec);
            };

            for (const auto& [partitionId, key] : grouped.KeyPartitions) {
                if (auto it = expectedKeys->find(key); it != expectedKeys->end()) {
                    UpdateDynamicPartitionSpec(flowView, partitionId, makeDynamicPartitionSpec(key, it->second));
                    expectedKeys->erase(it);
                    continue;
                }
                auto partition = GetOrCrash(flowView->State->ExecutionSpec->Layout->Partitions, partitionId);
                if (partition->State == EPartitionState::Completed) {
                    YT_TLOG_EVENT_FLUENT(GetContext()->PublicLogger, NLogging::ELogLevel::Info, "Removing completed source partition")
                        .With("PartitionId", partitionId)
                        .With("Partition", NYson::ConvertToYsonString(partition, EYsonFormat::Text));
                    flowView->State->ExecutionSpec->Layout->RemovePartition(partitionId);
                } else {
                    // The vanished key gets no spec from the expected-keys branch, and the ephemeral
                    // dynamic partition specs do not survive a controller restart. Without one the
                    // worker never starts the retirement job - StartJob is driven by the dynamic spec
                    // map - and the partition can never complete.
                    UpdateDynamicPartitionSpec(
                        flowView,
                        partitionId,
                        makeDynamicPartitionSpec(key, NYTree::GetEphemeralNodeFactory()->CreateMap()));
                    if (partition->State == EPartitionState::Executing) {
                        // The source dropped this key from ListKeys(): retire the partition for good by
                        // completing it (erases its source-key state) instead of interrupting it (which
                        // would preserve the offset for a recreation that will never come).
                        CompletePartition(flowView, partitionId);
                    } else {
                        // The partition is already completing; leave it to finish. It cannot be interrupting:
                        // GroupPartitions feeds this loop only Executing/Completing/Completed source partitions.
                        YT_VERIFY(partition->State == EPartitionState::Completing);
                    }
                }
            }

            for (const auto& [key, dynamicSourcePartitionSpec] : *expectedKeys) {
                CreateSourcePartition(flowView, key, makeDynamicPartitionSpec(key, dynamicSourcePartitionSpec));
            }
        }
    } else {
        THROW_ERROR_EXCEPTION("Computation with no inputs and no sources is not supported");
    }
}

THashMap<std::string, std::vector<TNodeTraverseDataPtr>> TUniversalComputationController::GetNodesByAvailabilityGroup(
    const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
    const TFlowViewPtr& flowView)
{
    THashMap<std::string, std::vector<TNodeTraverseDataPtr>> grouped;
    for (const auto& [partitionId, node] : traverseData) {
        const auto partition = GetOrCrash(flowView->State->ExecutionSpec->Layout->Partitions, partitionId);
        if (partition->SourceKey) {
            auto [streamId, sourceKey] = SplitUniversalPartitionKey(*partition->SourceKey);
            // Source stream renamed in spec - the partition's encoded streamId no longer maps to a live
            // source controller. Skip; orphan partitions are eventually pruned by InterruptPartition path
            // in DoPartitioning.
            if (!Sources_.contains(streamId)) {
                continue;
            }
            grouped[Format("%v-%v", streamId, GetOrCrash(Sources_, streamId)->GetGroup(sourceKey))].push_back(node);
        } else {
            grouped["input-default"].push_back(node);
        }
    }
    return grouped;
}

std::optional<TNodeTraverseDataPtr> TUniversalComputationController::GetFuturePartitionsNodeTraverseData(
    const TFlowViewPtr& flowView)
{
    auto node = New<TNodeTraverseData>();
    node->ReportTime = flowView->State->CurrentTimestamp;

    bool hasNotTrivialTraverse = false;
    for (const auto& [streamId, source] : Sources_) {
        auto traverse = source->GetFutureKeysStreamTraverseData();
        if (traverse.has_value()) {
            auto traverseCopy = CloneYsonStruct(*traverse);
            traverseCopy->Epoch = flowView->State->ExecutionSpec->GetEpoch();

            hasNotTrivialTraverse = true;


            node->Streams[streamId] = std::move(traverseCopy);
        }
    }
    if (!hasNotTrivialTraverse) {
        return std::nullopt;
    }

    YT_VERIFY(GetSpec()->TimerStreams.size() == 0);
    YT_VERIFY(GetSpec()->InputStreamIds.size() == 0);

    for (const auto& outputStreamId : GetSpec()->OutputStreamIds) {
        EStreamState streamState = EStreamState::Completed;
        TSystemTimestamp eventTimestamp = flowView->State->CurrentTimestamp;
        for (const auto& sourceStreamId : GetOrCrash(GetSpec()->StreamsDependency, outputStreamId)) {
            auto sourceTraverseIt = node->Streams.find(sourceStreamId);
            if (sourceTraverseIt == node->Streams.end()) {
                continue;
            }
            streamState = std::min(streamState, sourceTraverseIt->second->State);
            eventTimestamp = std::min(eventTimestamp, sourceTraverseIt->second->EventWatermark);
        }
        auto outputStreamTraverse = New<TStreamTraverseData>();
        outputStreamTraverse->Epoch = flowView->State->ExecutionSpec->GetEpoch();
        outputStreamTraverse->State = streamState;
        outputStreamTraverse->SystemWatermark = flowView->State->CurrentTimestamp;
        outputStreamTraverse->EventWatermark = eventTimestamp;
        node->Streams[outputStreamId] = std::move(outputStreamTraverse);
    }

    return node;
}

THashMap<TStreamId, ISourceControllerPtr> TUniversalComputationController::CreateSources(
    const TComputationControllerContextPtr& context,
    const TComputationSpecPtr& spec,
    const TDynamicComputationSpecPtr& dynamicSpec)
{
    THashMap<TStreamId, ISourceControllerPtr> sources;
    for (const auto& [streamId, sourceSpec] : spec->SourceStreams) {
        auto dynamicSourceSpec = GetOrDefault(dynamicSpec->SourceStreams, streamId, New<TDynamicSourceSpec>());
        auto sourceContext = New<TSourceControllerContext>();
        static_cast<TComputationControllerContextBase&>(*sourceContext) = *context;
        sourceContext->SourceStreamId = streamId;
        sourceContext->SourceSpec = sourceSpec;
        sourceContext->Profiler = sourceContext->Profiler.WithPrefix("/source").WithTag("stream_id", streamId.Underlying());
        sourceContext->StatusProfiler = sourceContext->StatusProfiler->WithPrefix(Format("/sources/%v", streamId));
        sourceContext->Logger = sourceContext->Logger.WithTag("SourceStreamId: %v", streamId.Underlying());
        auto dynamicSourceContext = New<TDynamicSourceControllerContext>();
        dynamicSourceContext->DynamicSourceSpec = dynamicSourceSpec;
        sources[streamId] = TRegistry::Get()->CreateSourceController(sourceContext, dynamicSourceContext);
    }
    return sources;
}

THashMap<TSinkId, ISinkControllerPtr> TUniversalComputationController::CreateSinks(
    const TComputationControllerContextPtr& context,
    const TComputationSpecPtr& spec,
    const TDynamicComputationSpecPtr& dynamicSpec)
{
    THashMap<TSinkId, ISinkControllerPtr> sinks;
    for (const auto& [sinkId, sinkSpec] : spec->Sinks) {
        auto dynamicSinkSpec = GetOrDefault(dynamicSpec->Sinks, sinkId, New<TDynamicSinkSpec>());
        auto sinkContext = New<TSinkControllerContext>();
        static_cast<TComputationControllerContextBase&>(*sinkContext) = *context;
        sinkContext->SinkId = sinkId;
        sinkContext->SinkSpec = sinkSpec;
        sinkContext->Profiler = sinkContext->Profiler.WithPrefix("/sink").WithTag("sink_id", sinkId.Underlying());
        sinkContext->StatusProfiler = sinkContext->StatusProfiler->WithPrefix(Format("/sinks/%v", sinkId));
        sinkContext->Logger = sinkContext->Logger.WithTag("SinkId: %v", sinkId.Underlying());
        auto dynamicSinkContext = New<TDynamicSinkControllerContext>();
        dynamicSinkContext->DynamicSinkSpec = dynamicSinkSpec;
        sinks[sinkId] = TRegistry::Get()->CreateSinkController(sinkContext, dynamicSinkContext);
    }
    return sinks;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Simple helper to calculate average value.
template <class T>
struct TAverage
{
    TAverage(double limit, const char* name)
        : Limit(limit)
        , Name(name)
    { }

    void Add(const TPartitionId& partitionId, const T& value)
    {
        Value += value;
        ++Count;
        Weights.emplace(partitionId, value);
    }

    T Get() const
    {
        return Value / (Count ? Count : 1);
    }

    T Limit;
    const char* Name;
    T Value = {};
    ssize_t Count = 0;
    THashMap<TPartitionId, T> Weights;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationController::InputAutoPartitioningCollectData(TInputAutoPartitioningContext& context) const
{
    YT_VERIFY(UsesRangePartitioning());

    context.AllPartitionsHaveStatuses = true;
    context.AllPartitionsHavePivots = true;
    const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
    for (const auto& [partitionId, keyRange] : context.PartitionRanges) {
        auto it = partitionStatuses.find(partitionId);
        if (it == partitionStatuses.end() || !it->second->CurrentJobStatus) {
            context.AllPartitionsHaveStatuses = false;
            context.AllPartitionsHavePivots = false;
            break;
        }
        if (it->second->CurrentJobStatus->InputMetrics->Global.Pivots.empty()) {
            context.AllPartitionsHavePivots = false;
        }
    }

    const auto& pipelineState = context.FlowView->State->ExecutionSpec->PipelineState;
    if (pipelineState->GetValue() != EPipelineState::Working) {
        context.NormalFlightDuration = TDuration::Zero();
        YT_TLOG_INFO("Partitioning: wait for working state")
            .With("PipelineState", pipelineState->GetValue())
            .With("LastRepartitionTime", LastRepartitionTime_)
            .With("PipelineStateUpdate", pipelineState->GetLastUpdate())
            .With("NormalFlightDuration", context.NormalFlightDuration);
    } else {
        TInstant baseInstant = std::max(LastRepartitionTime_, pipelineState->GetLastUpdate());
        context.AnotherComputationRecentlyRepartitioned = GetContext()->CommonContext->LastRepartitioningInstant > LastCommonRepartitioningInstant_;
        if (!context.AnotherComputationRecentlyRepartitioned) {
            baseInstant = std::max(baseInstant, GetContext()->CommonContext->LastRepartitioningInstant);
        }
        context.NormalFlightDuration = TInstant::Now() - baseInstant;
        YT_TLOG_INFO("Partitioning: calculating normal flight duration")
            .With("PipelineState", pipelineState->GetValue())
            .With("LastRepartitionTime", LastRepartitionTime_)
            .With("PipelineStateUpdate", pipelineState->GetLastUpdate())
            .With("NormalFlightDuration", context.NormalFlightDuration);
    }
}

double TUniversalComputationController::ApplyPeakHoldRelease(
    double previous,
    double target,
    TDuration elapsed,
    TDuration releaseHalfDelay)
{
    if (target >= previous || releaseHalfDelay == TDuration::Zero()) {
        // Instant attack (growth) or smoothing disabled.
        return target;
    }
    // Exponential release towards the (lower) target: the remaining gap halves every half-delay.
    double alpha = 1.0 - std::exp2(-elapsed.SecondsFloat() / releaseHalfDelay.SecondsFloat());
    return previous + alpha * (target - previous);
}

void TUniversalComputationController::InputAutoPartitioningCalculateOptimalCount(TInputAutoPartitioningContext& context)
{
    YT_VERIFY(UsesRangePartitioning());

    auto dynamicParameters = GetDynamicParameters();
    i64 minInputPartitionCount = dynamicParameters->MinPartitionCount.value_or(DefaultMinInputPartitionCount);
    i64 maxInputPartitionCount = dynamicParameters->MaxPartitionCount.value_or(DefaultMaxInputPartitionCount);
    if (maxInputPartitionCount < minInputPartitionCount) {
        if (dynamicParameters->MinPartitionCount && !dynamicParameters->MaxPartitionCount) {
            maxInputPartitionCount = minInputPartitionCount;
        } else {
            YT_ASSERT(!dynamicParameters->MinPartitionCount && dynamicParameters->MaxPartitionCount);
            minInputPartitionCount = maxInputPartitionCount;
        }
    }
    i64 sinkChannelMultiplier = dynamicParameters->SinkChannelMultiplier.value_or(DefaultSinkChannelMultiplier);
    double desiredAveragePartitionCpuLoad = dynamicParameters->DesiredAveragePartitionCpuLoad.value_or(DefaultDesiredAveragePartitionCpuLoad);
    double desiredAveragePartitionMemoryUsed = dynamicParameters->DesiredAveragePartitionMemoryUsed.value_or(DefaultDesiredAveragePartitionMemoryUsed);
    double desiredAveragePartitionMessagesPerSecond = dynamicParameters->DesiredAveragePartitionMessagesPerSecond.value_or(DefaultDesiredAveragePartitionMessagesPerSecond);
    double desiredAveragePartitionBytesPerSecond = dynamicParameters->DesiredAveragePartitionBytesPerSecond.value_or(DefaultDesiredAveragePartitionBytesPerSecond);
    double desiredAveragePartitionTimerCount = dynamicParameters->DesiredAveragePartitionTimerCount.value_or(DefaultDesiredAveragePartitionTimerCount);
    double allowedPartitionCountDeviation = dynamicParameters->AllowedPartitionCountDeviation.value_or(DefaultAllowedPartitionCountDeviation);
    TDuration partitionCountDoubleDelay = dynamicParameters->PartitionCountDoubleDelay.value_or(DefaultPartitionCountDoubleDelay);
    TDuration partitionCountHalfDelay = dynamicParameters->PartitionCountHalfDelay.value_or(DefaultPartitionCountHalfDelay);

    i64 currentPartitionCount = std::ssize(context.PartitionRanges);
    i64 workerCount = std::ssize(context.FlowView->State->Workers);

    // Use different criteria and find out proposed partition count for each criteria if applicable.
    struct TProposition
    {
        i64 Count;
        std::string_view Criteria;
        THashMap<TPartitionId, double> Weights;
    };

    std::vector<TProposition> suggestions;

    // Sink channels criteria.
    if (!Sinks_.empty()) {
        TSinkId widestStreamId;
        std::optional<i64> channels;
        for (const auto& [sinkId, sinkController] : Sinks_) {
            auto sinkChannels = sinkController->GetReceiverChannelCount();
            if (sinkChannels) {
                // Per-sink counts drive producer-id regeneration (a change for any sink matters).
                context.SinkChannelCounts[sinkId] = *sinkChannels;
            }
            if (!channels || sinkChannels > *channels) {
                widestStreamId = sinkId;
                channels = sinkChannels;
            }
        }
        if (channels) {
            i64 count = *channels * sinkChannelMultiplier;
            count = std::max(count, workerCount);
            suggestions.emplace_back(count, "sink channels");
            YT_TLOG_INFO("Partitioning: collected sink channels")
                .With("SinkId", widestStreamId)
                .With("ChannelCount", *channels)
                .With("ProposedPartitionCount", count);
        }
    }

    // Proposal based on current partitions' metrics.
    if (!context.PartitionRanges.empty()) {
        const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
        TAverage<double> averageCpuUsage(desiredAveragePartitionCpuLoad, "average CPU load");
        TAverage<double> averageMemoryUsage(desiredAveragePartitionMemoryUsed, "average memory usage");
        TAverage<double> averageMessagesPerSecond(desiredAveragePartitionMessagesPerSecond, "average messages per second");
        TAverage<double> averageBytesPerSecond(desiredAveragePartitionBytesPerSecond, "average bytes per second");
        TAverage<double> averageTimerCount(desiredAveragePartitionTimerCount, "average timer count");
        for (const auto& [partitionId, keyRange] : context.PartitionRanges) {
            auto it = partitionStatuses.find(partitionId);
            if (it == partitionStatuses.end() || !it->second->CurrentJobStatus) {
                continue;
            }
            const auto& status = it->second->CurrentJobStatus;
            if (status->PerformanceMetrics && (status->PerformanceMetrics->CpuUsage10m || status->PerformanceMetrics->CpuUsage30s)) {
                averageCpuUsage.Add(partitionId, status->PerformanceMetrics->CpuUsage10m ? *status->PerformanceMetrics->CpuUsage10m : *status->PerformanceMetrics->CpuUsage30s);
            }
            if (status->PerformanceMetrics && status->PerformanceMetrics->MemoryUsage10m) {
                averageMemoryUsage.Add(partitionId, status->PerformanceMetrics->MemoryUsage10m);
            }
            if (status->InputMetrics) {
                averageMessagesPerSecond.Add(partitionId, status->InputMetrics->Global.MessagesPerSecond);
                averageBytesPerSecond.Add(partitionId, status->InputMetrics->Global.BytesPerSecond);
            }
            if (status->FromPartitionTraverseData && status->FromPartitionTraverseData->Node) {
                ssize_t timerCount = 0;
                const auto& nodeStreams = status->FromPartitionTraverseData->Node->Streams;
                for (const auto& [streamId, stream] : GetSpec()->TimerStreams) {
                    auto it = nodeStreams.find(streamId);
                    if (it != nodeStreams.end() && it->second->InflightMetrics) {
                        timerCount += it->second->InflightMetrics->Count;
                    }
                }
                averageTimerCount.Add(partitionId, timerCount);
            }
        }
        YT_TLOG_INFO("Partitioning: collected metrics")
            .With("AverageCpuUsage", averageCpuUsage.Get())
            .With("AverageMemoryUsage", averageMemoryUsage.Get())
            .With("AverageMessagesPerSecond", averageMessagesPerSecond.Get())
            .With("AverageBytesPerSecond", averageBytesPerSecond.Get())
            .With("AverageTimerCount", averageTimerCount.Get());

        ssize_t minStatusCount = static_cast<ssize_t>(currentPartitionCount / allowedPartitionCountDeviation);
        TInstant now = TInstant::Now();
        TStringBuilder smoothingReport;
        for (auto* averageMetric : {&averageCpuUsage, &averageMemoryUsage, &averageMessagesPerSecond, &averageBytesPerSecond, &averageTimerCount}) {
            if (averageMetric->Limit <= 0 || averageMetric->Count < minStatusCount) {
                continue;
            }
            double ratio = averageMetric->Get() / averageMetric->Limit;
            double proposedByCriterion = std::round(currentPartitionCount * ratio);
            // Peak-hold smoothing per criterion: the proposed count may grow instantly but shrinks
            // only with the partition_count_half_delay release half-delay, so a transient metric dip
            // (e.g. a nightly CPU-load drop) does not quickly shrink partitions and leave the
            // computation short when load returns (YTFLOWSUPPORT-113).
            std::string criterion(averageMetric->Name);
            auto [it, inserted] = CriterionProposedCountEma_.emplace(criterion, TCriterionEmaState{proposedByCriterion, now});
            if (!inserted) {
                it->second.ProposedCount = ApplyPeakHoldRelease(it->second.ProposedCount, proposedByCriterion, now - it->second.UpdatedAt, partitionCountHalfDelay);
                it->second.UpdatedAt = now;
            }
            if (smoothingReport.GetLength() > 0) {
                smoothingReport.AppendString("; ");
            }
            smoothingReport.AppendFormat("%v: %v -> %v", criterion, proposedByCriterion, it->second.ProposedCount);
            suggestions.emplace_back(static_cast<ssize_t>(std::llround(it->second.ProposedCount)), averageMetric->Name, std::move(averageMetric->Weights));
        }
        if (smoothingReport.GetLength() > 0) {
            YT_TLOG_INFO("Partitioning: peak-hold smoothed criterion proposals")
                .With("RawToSmoothed", smoothingReport.Flush());
        }
        for (auto& suggestion : suggestions) {
            if (suggestion.Count < std::ssize(context.PartitionRanges)) {
                // If it suggests to decrease number of partitions, keep at least triple reserve for the case of future increase.
                suggestion.Count = std::min(suggestion.Count * 3, std::ssize(context.PartitionRanges));
            }
        }
    }

    // Make weights to have values for all partitions in rangePartitions.
    auto finalizeWeights = [&] () {
        double sum = 0.;
        for (const auto& [partitionId, weight] : context.Weights) {
            sum += weight;
        }
        double average = std::ssize(context.Weights) == 0 ? 1. : sum / std::ssize(context.Weights);
        for (const auto& [partitionId, range] : context.PartitionRanges) {
            if (!context.Weights.contains(partitionId)) {
                context.Weights[partitionId] = average;
            }
        }
    };

    // Strict result, recreate partitions if the proposed count now equal to current.
    auto strictResult = [&] (i64 proposedCount, THashMap<TPartitionId, double>&& weights) {
        context.ProposedCount = proposedCount;
        context.RecreateNow = proposedCount != currentPartitionCount;
        context.Weights = std::move(weights);
        finalizeWeights();
    };

    // Smart detection whether we should recreate right now for non-strict result.
    auto shouldRecreate = [&] (i64 proposedCount) {
        if (currentPartitionCount < minInputPartitionCount || currentPartitionCount > maxInputPartitionCount) {
            return true;
        }
        if (proposedCount <= currentPartitionCount * allowedPartitionCountDeviation &&
            proposedCount >= currentPartitionCount / allowedPartitionCountDeviation) {
            return false;
        }
        if (!context.AllPartitionsHaveStatuses || context.NormalFlightDuration == TDuration::Zero()) {
            return false;
        }
        TDuration delay;
        if (proposedCount > currentPartitionCount) {
            double proposedMultiple = static_cast<double>(proposedCount) / static_cast<double>(currentPartitionCount);
            TDuration referenceDelay = partitionCountDoubleDelay;
            delay = std::max(referenceDelay / log(proposedMultiple) * log(2), referenceDelay / 2);
            if (context.AnotherComputationRecentlyRepartitioned) {
                // Decrease delay (but not more than by half) if some other computation was repartitioned recently.
                // That would create a tension that would force all computations to repartition at the same time.
                // That in turn would allow to avoid several sequential delays causes by sequential repartitioning
                //  of different computation.
                TDuration timeSinceAnyRepartitioning = TInstant::Now() - GetContext()->CommonContext->LastRepartitioningInstant;
                TDuration crossComputationAffectDuration = std::min(referenceDelay, delay) / 2;
                if (timeSinceAnyRepartitioning < crossComputationAffectDuration) {
                    // Approximate linearly from 0.5 to 1.
                    double coefficient = 0.5 + 0.5 * (timeSinceAnyRepartitioning / crossComputationAffectDuration);
                    delay *= coefficient;
                    YT_TLOG_INFO("Partitioning: delay decreased due to cross computation affect")
                        .With("Coefficient", coefficient)
                        .With("NewDelay", delay.Seconds());
                }
            }
        } else {
            double proposedMultiple = static_cast<double>(currentPartitionCount) / static_cast<double>(proposedCount);
            TDuration referenceDelay = partitionCountHalfDelay;
            delay = std::max(referenceDelay / log(proposedMultiple) * log(2), referenceDelay / 2);
        }
        return context.NormalFlightDuration > delay;
    };

    // If there's no suggestions, use reasonable minumum.
    if (suggestions.empty()) {
        if (dynamicParameters->DesiredPartitionCount) {
            return strictResult(*dynamicParameters->DesiredPartitionCount, {});
        }
        context.ProposedCount = std::clamp(workerCount, minInputPartitionCount, maxInputPartitionCount);
        context.RecreateNow = shouldRecreate(context.ProposedCount);
        finalizeWeights();
        YT_TLOG_INFO("Partitioning: no suggestions for optimal partition count, use reasonable minimum")
            .With("ProposedCount", context.ProposedCount)
            .With("RecreateNow", context.RecreateNow)
            .With("CurrentCount", currentPartitionCount)
            .With("Criteria", workerCount ? "minimum" : "worker count")
            .With("NormalFlightDuration", context.NormalFlightDuration.Seconds());
        return;
    }

    // Find the most strict proposition and apply limits.
    ssize_t kMax = 0;
    for (ssize_t k = 1; k < std::ssize(suggestions); k++) {
        if (suggestions[k].Count > suggestions[kMax].Count) {
            kMax = k;
        }
    }
    if (dynamicParameters->DesiredPartitionCount) {
        return strictResult(*dynamicParameters->DesiredPartitionCount, std::move(suggestions[kMax].Weights));
    }

    context.ProposedCount = suggestions[kMax].Count;
    context.ProposedCount = std::clamp(context.ProposedCount, minInputPartitionCount, maxInputPartitionCount);
    // We should not decrease partition count significantly at once.
    if (context.ProposedCount < currentPartitionCount / 2) {
        context.ProposedCount = std::clamp(currentPartitionCount / 2, minInputPartitionCount, maxInputPartitionCount);
    }
    context.RecreateNow = shouldRecreate(context.ProposedCount);
    context.Weights = std::move(suggestions[kMax].Weights);
    finalizeWeights();

    YT_TLOG_INFO("Partitioning: calculated optimal partition count")
        .With("ProposedCount", context.ProposedCount)
        .With("RecreateNow", context.RecreateNow)
        .With("CurrentCount", currentPartitionCount)
        .With("Criteria", suggestions[kMax].Criteria)
        .With("NormalFlightDuration", context.NormalFlightDuration.Seconds());
}

void TUniversalComputationController::InputAutoPartitioningBuildRanges(TInputAutoPartitioningContext& context) const
{
    if (context.ProposedCount == 0) {
        return;
    }

    const auto& firstColumn = GetSpec()->GroupBySchema->Columns()[0];
    const auto uintType = NTableClient::SimpleLogicalType(NTableClient::ESimpleLogicalValueType::Uint64);
    const bool isUint = *firstColumn.LogicalType() == *uintType;

    if (!GetSpec()->ExperimentalEnableNonUintKey.value_or(TComputationSpec::ExperimentalEnableNonUintKeyDefault)) {
        YT_VERIFY(isUint);
        context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        return;
    }

    if (context.PartitionRanges.empty()) {
        if (isUint) {
            context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        } else {
            context.NewRanges = {UniversalKeyRange()};
        }
        // Dont't fill NewWeights since they are only used if PartitionRanges is not empty.
        return;
    }

    // Convert to vector and sort oldPartitionRanges.
    std::vector<std::pair<TPartitionId, TKeyRange>> oldSortedRanges(context.PartitionRanges.begin(), context.PartitionRanges.end());
    Sort(oldSortedRanges, [] (const auto& lhs, const auto& rhs) {
        return std::tie(lhs.second, lhs.first) < std::tie(rhs.second, rhs.first);
    });

    // Transform ranges with their subranges into plain list of (sub) ranges.
    struct TWeightedRange
        : TKeyRange
    {
        double Weight;
    };

    std::vector<TWeightedRange> splittedRanges;
    const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
    for (const auto& [partitionId, range] : oldSortedRanges) {
        double weight = context.Weights.at(partitionId);
        auto it = partitionStatuses.find(partitionId);
        if (it == partitionStatuses.end() || !it->second->CurrentJobStatus || !it->second->CurrentJobStatus->InputMetrics ||
            it->second->CurrentJobStatus->InputMetrics->Global.Pivots.empty()) {
            splittedRanges.push_back({range, weight});
            continue;
        }
        const std::vector<TKey>& splitters = it->second->CurrentJobStatus->InputMetrics->Global.Pivots;
        double subWeight = weight / (std::ssize(splitters) + 1);
        splittedRanges.push_back({{range.Lower, splitters.front()}, subWeight});
        weight -= subWeight;
        for (ssize_t i = 0; i < std::ssize(splitters) - 1; ++i) {
            splittedRanges.push_back({{splitters[i], splitters[i + 1]}, subWeight});
            weight -= subWeight;
        }
        splittedRanges.push_back({{splitters.back(), range.Upper}, weight});
    }

    // Join/split ranges to form required number of ranges.
    context.NewRanges.reserve(context.ProposedCount);
    context.NewWeights.reserve(context.ProposedCount);
    double remainingWeight = Accumulate(context.Weights | std::views::elements<1>, 0.);
    auto remainingCount = context.ProposedCount;
    ssize_t i = 0;
    while (remainingCount > 0 && i < std::ssize(splittedRanges)) {
        double requiredWeight = remainingWeight / remainingCount;
        double consumedWeight = splittedRanges[i].Weight;
        ssize_t j = i + 1;
        while (consumedWeight < requiredWeight && j < std::ssize(splittedRanges)) {
            consumedWeight += splittedRanges[j].Weight;
            j++;
        }
        if (consumedWeight > requiredWeight && j > i + 1) {
            if (consumedWeight - requiredWeight > requiredWeight - consumedWeight - splittedRanges[j - 1].Weight) {
                j--;
                consumedWeight -= splittedRanges[j].Weight;
            }
        }

        context.NewRanges.push_back({splittedRanges[i].Lower, splittedRanges[j - 1].Upper});
        context.NewWeights.push_back(consumedWeight);
        --remainingCount;
        remainingWeight -= consumedWeight;
        i = j;
    }
    context.NewRanges.back().Upper = splittedRanges.back().Upper;
    if ((std::ssize(context.NewRanges) != context.ProposedCount || !context.AllPartitionsHavePivots)) {
        if (isUint) {
            YT_TLOG_INFO("Partitioning: fall back to uniform uint split")
                .With("ResultSize", std::ssize(context.NewRanges))
                .With("PartitionCount", context.ProposedCount)
                .With("AllPartitionsHaveSplitters", context.AllPartitionsHavePivots)
                .With("SubrangeCount", std::ssize(splittedRanges));
            context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        } else {
            YT_TLOG_INFO("Partitioning: cannot recreate now, must wait for partition pivots")
                .With("ResultSize", std::ssize(context.NewRanges))
                .With("PartitionCount", context.ProposedCount)
                .With("AllPartitionsHaveSplitters", context.AllPartitionsHavePivots)
                .With("SubrangeCount", std::ssize(splittedRanges));
            context.RecreateNow = false;
        }
        // Calculated NewWeights are invalid. Clear the vector to yield this fact.
        context.NewWeights.clear();
    }
}

void TUniversalComputationController::InputAutoPartitioningTryRebalance(TInputAutoPartitioningContext& context) const
{
    double allowedPartitionCountDeviation = GetDynamicParameters()->AllowedPartitionCountDeviation.value_or(DefaultAllowedPartitionCountDeviation);
    if (!context.RecreateNow && context.AllPartitionsHavePivots && !context.NewWeights.empty() &&
        std::ssize(context.PartitionRanges) > 0 && context.NormalFlightDuration != TDuration::Zero() &&
        context.ProposedCount <= std::ssize(context.PartitionRanges) * allowedPartitionCountDeviation &&
        context.ProposedCount >= std::ssize(context.PartitionRanges) / allowedPartitionCountDeviation) {
        // Check whether the new range distribution is significantly more uniform than the current.
        auto getDeviation = [&] (const auto& weights) {
            double average = Accumulate(weights, 0.0) / std::ssize(weights);
            double deviation = Accumulate(weights, 0.0, [&] (double sum, double weight) {
                return sum + std::pow(weight - average, 2);
            });
            deviation = std::sqrt(deviation / std::ssize(weights));
            return deviation;
        };
        auto oldDeviation = getDeviation(context.Weights | std::views::elements<1>);
        auto newDeviation = getDeviation(context.NewWeights);
        newDeviation += 1e-5; // Add tiny value to make stronger comparison below and also to avoid zero denominator.
        if (newDeviation < oldDeviation) {
            double multiple = oldDeviation / newDeviation;
            auto partitionCountDoubleDelay = GetDynamicParameters()->PartitionCountDoubleDelay.value_or(DefaultPartitionCountDoubleDelay);
            auto delay = std::max(partitionCountDoubleDelay / log(multiple) * log(2), partitionCountDoubleDelay);
            if (context.NormalFlightDuration > delay) {
                context.RecreateNow = true;
                YT_TLOG_INFO("Partitioning: decided to rebuild partitions to make more uniform distribution")
                    .With("OldDeviation", oldDeviation)
                    .With("NewDeviation", newDeviation);
            }
        }
    }
}

std::optional<THashMap<TKey, NYTree::IMapNodePtr>> TUniversalComputationController::GetSourcePartitionKeys() const
{
    THashMap<TKey, NYTree::IMapNodePtr> keys;
    for (const auto& [streamId, source] : Sources_) {
        auto sourceKeys = source->ListKeys();
        if (!sourceKeys) {
            YT_TLOG_WARNING("No info about source keys for stream")
                .With("Stream", streamId);
            return std::nullopt;
        }
        for (const auto& [key, dynamicSourcePartitionSpec] : *sourceKeys) {
            keys[MakeUniversalPartitionKey(streamId, key)] = dynamicSourcePartitionSpec;
        }
    }
    return keys;
}

void TUniversalComputationController::ProcessSourcePartitionStatuses(
    const THashMap<TPartitionId, TKey>& keyPartitions,
    const TFlowViewPtr& flowView)
{
    THashMap<TStreamId, THashMap<TKey, TExtendedSourcePartitionStatusPtr>> statuses;
    for (const auto& [partitionId, key] : keyPartitions) {
        if (auto partitionJobStatus = flowView->Feedback->PartitionJobStatuses.FindPtr(partitionId)) {
            auto [streamId, sourceKey] = SplitUniversalPartitionKey(key);
            // Source stream renamed in spec - the partition's encoded streamId no longer maps to a live
            // source controller. Skip; the partition will be interrupted by the source-key matching loop
            // in DoPartitioning (it is not in expectedKeys for the new spec).
            if (!Sources_.contains(streamId)) {
                continue;
            }
            auto extendedStatus = New<TExtendedSourcePartitionStatus>();

            const auto lastPartitionStatus = ConvertTo<TUniversalComputationPartitionStatusPtr>((*partitionJobStatus)->LastPartitionStatus);
            if (lastPartitionStatus && lastPartitionStatus->ActiveSourceStatus.has_value()) {
                extendedStatus->PartitionStatus = *lastPartitionStatus->ActiveSourceStatus;
            }

            extendedStatus->PartitionState = GetOrCrash(flowView->State->ExecutionSpec->Layout->Partitions, partitionId)->State;
            statuses[streamId][sourceKey] = std::move(extendedStatus);
        }
    }
    for (const auto& [streamId, streamStatuses] : statuses) {
        GetOrCrash(Sources_, streamId)->ProcessPartitionStatuses(streamStatuses);
    }
}

TUniversalComputationController::TGroupedPartitions TUniversalComputationController::GroupPartitions(
    const std::vector<TPartitionId>& computationPartitions,
    const TFlowViewPtr& flowView) const
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;

    static const THashSet<EPartitionState> executingStates = {EPartitionState::Executing, EPartitionState::Completing, EPartitionState::Completed};

    TGroupedPartitions grouped;
    for (const auto& partitionId : computationPartitions) {
        auto partition = GetOrCrash(layout->Partitions, partitionId);
        if (executingStates.contains(partition->State)) {
            if (partition->LowerKey && partition->UpperKey) {
                grouped.RangePartitions[partitionId] = {*partition->LowerKey, *partition->UpperKey};
            } else if (partition->SourceKey) {
                grouped.KeyPartitions[partitionId] = *partition->SourceKey;
            } else {
                grouped.BadPartitions.insert(partitionId);
            }
        } else if (partition->State == EPartitionState::Interrupting) {
            grouped.InterruptingPartitions.insert(partitionId);
        }
    }
    return grouped;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
