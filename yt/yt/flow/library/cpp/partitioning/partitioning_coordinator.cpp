#include "partitioning_coordinator.h"

#include "partitioning_helpers.h"

#include <yt/yt/flow/library/cpp/common/source_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NPartitioning {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TComputationPartitioningState::Register(TRegistrar registrar)
{
    registrar.Parameter("last_applied_sink_topology_version", &TThis::LastAppliedSinkTopologyVersion)
        .Default();
    registrar.Parameter("retiring_source_partitions", &TThis::RetiringSourcePartitions)
        .Default();
    registrar.Parameter("throughput_autoscaler", &TThis::ThroughputAutoscaler)
        .Default();
    registrar.Parameter("throughput_demand_history", &TThis::ThroughputDemandHistory)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitioningCoordinatorState::Register(TRegistrar registrar)
{
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TPartitioningCoordinator::TPartitioningCoordinator(
    const THashMap<TComputationId, IComputationControllerPtr>& computationControllers,
    TPipelineSpecPtr pipelineSpec,
    IInitContextPtr initContext,
    NLogging::TLogger logger,
    NLogging::TLogger publicLogger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler)
    : Logger(std::move(logger))
    , PublicLogger(std::move(publicLogger))
{
    initContext->InitClient(PersistedState_, "v1");
    EraseNodesIf(PersistedState_->Computations, [&] (const auto& item) {
        return !computationControllers.contains(item.first);
    });

    ComputationOrder_.reserve(computationControllers.size());
    for (const auto& [computationId, controller] : computationControllers) {
        TComputationState state;
        state.Controller = controller;
        state.Spec = GetOrCrash(pipelineSpec->Computations, computationId);
        state.Logger = Logger.WithTag("ComputationId", computationId);
        state.PublicLogger = PublicLogger.WithTag("ComputationId", computationId);
        state.Profiler = profiler.WithTag("computation_id", computationId.Underlying());
        state.ProposedPartitionCountGauge = state.Profiler.Gauge("/proposed_partition_count");
        auto [it, _] = PersistedState_->Computations.emplace(
            computationId,
            New<TComputationPartitioningState>());
        state.PersistedState = it->second;
        ThroughputAutoscaler_.InitializeComputation(
            computationId,
            state.PersistedState->ThroughputAutoscaler,
            state.PersistedState->ThroughputDemandHistory);
        if (statusProfiler) {
            state.ThroughputErrorState = statusProfiler->ErrorState(Format("/%v/throughput", computationId));
        }
        EmplaceOrCrash(Computations_, computationId, std::move(state));
        ComputationOrder_.push_back(computationId);
    }
}

void TPartitioningCoordinator::BeginIteration(const TFlowViewPtr& committedFlowView)
{
    PendingPartitionChanges_.clear();
    ThroughputStatusPrepared_ = false;
    CommittedState_ = committedFlowView->State;
    ThroughputAutoscaler_.BeginIteration();
    for (auto& [_, state] : Computations_) {
        if (state.PreviousRetiringSourcePartitions) {
            state.PersistedState->RetiringSourcePartitions =
                std::move(*state.PreviousRetiringSourcePartitions);
            state.PreviousRetiringSourcePartitions.reset();
        }
        if (state.PendingAppliedSinkTopologyVersion) {
            state.PersistedState->LastAppliedSinkTopologyVersion =
                state.PreviousAppliedSinkTopologyVersion;
            state.PendingAppliedSinkTopologyVersion.reset();
            state.PreviousAppliedSinkTopologyVersion.reset();
        }
    }
}

TPartitioningCoordinator::TGroupedPartitions TPartitioningCoordinator::GroupPartitions(
    const std::vector<TPartitionId>& computationPartitions,
    const TFlowViewPtr& flowView)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    static const THashSet<EPartitionState> executingStates = {
        EPartitionState::Executing,
        EPartitionState::Completing,
        EPartitionState::Completed,
    };

    TGroupedPartitions grouped;
    for (const auto& partitionId : computationPartitions) {
        const auto& partition = GetOrCrash(layout->Partitions, partitionId);
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

bool TPartitioningCoordinator::IsFullCoverage(
    const TComputationId& computationId,
    const std::vector<TPartitionId>& computationPartitions,
    const TFlowViewPtr& flowView)
{
    const auto grouped = GroupPartitions(computationPartitions, flowView);
    if (!grouped.BadPartitions.empty()) {
        return false;
    }

    const auto& controller = GetOrCrash(Computations_, computationId).Controller;
    auto topology = controller->DescribePartitioningTopology();
    if (std::holds_alternative<IComputationController::TPartitioningTopology::TRange>(topology.Value)) {
        return grouped.KeyPartitions.empty() &&
            !TestRangeOverlaps(GetValues(grouped.RangePartitions)) &&
            UniteRanges(GetValues(grouped.RangePartitions)) == std::vector{UniversalKeyRange()};
    }

    const auto& sourceTopology =
        std::get<IComputationController::TPartitioningTopology::TSource>(topology.Value);
    if (!grouped.RangePartitions.empty() || !sourceTopology.ExpectedKeys) {
        return false;
    }
    const auto actualKeys = GetValues(grouped.KeyPartitions);
    const THashSet<TKey> actualKeySet(actualKeys.begin(), actualKeys.end());
    return actualKeys.size() == sourceTopology.ExpectedKeys->size() &&
        actualKeySet == *sourceTopology.ExpectedKeys;
}

void TPartitioningCoordinator::Sync()
{
    for (auto& [computationId, state] : Computations_) {
        state.PersistedState->ThroughputAutoscaler = ThroughputAutoscaler_.GetFeedbackState(computationId);
        state.PersistedState->ThroughputDemandHistory = ThroughputAutoscaler_.SaveDemandHistory(computationId);
    }
}

void TPartitioningCoordinator::Commit()
{
    ThroughputAutoscaler_.Commit();
    for (const auto& [computationId, state] : Computations_) {
        if (ThroughputStatusPrepared_ && state.ThroughputErrorState) {
            state.ThroughputErrorState->SetError(EnableThroughputAutoscaler_
                    ? ThroughputAutoscaler_.CheckComputationStatus(computationId)
                    : TError());
        }
    }
    for (const auto& change : PendingPartitionChanges_) {
        const auto logger = PublicLogger.WithTag("ComputationId", change.ComputationId);
        YT_TLOG_EVENT(logger, NLogging::ELogLevel::Info, "Partitioning: applied partition count change")
            .With("Initiator", change.Initiator)
            .With("Reason", change.Reason)
            .With("PreviousPartitionCount", change.PreviousPartitionCount)
            .With("NewPartitionCount", change.NewPartitionCount);
        auto& state = GetOrCrash(Computations_, change.ComputationId);
        auto [it, inserted] = state.RepartitionCounters.try_emplace(change.Initiator);
        if (inserted) {
            it->second = state.Profiler.WithTag("initiator", change.Initiator).Counter("/repartition_count");
        }
        it->second.Increment();
    }
    PendingPartitionChanges_.clear();
    for (auto& [_, state] : Computations_) {
        state.PendingAppliedSinkTopologyVersion.reset();
        state.PreviousAppliedSinkTopologyVersion.reset();
        state.PreviousRetiringSourcePartitions.reset();
    }
}

void TPartitioningCoordinator::DoPartitioning(const TFlowViewPtr& flowView)
{
    EnableThroughputAutoscaler_ = flowView->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->EnableThroughputAutoscaler;

    const auto& layout = flowView->State->ExecutionSpec->Layout;

    THashMap<TComputationId, std::vector<TPartitionId>> partitions;
    for (const auto& [partitionId, partition] : layout->Partitions) {
        if (Computations_.contains(partition->ComputationId)) {
            partitions[partition->ComputationId].push_back(partitionId);
        } else if (
            partition->State == EPartitionState::Executing ||
            partition->State == EPartitionState::Completing ||
            partition->State == EPartitionState::Interrupting)
        {
            layout->UpdatePartition(
                partitionId,
                EPartitionState::Interrupted,
                flowView->State->ExecutionSpec->GetEpoch(),
                TInstant::Now());
        }
    }

    THashMap<TComputationId, TPreparedComputationPartitioning> prepared;
    TThroughputAutoscaler::TComputations computations;
    for (const auto& computationId : ComputationOrder_) {
        auto grouped = GroupPartitions(partitions[computationId], flowView);
        auto& state = GetOrCrash(Computations_, computationId);
        auto description = state.Controller->DescribePartitioning(
            BuildPartitioningStatus(grouped, flowView));
        if (EnableThroughputAutoscaler_) {
            TThroughputAutoscaler::TComputation computation{
                .Spec = state.Spec,
            };
            if (const auto* range = std::get_if<IComputationController::TPartitioningDescription::TRange>(
                &description.Value))
            {
                computation.PartitioningSpec = range->PartitioningSpec;
            }
            EmplaceOrCrash(computations, computationId, std::move(computation));
        }
        EmplaceOrCrash(
            prepared,
            computationId,
            TPreparedComputationPartitioning{
                .Grouped = std::move(grouped),
                .Description = std::move(description),
            });
    }

    if (EnableThroughputAutoscaler_) {
        YT_VERIFY(CommittedState_);
        const auto& config = flowView->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->Throughput;
        ThroughputAutoscaler_.Reconfigure(std::move(computations), config);
        ThroughputAutoscaler_.Evaluate({
            .Now = flowView->Feedback->UpdateTime,
            .Config = config,
            .Computations = ProcessingMeter_.Collect(
                CommittedState_,
                flowView,
                config,
                ThroughputAutoscaler_.ComputeMeasurementRequests()),
        });
    }

    for (const auto& computationId : ComputationOrder_) {
        auto& state = GetOrCrash(Computations_, computationId);
        if (const auto telemetry = EnableThroughputAutoscaler_
                ? ThroughputAutoscaler_.FindTelemetry(computationId)
                : std::nullopt)
        {
            ProfileThroughput(state, *telemetry);
        } else {
            state.ThroughputSensors.reset();
        }
        DoComputationPartitioning(
            computationId,
            state,
            std::move(GetOrCrash(prepared, computationId)),
            flowView);
    }
    ThroughputStatusPrepared_ = true;
}

void TPartitioningCoordinator::DoComputationPartitioning(
    const TComputationId& computationId,
    TComputationState& state,
    TPreparedComputationPartitioning prepared,
    const TFlowViewPtr& flowView)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& logger = state.Logger;

    auto grouped = std::move(prepared.Grouped);
    auto blockedStreamComputer = New<TBlockedStreamComputer>(flowView, grouped.InterruptingPartitions, logger);

    for (const auto& partitionId : grouped.BadPartitions) {
        InterruptPartition(flowView, partitionId);
    }

    static const auto trivialDynamicPartitionSpec = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& partitionId : grouped.InterruptingPartitions) {
        UpdateDynamicPartitionSpec(flowView, partitionId, trivialDynamicPartitionSpec, logger);
    }

    auto rememberRetiringSourcePartitions = [&] {
        if (!state.PreviousRetiringSourcePartitions) {
            state.PreviousRetiringSourcePartitions =
                state.PersistedState->RetiringSourcePartitions;
        }
    };
    auto eraseRetiringSourcePartition = [&] (const TPartitionId& partitionId) {
        if (state.PersistedState->RetiringSourcePartitions.contains(partitionId)) {
            rememberRetiringSourcePartitions();
            state.PersistedState->RetiringSourcePartitions.erase(partitionId);
        }
    };

    auto description = std::move(prepared.Description);
    if (auto* rangeDescription = std::get_if<IComputationController::TPartitioningDescription::TRange>(
        &description.Value))
    {
        for (const auto& partitionId : GetKeys(grouped.KeyPartitions)) {
            // A source-shaped partition is invalid for range partitioning; interrupt and forget it.
            InterruptPartition(flowView, partitionId);
            eraseRetiringSourcePartition(partitionId);
        }

        auto makeDynamicPartitionSpec = [&] (const TKey& lower, const TKey& upper) {
            auto blockedStreams = blockedStreamComputer->GetBlockedStreams(lower, upper);
            if (blockedStreams.empty()) {
                return trivialDynamicPartitionSpec;
            }
            return BuildRangeDynamicPartitionSpec(std::move(blockedStreams));
        };

        // Rebuild invalid range coverage from scratch. Add every invalid partition to the blocker
        // before interrupting it so its inflight output remains protected by the replacement ranges.
        if (
            TestRangeOverlaps(GetValues(grouped.RangePartitions)) ||
            UniteRanges(GetValues(grouped.RangePartitions)) != std::vector{UniversalKeyRange()})
        {
            for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
                blockedStreamComputer->AddInterruptingPartition(partitionId);
                InterruptPartition(flowView, partitionId);
            }
            grouped.RangePartitions.clear();
        }

        // A new leader starts its cooldown before using fresh job samples.
        if (state.LastRepartitionTime == TInstant::Zero()) {
            state.LastRepartitionTime = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
        }

        TInputAutoPartitioningContext context(flowView, grouped.RangePartitions);
        context.MaxSinkChannelCount = rangeDescription->MaxSinkChannelCount;
        auto parameters = BuildRangePartitioningParameters(
            rangeDescription->PartitioningSpec,
            computationId,
            flowView);
        InputAutoPartitioningCollectData(state, context);
        InputAutoPartitioningCalculateOptimalCount(state, parameters, context);
        InputAutoPartitioningBuildRanges(state, parameters, context);
        InputAutoPartitioningTryRebalance(state, parameters, context);
        state.LastObservedCommonRepartitioningInstant = LastRepartitioningInstant_;

        const bool hasSinkTopologyVersion =
            rangeDescription->SinkTopologyVersion != TVersion(0);
        const bool sinkTopologyChanged =
            hasSinkTopologyVersion &&
            state.PersistedState->LastAppliedSinkTopologyVersion &&
            *state.PersistedState->LastAppliedSinkTopologyVersion !=
                rangeDescription->SinkTopologyVersion;
        const bool initializeSinkTopologyVersion =
            hasSinkTopologyVersion &&
            !state.PersistedState->LastAppliedSinkTopologyVersion &&
            !sinkTopologyChanged;
        if (sinkTopologyChanged) {
            YT_TLOG_EVENT(
                logger,
                NLogging::ELogLevel::Info,
                "Partitioning: sink topology version changed, forcing recreation to regenerate producer ids")
                .With("PreviousVersion", state.PersistedState->LastAppliedSinkTopologyVersion)
                .With("NewVersion", rangeDescription->SinkTopologyVersion);
        }
        // A sink topology change invalidates persisted per-partition producer ids (YTFLOW-572).
        // Reuse the freshly calculated ranges even if the ordinary cooldown rejected their count.
        // If non-uint ranges cannot be built without pivots, keep the version pending and retry.
        if (sinkTopologyChanged && !context.RecreateNow) {
            const bool proposedRangesAreUsable =
                std::ssize(context.NewRanges) == context.ProposedCount &&
                !context.NewRanges.empty() &&
                !TestRangeOverlaps(context.NewRanges) &&
                UniteRanges(context.NewRanges) == std::vector{UniversalKeyRange()};
            if (proposedRangesAreUsable) {
                context.RecreateNow = true;
                context.DecisionInitiator = "sink_topology";
                context.DecisionReason = "sink_topology_version_changed";
            }
        }

        if (EnableThroughputAutoscaler_ && ThroughputAutoscaler_.FindDecision(computationId)) {
            ThroughputAutoscaler_.AcceptPartitionCount(
                computationId,
                std::ssize(grouped.RangePartitions),
                context.RecreateNow ? std::ssize(context.NewRanges) : std::ssize(grouped.RangePartitions));
        }

        if (context.RecreateNow) {
            PendingPartitionChanges_.push_back({
                .ComputationId = computationId,
                .PreviousPartitionCount = std::ssize(grouped.RangePartitions),
                .NewPartitionCount = std::ssize(context.NewRanges),
                .Initiator = context.DecisionInitiator,
                .Reason = context.DecisionReason,
            });
            for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
                blockedStreamComputer->AddInterruptingPartition(partitionId);
                InterruptPartition(flowView, partitionId);
            }
            for (const auto& [lower, upper] : context.NewRanges) {
                CreateRangePartition(
                    computationId,
                    flowView,
                    lower,
                    upper,
                    makeDynamicPartitionSpec(lower, upper),
                    logger);
            }
            state.LastRepartitionTime = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
            LastRepartitioningInstant_ = state.LastRepartitionTime;
            state.LastObservedCommonRepartitioningInstant = state.LastRepartitionTime;
        } else {
            for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
                const auto& partition = GetOrCrash(layout->Partitions, partitionId);
                UpdateDynamicPartitionSpec(
                    flowView,
                    partitionId,
                    makeDynamicPartitionSpec(*partition->LowerKey, *partition->UpperKey),
                    logger);
            }
        }
        if (initializeSinkTopologyVersion || (sinkTopologyChanged && context.RecreateNow)) {
            state.PreviousAppliedSinkTopologyVersion =
                state.PersistedState->LastAppliedSinkTopologyVersion;
            state.PendingAppliedSinkTopologyVersion = rangeDescription->SinkTopologyVersion;
            state.PersistedState->LastAppliedSinkTopologyVersion =
                rangeDescription->SinkTopologyVersion;
        }
    } else if (auto* sourceDescription = std::get_if<IComputationController::TPartitioningDescription::TSource>(
        &description.Value))
    {
        for (const auto& partitionId : GetKeys(grouped.RangePartitions)) {
            // A range-shaped partition is invalid for source partitioning; interrupt and forget it.
            InterruptPartition(flowView, partitionId);
        }

        if (!sourceDescription->ExpectedKeys) {
            return;
        }
        auto expectedKeys = std::move(*sourceDescription->ExpectedKeys);

        auto makeDynamicPartitionSpec = [&] (
            const TKey& sourceKey,
            IMapNodePtr activeSourceSpec) {
            return BuildSourceDynamicPartitionSpec(
                std::move(activeSourceSpec),
                blockedStreamComputer->GetBlockedStreams(sourceKey),
                sourceDescription->UnavailableKeys.contains(sourceKey));
        };

        for (const auto& [partitionId, key] : grouped.KeyPartitions) {
            const auto& partition = GetOrCrash(layout->Partitions, partitionId);
            auto expectedIt = expectedKeys.find(key);
            if (partition->State == EPartitionState::Completed) {
                const bool isRetiring =
                    state.PersistedState->RetiringSourcePartitions.contains(partitionId);
                if (
                    !isRetiring &&
                    expectedIt != expectedKeys.end())
                {
                    expectedKeys.erase(expectedIt);
                    continue;
                }

                YT_TLOG_EVENT(
                    state.PublicLogger,
                    NLogging::ELogLevel::Info,
                    "Removing completed source partition")
                    .With("PartitionId", partitionId)
                    .With("Partition", ConvertToYsonString(partition, EYsonFormat::Text));
                layout->RemovePartition(partitionId);
                if (isRetiring) {
                    eraseRetiringSourcePartition(partitionId);
                }
                continue;
            }

            if (partition->State == EPartitionState::Executing && expectedIt != expectedKeys.end()) {
                UpdateDynamicPartitionSpec(
                    flowView,
                    partitionId,
                    makeDynamicPartitionSpec(key, std::move(expectedIt->second)),
                    logger);
                expectedKeys.erase(expectedIt);
                continue;
            }

            if (
                expectedIt == expectedKeys.end() &&
                !state.PersistedState->RetiringSourcePartitions.contains(partitionId))
            {
                rememberRetiringSourcePartitions();
                state.PersistedState->RetiringSourcePartitions.insert(partitionId);
            }

            // Ephemeral specs do not survive a restart. Keep publishing an empty active-source
            // spec while this partition owns the key through completion.
            UpdateDynamicPartitionSpec(
                flowView,
                partitionId,
                makeDynamicPartitionSpec(key, GetEphemeralNodeFactory()->CreateMap()),
                logger);
            if (partition->State == EPartitionState::Executing) {
                CompletePartition(flowView, partitionId);
            } else {
                YT_VERIFY(partition->State == EPartitionState::Completing);
            }
            if (expectedIt != expectedKeys.end()) {
                expectedKeys.erase(expectedIt);
            }
        }

        for (auto& [key, activeSourceSpec] : expectedKeys) {
            CreateSourcePartition(
                computationId,
                flowView,
                key,
                makeDynamicPartitionSpec(key, std::move(activeSourceSpec)),
                logger);
        }
    } else {
        YT_ABORT();
    }
}

IComputationController::TPartitioningStatus TPartitioningCoordinator::BuildPartitioningStatus(
    const TGroupedPartitions& grouped,
    const TFlowViewPtr& flowView) const
{
    IComputationController::TPartitioningStatus status;

    const auto& layout = flowView->State->ExecutionSpec->Layout;
    for (const auto& [partitionId, sourceKey] : grouped.KeyPartitions) {
        auto& sourceStatus = status.SourcePartitions[sourceKey];
        const auto* jobStatus = flowView->Feedback->PartitionJobStatuses.FindPtr(partitionId);
        if (!jobStatus) {
            continue;
        }

        auto extendedStatus = New<TExtendedSourcePartitionStatus>();
        if ((*jobStatus)->LastPartitionStatus) {
            const auto partitionStatus = ConvertTo<TComputationPartitionStatusPtr>(
                (*jobStatus)->LastPartitionStatus);
            if (partitionStatus && partitionStatus->ActiveSourceStatus) {
                extendedStatus->PartitionStatus = *partitionStatus->ActiveSourceStatus;
            }
        }
        extendedStatus->PartitionState = GetOrCrash(layout->Partitions, partitionId)->State;
        sourceStatus = std::move(extendedStatus);
    }

    return status;
}

TPartitioningCoordinator::TRangePartitioningParameters
TPartitioningCoordinator::BuildRangePartitioningParameters(
    TPartitioningSpecPtr partitioningSpec,
    const TComputationId& computationId,
    const TFlowViewPtr& flowView) const
{
    TRangePartitioningParameters parameters;
    parameters.ComputationId = computationId;
    parameters.PartitioningSpec = std::move(partitioningSpec);

    const auto& computationSpec = GetOrCrash(
        flowView->State->ExecutionSpec->PipelineSpec->GetValue()->Computations,
        computationId);
    for (const auto& streamId : GetKeys(computationSpec->TimerStreams)) {
        parameters.TimerStreamIds.insert(streamId);
    }
    const auto uintType = NTableClient::SimpleLogicalType(
        NTableClient::ESimpleLogicalValueType::Uint64);
    parameters.FirstKeyIsUint =
        *computationSpec->GroupBySchema->Columns()[0].LogicalType() == *uintType;
    parameters.EnableNonUintKey = computationSpec->ExperimentalEnableNonUintKey.value_or(
        TComputationSpec::ExperimentalEnableNonUintKeyDefault);
    return parameters;
}

IMapNodePtr TPartitioningCoordinator::BuildRangeDynamicPartitionSpec(
    THashSet<TStreamId> blockedOutputStreams)
{
    YT_VERIFY(!blockedOutputStreams.empty());
    auto spec = New<IComputation::TDynamicPartitionSpec>();
    spec->BlockedOutputStreams = std::move(blockedOutputStreams);
    return ConvertTo<IMapNodePtr>(spec);
}

IMapNodePtr TPartitioningCoordinator::BuildSourceDynamicPartitionSpec(
    IMapNodePtr activeSourceSpec,
    THashSet<TStreamId> blockedOutputStreams,
    bool availabilityGroupUnavailable)
{
    YT_VERIFY(activeSourceSpec);
    auto spec = New<IComputation::TDynamicPartitionSpec>();
    spec->ActiveSource = std::move(activeSourceSpec);
    spec->BlockedOutputStreams = std::move(blockedOutputStreams);
    spec->AvailabilityGroupUnavailable = availabilityGroupUnavailable;
    return ConvertTo<IMapNodePtr>(spec);
}

void TPartitioningCoordinator::InterruptPartition(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId) const
{
    flowView->State->ExecutionSpec->Layout->UpdatePartition(
        partitionId,
        EPartitionState::Interrupting,
        flowView->State->ExecutionSpec->GetEpoch(),
        TInstant::Now());
}

void TPartitioningCoordinator::CompletePartition(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId) const
{
    flowView->State->ExecutionSpec->Layout->UpdatePartition(
        partitionId,
        EPartitionState::Completing,
        flowView->State->ExecutionSpec->GetEpoch(),
        TInstant::Now());
}

void TPartitioningCoordinator::CreateSourcePartition(
    const TComputationId& computationId,
    const TFlowViewPtr& flowView,
    const TKey& sourceKey,
    const IMapNodePtr& dynamicComputationPartitionSpec,
    const NLogging::TLogger& logger) const
{
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    partition->ComputationId = computationId;
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = flowView->State->ExecutionSpec->GetEpoch();
    partition->StateTimestamp = TInstant::Now();
    partition->SourceKey = sourceKey;
    flowView->State->ExecutionSpec->Layout->CreatePartition(partition);
    UpdateDynamicPartitionSpec(flowView, partition->PartitionId, dynamicComputationPartitionSpec, logger);
}

void TPartitioningCoordinator::CreateRangePartition(
    const TComputationId& computationId,
    const TFlowViewPtr& flowView,
    const TKey& lowerKey,
    const TKey& upperKey,
    const IMapNodePtr& dynamicComputationPartitionSpec,
    const NLogging::TLogger& logger) const
{
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    partition->ComputationId = computationId;
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = flowView->State->ExecutionSpec->GetEpoch();
    partition->StateTimestamp = TInstant::Now();
    partition->LowerKey = lowerKey;
    partition->UpperKey = upperKey;
    flowView->State->ExecutionSpec->Layout->CreatePartition(partition);
    UpdateDynamicPartitionSpec(flowView, partition->PartitionId, dynamicComputationPartitionSpec, logger);
}

void TPartitioningCoordinator::UpdateDynamicPartitionSpec(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    const IMapNodePtr& dynamicComputationPartitionSpec,
    const NLogging::TLogger& logger) const
{
    const auto& spec = flowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    // The iteration owns a deep clone of the ephemeral state, so in-place mutation is safe;
    // job-manager-owned fields are left untouched.
    // TODO: Improve perf.
    if (
        spec->ComputationPartitionSpec &&
        (spec->ComputationPartitionSpec == dynamicComputationPartitionSpec ||
            AreNodesEqual(spec->ComputationPartitionSpec, dynamicComputationPartitionSpec)))
    {
        return;
    }
    spec->ComputationPartitionSpec = dynamicComputationPartitionSpec;
    YT_TLOG_EVENT(logger, NLogging::ELogLevel::Info, "Update dynamic partition spec")
        .With("PartitionId", partitionId)
        .With("NewDynamicPartitionSpec", ConvertToYsonString(spec, EYsonFormat::Text));
}

TPartitioningCoordinator::TThroughputSensors::TThroughputSensors(const NProfiling::TProfiler& profiler)
    : Demand(profiler.Gauge("/demand"))
    , SteadyDemand(profiler.Gauge("/steady_demand"))
    , PeakSteadyDemand(profiler.Gauge("/peak_steady_demand"))
    , Processed(profiler.Gauge("/processed"))
    , Capacity(profiler.Gauge("/capacity"))
    , DemandKnown(profiler.Gauge("/demand_known"))
    , SteadyDemandKnown(profiler.Gauge("/steady_demand_known"))
    , ProcessedKnown(profiler.Gauge("/processed_known"))
    , CapacityKnown(profiler.Gauge("/capacity_known"))
    , TargetPartitionCount(profiler.Gauge("/target_partition_count"))
    , TargetPartitionCountKnown(profiler.Gauge("/target_partition_count_known"))
    , AnchorPartitionCount(profiler.Gauge("/anchor_partition_count"))
    , AnchorCapacity(profiler.Gauge("/anchor_capacity"))
    , AnchorKnown(profiler.Gauge("/anchor_known"))
    , ExplorationOriginPartitionCount(profiler.Gauge("/exploration_origin_partition_count"))
    , ExplorationOriginProcessed(profiler.Gauge("/exploration_origin_processed"))
    , ExplorationBestProcessed(profiler.Gauge("/exploration_best_processed"))
    , ExplorationPartitionCountLimit(profiler.Gauge("/exploration_partition_count_limit"))
    , ExplorationKnown(profiler.Gauge("/exploration_known"))
    , SearchPhase(profiler.Gauge("/search_phase"))
    , CapacityDegraded(profiler.Gauge("/capacity_degraded"))
    , RetryWarmupSeconds(profiler.Gauge("/retry_warmup_seconds"))
    , WarmupReady(profiler.Gauge("/warmup_ready"))
    , Repartitioning(profiler.Gauge("/repartitioning"))
{ }

TPartitioningCoordinator::TCriterionSensors::TCriterionSensors(const NProfiling::TProfiler& profiler)
    : Average(profiler.Gauge("/criterion_average"))
    , ProposedPartitionCount(profiler.Gauge("/criterion_proposed_partition_count"))
    , DecisionCount(profiler.Counter("/decision_count"))
{ }

void TPartitioningCoordinator::ProfileThroughput(
    TComputationState& state,
    const TThroughputAutoscalerTelemetry& telemetry)
{
    if (!state.ThroughputSensors) {
        state.ThroughputSensors.emplace(state.Profiler.WithPrefix("/throughput"));
    }
    auto& sensors = *state.ThroughputSensors;
    if (telemetry.Demand) {
        sensors.Demand.Update(*telemetry.Demand);
    }
    if (telemetry.SteadyDemand) {
        sensors.SteadyDemand.Update(*telemetry.SteadyDemand);
    }
    if (telemetry.Processed) {
        sensors.Processed.Update(*telemetry.Processed);
    }
    if (telemetry.Capacity) {
        sensors.Capacity.Update(*telemetry.Capacity);
    }
    sensors.DemandKnown.Update(telemetry.Demand ? 1 : 0);
    sensors.SteadyDemandKnown.Update(telemetry.SteadyDemand ? 1 : 0);
    sensors.PeakSteadyDemand.Update(telemetry.PeakSteadyDemand);
    sensors.ProcessedKnown.Update(telemetry.Processed ? 1 : 0);
    sensors.CapacityKnown.Update(telemetry.Capacity ? 1 : 0);
    if (telemetry.TargetPartitionCount) {
        sensors.TargetPartitionCount.Update(*telemetry.TargetPartitionCount);
    }
    sensors.TargetPartitionCountKnown.Update(telemetry.TargetPartitionCount ? 1 : 0);
    if (telemetry.Anchor) {
        sensors.AnchorPartitionCount.Update(telemetry.Anchor->PartitionCount);
        sensors.AnchorCapacity.Update(telemetry.Anchor->Capacity);
    }
    sensors.AnchorKnown.Update(telemetry.Anchor ? 1 : 0);
    if (telemetry.Exploration) {
        sensors.ExplorationOriginPartitionCount.Update(telemetry.Exploration->OriginPartitionCount);
        sensors.ExplorationOriginProcessed.Update(telemetry.Exploration->OriginProcessed);
        sensors.ExplorationBestProcessed.Update(telemetry.Exploration->BestProcessed);
        sensors.ExplorationPartitionCountLimit.Update(*telemetry.ExplorationPartitionCountLimit);
    }
    sensors.ExplorationKnown.Update(telemetry.Exploration ? 1 : 0);
    sensors.SearchPhase.Update(static_cast<int>(telemetry.Phase));
    sensors.CapacityDegraded.Update(telemetry.CapacityDegraded ? 1 : 0);
    sensors.RetryWarmupSeconds.Update(telemetry.RetryWarmupPeriod.SecondsFloat());
    sensors.WarmupReady.Update(telemetry.WarmupReady ? 1 : 0);
    sensors.Repartitioning.Update(telemetry.Repartitioning ? 1 : 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
