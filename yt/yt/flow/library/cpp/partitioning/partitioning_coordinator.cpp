#include "partitioning_coordinator.h"

#include "partitioning_helpers.h"

#include <yt/yt/flow/library/cpp/common/source_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/iterator/concatenate.h>

#include <algorithm>
#include <cmath>
#include <set>

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
    registrar.Parameter("key_count_partitioning", &TThis::KeyCountPartitioning)
        .DefaultNew();
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
        state.KeyCount = std::make_unique<TKeyCountPartitioning>(
            state.PersistedState->KeyCountPartitioning,
            statusProfiler ? statusProfiler->ErrorState(Format("/%v/key_count_partitioning", computationId)) : nullptr);
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
    THashMap<TComputationId, TKeyCountPartitioningStatePtr> keyCountStates;
    for (auto& [computationId, state] : Computations_) {
        const auto spec = DynamicPointerCast<TPartitioningSpec>(state.Controller->GetParametersBase());
        if (spec && spec->KeyCountPartitioning->Enable) {
            keyCountStates.emplace(computationId, state.PersistedState->KeyCountPartitioning);
        }
        state.KeyCount->BeginIteration(committedFlowView);
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
    if (!keyCountStates.empty()) {
        for (const auto& [id, partition] : committedFlowView->State->ExecutionSpec->Layout->Partitions) {
            const auto stateIt = keyCountStates.find(partition->ComputationId);
            if (stateIt == keyCountStates.end() || !partition->CurrentJobId) {
                continue;
            }
            const auto& status = committedFlowView->Feedback->GetFreshCurrentJobStatus(id, TInstant::Zero());
            if (status && status->JobId == *partition->CurrentJobId && status->ObservedKeys) {
                IngestObservedKeys(stateIt->second, *status->ObservedKeys);
            }
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
        state.KeyCount->Commit();
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

    flowView->EphemeralState->KeyCountPartitioningBlockedComputations.clear();
    flowView->EphemeralState->KeyCountPartitioning.clear();
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
                &description.Value);
                range && !range->PartitioningSpec->KeyCountPartitioning->Enable)
            {
                computation.PartitioningSpec = range->DynamicPartitioningSpec;
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
            state.LastPartitionCountChangeTime = state.LastRepartitionTime;
        }

        const bool hasSinkTopologyVersion =
            rangeDescription->SinkTopologyVersion != TVersion(0);
        const bool sinkTopologyChanged =
            hasSinkTopologyVersion &&
            state.PersistedState->LastAppliedSinkTopologyVersion &&
            *state.PersistedState->LastAppliedSinkTopologyVersion !=
                rangeDescription->SinkTopologyVersion;

        TInputAutoPartitioningContext context(flowView, grouped.RangePartitions);
        context.MaxSinkChannelCount = rangeDescription->MaxSinkChannelCount;
        auto parameters = BuildRangePartitioningParameters(
            rangeDescription->DynamicPartitioningSpec,
            computationId,
            flowView);
        if (rangeDescription->PartitioningSpec->KeyCountPartitioning->Enable) {
            auto result = state.KeyCount->Partition(
                computationId,
                rangeDescription->PartitioningSpec,
                rangeDescription->DynamicPartitioningSpec,
                grouped.RangePartitions,
                flowView,
                parameters.FirstKeyIsUint);
            if (!result.IsOK()) {
                return;
            }
            auto& decision = result.Value();
            context.NewRanges = std::move(decision.Ranges);
            context.RetainedPartitions = std::move(decision.RetainedPartitions);
            context.ProposedCount = std::ssize(context.NewRanges);
            context.RecreateNow = !decision.Reason.empty();
            context.DecisionInitiator = "key_count_partitioning";
            context.DecisionReason = std::move(decision.Reason);
        } else {
            InputAutoPartitioningCollectData(state, context);
            InputAutoPartitioningCalculateOptimalCount(state, parameters, context);
            if (!context.RecreateNow && !sinkTopologyChanged && !context.PartitionRanges.empty()) {
                context.ProposedCount = std::ssize(context.PartitionRanges);
            }
            InputAutoPartitioningBuildRanges(state, parameters, context);
            if (!context.RecreateNow && !sinkTopologyChanged && context.ProposedCount != std::ssize(context.PartitionRanges)) {
                context.ProposedCount = std::ssize(context.PartitionRanges);
                InputAutoPartitioningBuildRanges(state, parameters, context);
            }
            InputAutoPartitioningTryRebalance(state, parameters, context);
        }
        state.LastObservedCommonRepartitioningInstant = LastRepartitioningInstant_;

        const bool initializeSinkTopologyVersion =
            hasSinkTopologyVersion &&
            !state.PersistedState->LastAppliedSinkTopologyVersion &&
            !sinkTopologyChanged;
        if (sinkTopologyChanged) {
            context.RetainedPartitions.clear();
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
            std::set<TKeyRange> retainedRanges;
            for (const auto& [partitionId, range] : grouped.RangePartitions) {
                if (context.RetainedPartitions.contains(partitionId)) {
                    retainedRanges.insert(range);
                    UpdateDynamicPartitionSpec(flowView, partitionId, makeDynamicPartitionSpec(range.Lower, range.Upper), logger);
                    continue;
                }
                blockedStreamComputer->AddInterruptingPartition(partitionId);
                InterruptPartition(flowView, partitionId);
            }
            for (const auto& [lower, upper] : context.NewRanges) {
                if (retainedRanges.contains({lower, upper})) {
                    continue;
                }
                CreateRangePartition(
                    computationId,
                    flowView,
                    lower,
                    upper,
                    makeDynamicPartitionSpec(lower, upper),
                    logger);
            }
            state.LastRepartitionTime = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
            if (context.NewRanges.size() != context.PartitionRanges.size()) {
                state.LastPartitionCountChangeTime = state.LastRepartitionTime;
            }
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
    TDynamicPartitioningSpecPtr partitioningSpec,
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

////////////////////////////////////////////////////////////////////////////////

TPartitioningCoordinator::TInputAutoPartitioningContext::TInputAutoPartitioningContext(
    const TFlowViewPtr& flowView,
    const THashMap<TPartitionId, TKeyRange>& partitionRanges)
    : FlowView(flowView)
    , PartitionRanges(partitionRanges)
{ }

namespace {

template <class T>
struct TAverage
{
    TAverage(double limit, EPartitioningCriterion criterion)
        : Limit(limit)
        , Criterion(criterion)
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
    EPartitioningCriterion Criterion;
    T Value = {};
    ssize_t Count = 0;
    THashMap<TPartitionId, T> Weights;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitioningCoordinator::InputAutoPartitioningCollectData(
    TComputationState& state,
    TInputAutoPartitioningContext& context) const
{
    const auto& Logger = state.Logger;

    context.AllPartitionsHaveStatuses = true;
    const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
    for (const auto& [partitionId, keyRange] : context.PartitionRanges) {
        auto it = partitionStatuses.find(partitionId);
        if (it == partitionStatuses.end() || !it->second->CurrentJobStatus) {
            context.AllPartitionsHaveStatuses = false;
            break;
        }
    }

    const auto& pipelineState = context.FlowView->State->ExecutionSpec->PipelineState;
    if (pipelineState->GetValue() != EPipelineState::Working) {
        context.NormalFlightDuration = TDuration::Zero();
        context.RebalanceFlightDuration = TDuration::Zero();
        YT_TLOG_INFO("Partitioning: wait for working state")
            .With("PipelineState", pipelineState->GetValue())
            .With("LastRepartitionTime", state.LastRepartitionTime)
            .With("PipelineStateUpdate", pipelineState->GetLastUpdate())
            .With("NormalFlightDuration", context.NormalFlightDuration);
    } else {
        TInstant baseInstant = std::max(state.LastPartitionCountChangeTime, pipelineState->GetLastUpdate());
        TInstant rebalanceBaseInstant = std::max(state.LastRepartitionTime, pipelineState->GetLastUpdate());
        context.AnotherComputationRecentlyRepartitioned =
            LastRepartitioningInstant_ > state.LastObservedCommonRepartitioningInstant;
        if (!context.AnotherComputationRecentlyRepartitioned) {
            rebalanceBaseInstant = std::max(rebalanceBaseInstant, LastRepartitioningInstant_);
            // Moving our own boundaries must not restart the count-change cooldown.
            if (LastRepartitioningInstant_ != state.LastRepartitionTime) {
                baseInstant = std::max(baseInstant, LastRepartitioningInstant_);
            }
        }
        // Both operands use the cluster clock; node-clock skew would corrupt the cooldown.
        const auto now = TInstant::Seconds(context.FlowView->State->CurrentTimestamp.Underlying());
        context.NormalFlightDuration = now - baseInstant;
        context.RebalanceFlightDuration = now - rebalanceBaseInstant;
        YT_TLOG_INFO("Partitioning: calculating normal flight duration")
            .With("PipelineState", pipelineState->GetValue())
            .With("LastRepartitionTime", state.LastRepartitionTime)
            .With("PipelineStateUpdate", pipelineState->GetLastUpdate())
            .With("NormalFlightDuration", context.NormalFlightDuration);
    }
}

double TPartitioningCoordinator::ApplyPeakHoldRelease(
    double previous,
    double target,
    TDuration elapsed,
    TDuration releaseHalfDelay)
{
    if (target >= previous || releaseHalfDelay == TDuration::Zero()) {
        // Growth is immediate; smoothing can also be disabled explicitly.
        return target;
    }
    // The remaining gap to the lower target halves every release half-delay.
    double alpha = 1.0 - std::exp2(-elapsed.SecondsFloat() / releaseHalfDelay.SecondsFloat());
    return previous + alpha * (target - previous);
}

void TPartitioningCoordinator::InputAutoPartitioningCalculateOptimalCount(
    TComputationState& state,
    const TRangePartitioningParameters& parameters,
    TInputAutoPartitioningContext& context)
{
    const auto& Logger = state.Logger;
    const auto& spec = parameters.PartitioningSpec;

    const auto [minInputPartitionCount, maxInputPartitionCount] = spec->GetPartitionCountBounds();
    const i64 sinkChannelMultiplier =
        spec->SinkChannelMultiplier.value_or(DefaultSinkChannelMultiplier);
    const double desiredAveragePartitionCpuLoad =
        spec->DesiredAveragePartitionCpuLoad.value_or(DefaultDesiredAveragePartitionCpuLoad);
    const double desiredAveragePartitionMemoryUsed =
        spec->DesiredAveragePartitionMemoryUsed.value_or(DefaultDesiredAveragePartitionMemoryUsed);
    const double desiredAveragePartitionMessagesPerSecond =
        spec->DesiredAveragePartitionMessagesPerSecond.value_or(
        DefaultDesiredAveragePartitionMessagesPerSecond);
    const double desiredAveragePartitionBytesPerSecond =
        spec->DesiredAveragePartitionBytesPerSecond.value_or(
        DefaultDesiredAveragePartitionBytesPerSecond);
    const double desiredAveragePartitionTimerCount =
        spec->DesiredAveragePartitionTimerCount.value_or(DefaultDesiredAveragePartitionTimerCount);
    const double allowedPartitionCountDeviation =
        spec->AllowedPartitionCountDeviation.value_or(DefaultAllowedPartitionCountDeviation);
    const TDuration partitionCountDoubleDelay =
        spec->PartitionCountDoubleDelay.value_or(DefaultPartitionCountDoubleDelay);
    const TDuration partitionCountHalfDelay =
        spec->PartitionCountHalfDelay.value_or(DefaultPartitionCountHalfDelay);

    const i64 currentPartitionCount = std::ssize(context.PartitionRanges);
    const i64 workerCount = std::ssize(context.FlowView->State->Workers);

    struct TProposition
    {
        i64 Count;
        EPartitioningCriterion Criterion;
        THashMap<TPartitionId, double> Weights;
        double ResourcePressure = 0;

        bool IsThroughput() const
        {
            return Criterion == EPartitioningCriterion::Throughput || Criterion == EPartitioningCriterion::ThroughputHold;
        }
    };

    std::vector<TProposition> suggestions;
    THashMap<TPartitionId, double> throughputWeights;
    const auto throughputDecision = EnableThroughputAutoscaler_
        ? ThroughputAutoscaler_.FindDecision(parameters.ComputationId)
        : std::nullopt;
    if (context.MaxSinkChannelCount) {
        i64 count = *context.MaxSinkChannelCount * sinkChannelMultiplier;
        count = std::max(count, workerCount);
        suggestions.emplace_back(count, EPartitioningCriterion::SinkChannels);
        YT_TLOG_INFO("Partitioning: collected sink channels")
            .With("ChannelCount", *context.MaxSinkChannelCount)
            .With("ProposedPartitionCount", count);
    }

    if (!context.PartitionRanges.empty()) {
        const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
        TAverage<double> averageCpuUsage(desiredAveragePartitionCpuLoad, EPartitioningCriterion::Cpu);
        TAverage<double> averageMemoryUsage(desiredAveragePartitionMemoryUsed, EPartitioningCriterion::Memory);
        TAverage<double> averageMessagesPerSecond(
            EnableThroughputAutoscaler_ ? 0 : desiredAveragePartitionMessagesPerSecond,
            EPartitioningCriterion::Messages);
        TAverage<double> averageBytesPerSecond(
            EnableThroughputAutoscaler_ ? 0 : desiredAveragePartitionBytesPerSecond,
            EPartitioningCriterion::Bytes);
        TAverage<double> averageTimerCount(desiredAveragePartitionTimerCount, EPartitioningCriterion::Timers);
        for (const auto& [partitionId, keyRange] : context.PartitionRanges) {
            auto it = partitionStatuses.find(partitionId);
            if (it == partitionStatuses.end() || !it->second->CurrentJobStatus) {
                continue;
            }
            const auto& status = it->second->CurrentJobStatus;
            if (status->PerformanceMetrics &&
                (status->PerformanceMetrics->CpuUsage10m || status->PerformanceMetrics->CpuUsage30s))
            {
                averageCpuUsage.Add(
                    partitionId,
                    status->PerformanceMetrics->CpuUsage10m
                        ? *status->PerformanceMetrics->CpuUsage10m
                        : *status->PerformanceMetrics->CpuUsage30s);
            }
            if (status->PerformanceMetrics && status->PerformanceMetrics->MemoryUsage10m) {
                averageMemoryUsage.Add(partitionId, *status->PerformanceMetrics->MemoryUsage10m);
            }
            if (status->InputMetrics) {
                averageMessagesPerSecond.Add(partitionId, status->InputMetrics->Global.MessagesPerSecond);
                averageBytesPerSecond.Add(partitionId, status->InputMetrics->Global.BytesPerSecond);
            }
            if (status->FromPartitionTraverseData && status->FromPartitionTraverseData->Node) {
                ssize_t timerCount = 0;
                const auto& nodeStreams = status->FromPartitionTraverseData->Node->Streams;
                for (const auto& streamId : parameters.TimerStreamIds) {
                    auto streamIt = nodeStreams.find(streamId);
                    if (streamIt != nodeStreams.end() && streamIt->second->InflightMetrics) {
                        timerCount += streamIt->second->InflightMetrics->Count;
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

        const ssize_t minStatusCount =
            static_cast<ssize_t>(currentPartitionCount / allowedPartitionCountDeviation);
        const auto now = TInstant::Seconds(context.FlowView->State->CurrentTimestamp.Underlying());
        TStringBuilder smoothingReport;
        for (auto* averageMetric : {
                &averageCpuUsage,
                &averageMemoryUsage,
                &averageMessagesPerSecond,
                &averageBytesPerSecond,
                &averageTimerCount})
        {
            if (averageMetric->Limit <= 0 || averageMetric->Count < minStatusCount) {
                continue;
            }
            const double ratio = averageMetric->Get() / averageMetric->Limit;
            const double proposedByCriterion = std::round(currentPartitionCount * ratio);
            auto& sensors = state.CriterionSensors.try_emplace(
                averageMetric->Criterion,
                state.Profiler.WithTag("criterion", FormatEnum(averageMetric->Criterion)))
                .first->second;
            sensors.Average.Update(averageMetric->Get());
            sensors.ProposedPartitionCount.Update(proposedByCriterion);
            // Peak-hold smoothing lets a criterion grow instantly but releases it only with the
            // configured half-delay, avoiding rapid shrinkage after a transient load dip.
            const auto criterion = averageMetric->Criterion;
            auto [it, inserted] = state.CriterionProposedCountEma.emplace(
                criterion,
                TCriterionEmaState{proposedByCriterion, now});
            if (!inserted) {
                it->second.ProposedCount = ApplyPeakHoldRelease(
                    it->second.ProposedCount,
                    proposedByCriterion,
                    now - it->second.UpdatedAt,
                    partitionCountHalfDelay);
                it->second.UpdatedAt = now;
            }
            if (smoothingReport.GetLength() > 0) {
                smoothingReport.AppendString("; ");
            }
            smoothingReport.AppendFormat(
                "%v: %v -> %v",
                criterion,
                proposedByCriterion,
                it->second.ProposedCount);
            suggestions.emplace_back(
                static_cast<ssize_t>(std::llround(it->second.ProposedCount)),
                averageMetric->Criterion,
                std::move(averageMetric->Weights),
                ratio);
        }
        if (smoothingReport.GetLength() > 0) {
            YT_TLOG_INFO("Partitioning: peak-hold smoothed criterion proposals")
                .With("RawToSmoothed", smoothingReport.Flush());
        }
        for (auto& suggestion : suggestions) {
            if (suggestion.Count < std::ssize(context.PartitionRanges)) {
                suggestion.Count = std::min(
                    suggestion.Count * 3,
                    std::ssize(context.PartitionRanges));
            }
        }
        if (EnableThroughputAutoscaler_) {
            throughputWeights = std::move(averageMessagesPerSecond.Weights);
        }
    }

    // The throughput target already includes its downscale reserve.
    if (throughputDecision) {
        suggestions.emplace_back(
            throughputDecision->TargetPartitionCount,
            EPartitioningCriterion::Throughput,
            THashMap<TPartitionId, double>{});
    } else if (EnableThroughputAutoscaler_ && !spec->DesiredPartitionCount && currentPartitionCount > 0) {
        suggestions.emplace_back(currentPartitionCount, EPartitioningCriterion::ThroughputHold);
    }

    auto finalizeWeights = [&] {
        if (EnableThroughputAutoscaler_) {
            context.Weights = std::move(throughputWeights);
        }
        double sum = 0.0;
        for (const auto& [partitionId, weight] : context.Weights) {
            sum += weight;
        }
        const double average = context.Weights.empty() ? 1.0 : sum / std::ssize(context.Weights);
        for (const auto& [partitionId, range] : context.PartitionRanges) {
            if (!context.Weights.contains(partitionId)) {
                context.Weights[partitionId] = average;
            }
        }
    };

    auto strictResult = [&] (i64 proposedCount, THashMap<TPartitionId, double>&& weights) {
        context.ProposedCount = proposedCount;
        context.RecreateNow = proposedCount != currentPartitionCount;
        context.DecisionInitiator = "manual";
        context.DecisionReason = "desired_partition_count";
        context.Weights = std::move(weights);
        finalizeWeights();
    };

    auto shouldRecreate = [&] (i64 proposedCount, bool throughput = false) {
        if (proposedCount == currentPartitionCount) {
            return false;
        }
        if (currentPartitionCount < minInputPartitionCount || currentPartitionCount > maxInputPartitionCount) {
            return true;
        }
        // Throughput growth uses its own capacity tolerance instead of the legacy count deadband.
        const bool throughputScaleUp = throughput && proposedCount > currentPartitionCount;
        if (
            !throughputScaleUp &&
            proposedCount <= currentPartitionCount * allowedPartitionCountDeviation &&
            proposedCount >= currentPartitionCount / allowedPartitionCountDeviation)
        {
            return false;
        }
        if (!context.AllPartitionsHaveStatuses || context.NormalFlightDuration == TDuration::Zero()) {
            return false;
        }
        if (throughput) {
            return true;
        }

        TDuration delay;
        if (proposedCount > currentPartitionCount) {
            const double proposedMultiple =
                static_cast<double>(proposedCount) / static_cast<double>(currentPartitionCount);
            const TDuration referenceDelay = partitionCountDoubleDelay;
            delay = std::max(referenceDelay / log(proposedMultiple) * log(2), referenceDelay / 2);
            if (context.AnotherComputationRecentlyRepartitioned) {
                const TDuration timeSinceAnyRepartitioning =
                    TInstant::Seconds(context.FlowView->State->CurrentTimestamp.Underlying()) -
                    LastRepartitioningInstant_;
                const TDuration crossComputationAffectDuration = std::min(referenceDelay, delay) / 2;
                if (timeSinceAnyRepartitioning < crossComputationAffectDuration) {
                    const double coefficient =
                        0.5 + 0.5 * (timeSinceAnyRepartitioning / crossComputationAffectDuration);
                    delay *= coefficient;
                    YT_TLOG_INFO("Partitioning: delay decreased due to cross computation affect")
                        .With("Coefficient", coefficient)
                        .With("NewDelay", delay.Seconds());
                }
            }
        } else {
            const double proposedMultiple =
                static_cast<double>(currentPartitionCount) / static_cast<double>(proposedCount);
            const TDuration referenceDelay = partitionCountHalfDelay;
            delay = std::max(referenceDelay / log(proposedMultiple) * log(2), referenceDelay / 2);
        }
        return context.NormalFlightDuration > delay;
    };

    if (suggestions.empty()) {
        if (spec->DesiredPartitionCount) {
            return strictResult(*spec->DesiredPartitionCount, {});
        }
        context.ProposedCount = std::clamp(workerCount, minInputPartitionCount, maxInputPartitionCount);
        context.DecisionInitiator = "minimum";
        context.DecisionReason = workerCount ? "worker_count" : "minimum_partition_count";
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

    // A ready throughput proposal must not release a larger legacy proposal before its delay.
    if (EnableThroughputAutoscaler_ && !spec->DesiredPartitionCount) {
        for (auto& suggestion : suggestions) {
            const i64 count = std::clamp(suggestion.Count, minInputPartitionCount, maxInputPartitionCount);
            if (!suggestion.IsThroughput() && count > currentPartitionCount && !shouldRecreate(count)) {
                suggestion.Count = currentPartitionCount;
            }
        }
    }

    ssize_t maxSuggestionIndex = 0;
    for (ssize_t index = 1; index < std::ssize(suggestions); ++index) {
        if (suggestions[index].Count > suggestions[maxSuggestionIndex].Count ||
            (suggestions[index].Count == suggestions[maxSuggestionIndex].Count && suggestions[index].IsThroughput()))
        {
            maxSuggestionIndex = index;
        }
    }
    ssize_t weightsSuggestionIndex = maxSuggestionIndex;
    for (ssize_t index = 0; index < std::ssize(suggestions); ++index) {
        if (!suggestions[index].Weights.empty() &&
            (suggestions[weightsSuggestionIndex].Weights.empty() ||
                suggestions[index].ResourcePressure > suggestions[weightsSuggestionIndex].ResourcePressure))
        {
            weightsSuggestionIndex = index;
        }
    }
    if (spec->DesiredPartitionCount) {
        return strictResult(
            *spec->DesiredPartitionCount,
            std::move(suggestions[weightsSuggestionIndex].Weights));
    }

    context.ProposedCount = std::clamp(
        suggestions[maxSuggestionIndex].Count,
        minInputPartitionCount,
        maxInputPartitionCount);
    context.DecisionInitiator = suggestions[maxSuggestionIndex].IsThroughput() ? "throughput" : "legacy";
    context.DecisionReason = FormatEnum(suggestions[maxSuggestionIndex].Criterion);
    const bool returningToWorkingPoint = throughputDecision &&
        throughputDecision->Action == EThroughputPartitioningAction::ReturnToWorkingPoint;
    if (!returningToWorkingPoint && context.ProposedCount < currentPartitionCount / 2) {
        context.ProposedCount = std::clamp(
            currentPartitionCount / 2,
            minInputPartitionCount,
            maxInputPartitionCount);
    }
    const bool throughputShrink = throughputDecision &&
        throughputDecision->TargetPartitionCount <= context.ProposedCount &&
        context.ProposedCount < currentPartitionCount;
    context.RecreateNow = shouldRecreate(
        context.ProposedCount,
        suggestions[maxSuggestionIndex].IsThroughput() || throughputShrink);
    context.Weights = std::move(suggestions[weightsSuggestionIndex].Weights);
    finalizeWeights();

    YT_TLOG_INFO("Partitioning: calculated optimal partition count")
        .With("ProposedCount", context.ProposedCount)
        .With("RecreateNow", context.RecreateNow)
        .With("CurrentCount", currentPartitionCount)
        .With("Criteria", suggestions[maxSuggestionIndex].Criterion)
        .With("Initiator", context.DecisionInitiator)
        .With("Reason", context.RecreateNow ? context.DecisionReason : "cooldown_or_deadband")
        .With("NormalFlightDuration", context.NormalFlightDuration.Seconds());

    state.ProposedPartitionCountGauge.Update(context.ProposedCount);
    state.CriterionSensors.try_emplace(
        suggestions[maxSuggestionIndex].Criterion,
        state.Profiler.WithTag("criterion", context.DecisionReason))
        .first->second.DecisionCount.Increment();
}

void TPartitioningCoordinator::InputAutoPartitioningBuildRanges(
    const TComputationState& state,
    const TRangePartitioningParameters& parameters,
    TInputAutoPartitioningContext& context) const
{
    context.NewRanges.clear();
    context.NewWeights.clear();
    context.HasRequiredPivots = context.AllPartitionsHaveStatuses;
    if (context.ProposedCount == 0) {
        return;
    }

    const auto& Logger = state.Logger;

    if (!parameters.EnableNonUintKey) {
        YT_VERIFY(parameters.FirstKeyIsUint);
        context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        return;
    }

    if (context.PartitionRanges.empty()) {
        context.NewRanges = parameters.FirstKeyIsUint
            ? SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount)
            : std::vector<TKeyRange>{UniversalKeyRange()};
        return;
    }

    auto oldSortedRanges = GetSortedPartitionRanges(context.PartitionRanges);

    struct TWeightedRange
        : TKeyRange
    {
        double Weight = 0;
    };

    const double totalWeight = Accumulate(context.Weights | std::views::elements<1>, 0.0);
    std::vector<TWeightedRange> splitRanges;
    const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
    double totalMessagesPerSecond = 0;
    for (const auto& [partitionId, range] : oldSortedRanges) {
        auto it = partitionStatuses.find(partitionId);
        if (it != partitionStatuses.end() && it->second->CurrentJobStatus && it->second->CurrentJobStatus->InputMetrics) {
            totalMessagesPerSecond += it->second->CurrentJobStatus->InputMetrics->Global.MessagesPerSecond;
        } else {
            context.HasRequiredPivots = false;
        }
    }
    const double pivotRateThreshold = 0.5 * totalMessagesPerSecond / std::ssize(context.PartitionRanges);
    for (const auto& [partitionId, range] : oldSortedRanges) {
        double weight = context.Weights.at(partitionId);
        auto it = partitionStatuses.find(partitionId);
        if (
            it == partitionStatuses.end() ||
            !it->second->CurrentJobStatus ||
            !it->second->CurrentJobStatus->InputMetrics ||
            it->second->CurrentJobStatus->InputMetrics->Global.Pivots.empty())
        {
            if (it != partitionStatuses.end() && it->second->CurrentJobStatus && it->second->CurrentJobStatus->InputMetrics) {
                const double rate = it->second->CurrentJobStatus->InputMetrics->Global.MessagesPerSecond;
                if (rate > 0 && rate >= pivotRateThreshold) {
                    context.HasRequiredPivots = false;
                }
            }
            splitRanges.push_back({range, weight});
            continue;
        }
        const auto& splitters = it->second->CurrentJobStatus->InputMetrics->Global.Pivots;
        const double subWeight = weight / (std::ssize(splitters) + 1);
        splitRanges.push_back({{range.Lower, splitters.front()}, subWeight});
        weight -= subWeight;
        for (ssize_t index = 0; index < std::ssize(splitters) - 1; ++index) {
            splitRanges.push_back({{splitters[index], splitters[index + 1]}, subWeight});
            weight -= subWeight;
        }
        splitRanges.push_back({{splitters.back(), range.Upper}, weight});
    }

    ssize_t nonEmptyCount = 0;
    double pendingWeight = 0;
    for (const auto& range : splitRanges) {
        pendingWeight += range.Weight;
        if (range.Lower < range.Upper) {
            splitRanges[nonEmptyCount++] = {{range.Lower, range.Upper}, pendingWeight};
            pendingWeight = 0;
        }
    }
    splitRanges.resize(nonEmptyCount);
    splitRanges.back().Weight += pendingWeight;

    context.NewRanges.reserve(context.ProposedCount);
    context.NewWeights.reserve(context.ProposedCount);
    double remainingWeight = totalWeight;
    auto remainingCount = context.ProposedCount;
    ssize_t index = 0;
    while (remainingCount > 0 && index < std::ssize(splitRanges)) {
        const double requiredWeight = remainingWeight / remainingCount;
        double consumedWeight = splitRanges[index].Weight;
        ssize_t endIndex = index + 1;
        const ssize_t maxEndIndex = std::ssize(splitRanges) - (remainingCount - 1);
        while (consumedWeight < requiredWeight && endIndex < maxEndIndex) {
            consumedWeight += splitRanges[endIndex].Weight;
            ++endIndex;
        }
        if (consumedWeight > requiredWeight && endIndex > index + 1) {
            if (
                consumedWeight - requiredWeight >
                requiredWeight - (consumedWeight - splitRanges[endIndex - 1].Weight))
            {
                --endIndex;
                consumedWeight -= splitRanges[endIndex].Weight;
            }
        }

        context.NewRanges.push_back({
            splitRanges[index].Lower,
            splitRanges[endIndex - 1].Upper,
        });
        context.NewWeights.push_back(consumedWeight);
        --remainingCount;
        remainingWeight -= consumedWeight;
        index = endIndex;
    }
    context.NewRanges.back().Upper = splitRanges.back().Upper;
    if (std::ssize(context.NewRanges) != context.ProposedCount || !context.HasRequiredPivots) {
        if (parameters.FirstKeyIsUint) {
            YT_TLOG_INFO("Partitioning: fall back to uniform uint split")
                .With("ResultSize", std::ssize(context.NewRanges))
                .With("PartitionCount", context.ProposedCount)
                .With("HasRequiredPivots", context.HasRequiredPivots)
                .With("SubrangeCount", std::ssize(splitRanges));
            context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        } else {
            YT_TLOG_INFO("Partitioning: cannot recreate now, must wait for partition pivots")
                .With("ResultSize", std::ssize(context.NewRanges))
                .With("PartitionCount", context.ProposedCount)
                .With("HasRequiredPivots", context.HasRequiredPivots)
                .With("SubrangeCount", std::ssize(splitRanges));
            context.RecreateNow = false;
        }
        // The calculated weights no longer describe NewRanges after fallback or deferral.
        context.NewWeights.clear();
    }
}

void TPartitioningCoordinator::InputAutoPartitioningTryRebalance(
    const TComputationState& state,
    const TRangePartitioningParameters& parameters,
    TInputAutoPartitioningContext& context) const
{
    const auto& Logger = state.Logger;
    const auto& spec = parameters.PartitioningSpec;

    const double allowedPartitionCountDeviation =
        spec->AllowedPartitionCountDeviation.value_or(
        DefaultAllowedPartitionCountDeviation);
    if (
        !context.RecreateNow &&
        context.HasRequiredPivots &&
        !context.NewWeights.empty() &&
        !context.PartitionRanges.empty() &&
        context.RebalanceFlightDuration != TDuration::Zero() &&
        context.ProposedCount <= std::ssize(context.PartitionRanges) * allowedPartitionCountDeviation &&
        context.ProposedCount >= std::ssize(context.PartitionRanges) / allowedPartitionCountDeviation)
    {
        auto oldRanges = GetSortedPartitionRanges(context.PartitionRanges);
        if (std::ranges::equal(oldRanges | std::views::values, context.NewRanges)) {
            return;
        }
        auto getDeviation = [] (const auto& weights) {
            const double average = Accumulate(weights, 0.0) / std::ssize(weights);
            double deviation = Accumulate(weights, 0.0, [&] (double sum, double weight) {
                return sum + std::pow(weight - average, 2);
            });
            return std::sqrt(deviation / std::ssize(weights));
        };
        const double oldDeviation = getDeviation(context.Weights | std::views::elements<1>);
        const double newDeviation = getDeviation(context.NewWeights);
        const double averageWeight = Accumulate(context.Weights | std::views::elements<1>, 0.0) /
            std::ssize(context.PartitionRanges);
        if (averageWeight > 0 && newDeviation + 1e-5 < oldDeviation) {
            const double relativeDeviation = oldDeviation / averageWeight;
            const auto partitionCountDoubleDelay =
                spec->PartitionCountDoubleDelay.value_or(
                DefaultPartitionCountDoubleDelay);
            const auto delay = partitionCountDoubleDelay / std::min(1.0 + relativeDeviation, 10.0);
            if (context.RebalanceFlightDuration > delay) {
                context.RecreateNow = true;
                context.DecisionInitiator = "rebalance";
                context.DecisionReason = "weight_deviation";
                YT_TLOG_INFO("Partitioning: decided to rebuild partitions to make more uniform distribution")
                    .With("OldDeviation", oldDeviation)
                    .With("NewDeviation", newDeviation)
                    .With("RelativeDeviation", relativeDeviation)
                    .With("Delay", delay)
                    .With("Initiator", "rebalance")
                    .With("Reason", "weight_deviation");
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
