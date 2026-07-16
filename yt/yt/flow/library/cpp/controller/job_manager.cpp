#include "private.h"

#include "job_balancer.h"
#include "job_manager.h"
#include "state_manager.h"
#include "yt_connector.h"

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <library/cpp/iterator/concatenate.h>

#include <util/generic/set.h>

namespace NYT::NFlow::NController {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

//! This is the "infinity" value that we report to the LastPartitionOkTime metrics if the job had never been seen in "OK" state.
static constexpr TDuration LastPartitionOkTimeInf = TDuration::Minutes(30);

////////////////////////////////////////////////////////////////////////////////

class TJobManager
    : public IJobManager
{
public:
    TJobManager(TJobManagerContextPtr context, TPipelineSpecPtr spec, TDynamicPipelineSpecPtr dynamicSpec, TJobManagerStatePtr state, IPipelineAuthenticatorPtr authenticator)
        : Context_(std::move(context))
        , Spec_(std::move(spec))
        , State_(CloneYsonStruct(state))
        , StateManager_(New<TStateManager>(State_))
        , PipelineAuthenticator_(std::move(authenticator))
        , DynamicSpec_(std::move(dynamicSpec))
        , ResourceManager_(CreateResourceManager(CreateResourceManagerContext(), Spec_->Resources, DynamicSpec_->Resources))
        , ComputationControllers_()
        , Profiler_(WithPipelineRelatedTags(
            ControllerProfiler()
                .WithGlobal()
                .WithPrefix("/job_manager")))
        , FailedJobsCounter_(Profiler_.Counter("/failed_jobs"))
        , SucceededJobsCounter_(Profiler_.Counter("/succeeded_jobs"))
        , LostJobsCounter_(Profiler_.Counter("/lost_jobs"))
        , PartitioningMutationsCounter_(Profiler_.Counter("/partitioning_mutations"))
        , DistributingMutationsCounter_(Profiler_.Counter("/distributing_mutations"))
        , MaxTimeSincePartitionOkGauge_(Profiler_.Gauge("/max_time_since_partition_ok"))
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->MainCycleInvoker);

        THROW_ERROR_EXCEPTION_UNLESS(State_, "Initial JobManagerState cannot be null");

        THashSet<TWorkerGroupId> workerGroups;
        auto computationControllerCommonContext = New<TComputationControllerCommonContext>();

        // Collect the controller-side required resources across all computations and load them
        // once. LoadRequiredResources also drives the always-on resources, so it is called even
        // when the set is empty.
        THashSet<TResourceId> requiredResourceIds;
        for (const auto& [computationId, spec] : Spec_->Computations) {
            for (const auto& [resourceId, resourceDescription] : spec->RequiredResourceIds) {
                if (resourceDescription->Controller) {
                    requiredResourceIds.insert(resourceId);
                }
            }
        }
        WaitFor(ResourceManager_->LoadRequiredResources(requiredResourceIds))
            .ThrowOnError();

        for (const auto& [computationId, spec] : Spec_->Computations) {
            const auto dynamicSpec = GetOrDefault(DynamicSpec_->Computations, computationId, New<TDynamicComputationSpec>());
            workerGroups.insert(spec->WorkerGroup);
            auto context = New<TComputationControllerContext>(computationControllerCommonContext);
            context->ComputationSpec = spec;
            context->ComputationId = computationId;
            context->TimeProvider = Context_->TimeProvider;
            context->Profiler = WithPipelineRelatedTags(
                ControllerProfiler()
                    .WithPrefix("/computation")
                    .WithTag("computation_id", computationId.Underlying())
                    .WithGlobal());
            context->StatusProfiler = Context_->StatusProfiler->WithPrefix(Format("/computation_controllers/%v", computationId.Underlying()));
            context->Logger = Logger().WithTag("ComputationId: %v", computationId.Underlying());
            context->ClientsCache = Context_->ClientsCache;
            context->PipelinePath = Context_->PipelinePath;
            context->Invoker = Context_->MainCycleInvoker;
            context->PublicLogger = PublicControllerLogger().WithTag("ComputationId: %v", computationId.Underlying());
            auto dynamicContext = New<TDynamicComputationControllerContext>();
            dynamicContext->DynamicComputationSpec = dynamicSpec;

            for (const auto& [resourceId, resourceDescription] : spec->RequiredResourceIds) {
                if (!resourceDescription->Controller) {
                    continue;
                }

                auto aliasResourceId = resourceDescription->Alias ? *resourceDescription->Alias : resourceId;
                EmplaceOrCrash(context->StaticResources, aliasResourceId, ResourceManager_->Get(resourceId));
            }

            YT_LOG_INFO("Creating controller (ComputationId: %v)",
                computationId);
            try {
                auto controller = TRegistry::Get()->CreateComputationController(context, dynamicContext);
                controller->Init(StateManager_->CreateContext(computationId));
                ComputationControllers_[computationId] = controller;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to create computation controller")
                    << TErrorAttribute("computation_id", computationId)
                    << TError(ex);
            }
        }

        for (const auto& workerGroup : workerGroups) {
            EmplaceOrCrash(BalanceSynchronizers_, workerGroup, NBalancer::CreateBalanceAsyncSynchronizer(Profiler_.WithPrefix("/job_balancer"), workerGroup));
        }
        ActualizeBalancing();
    }

    void ActualizeBalancing()
    {
        const auto dynamicJobManagerSpec = DynamicSpec_->JobManager;
        for (const auto& [workerGroup, balanceSynchronizer] : BalanceSynchronizers_) {
            const auto groupJobBalancerSpec = GetOrDefault(dynamicJobManagerSpec->WorkerGroupOverride, workerGroup, dynamicJobManagerSpec);
            if (groupJobBalancerSpec->AsyncBalancing) {
                balanceSynchronizer->StartBalancing(Context_->Invoker);
            } else {
                balanceSynchronizer->StopBalancing();
            }
        }
    }

    void Reconfigure(TDynamicPipelineSpecPtr dynamicSpec) override
    {
        DynamicSpec_ = std::move(dynamicSpec);

        ActualizeBalancing();

        for (const auto& [computationId, controller] : ComputationControllers_) {
            auto dynamicContext = New<TDynamicComputationControllerContext>();
            auto iter = DynamicSpec_->Computations.find(computationId);
            if (iter != DynamicSpec_->Computations.end()) {
                dynamicContext->DynamicComputationSpec = iter->second;
            } else {
                dynamicContext->DynamicComputationSpec = New<TDynamicComputationSpec>();
            }
            controller->Reconfigure(dynamicContext);
        }

        // Reconfigure resources.
        ResourceManager_->Reconfigure(DynamicSpec_->Resources);
    }

    void AggregateTraverseData(const TFlowViewPtr& flowView) override
    {
        const auto& traverseData = flowView->State->TraverseData;
        const auto& spec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
        const auto& extendedSpec = flowView->State->ExecutionSpec->ExtendedPipelineSpec->GetValue();
        const auto& currentPartitionJobStatuses = flowView->Feedback->PartitionJobStatuses;
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        THashMap<TComputationId, std::vector<TPartitionId>> groupedPartitions;
        for (const auto& [partitionId, partition] : layout->Partitions) {
            // Ignore partitions from unknown computations.
            if (!ComputationControllers_.contains(partition->ComputationId)) {
                continue;
            }
            if (partition->State == EPartitionState::Completed) {
                groupedPartitions[partition->ComputationId].push_back(partitionId);
            } else if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
                auto partitionJobStatusIt = currentPartitionJobStatuses.find(partitionId);
                if (partitionJobStatusIt == currentPartitionJobStatuses.end() || !partitionJobStatusIt->second->LastTraverseData) {
                    YT_LOG_WARNING("Not all partitions has traverse data (PartitionId: %v, ComputationId: %v)",
                        partitionId,
                        partition->ComputationId);
                    return;
                }
                groupedPartitions[partition->ComputationId].push_back(partitionId);
            }
        }
        {
            THashSet<TComputationId> uncoveredComputations;
            for (const auto& [computationId, controller] : ComputationControllers_) {
                if (!controller->IsFullCoverage(groupedPartitions[computationId], flowView)) {
                    YT_LOG_INFO("Computation does not have full coverage (ComputationId: %v)",
                        computationId);
                    uncoveredComputations.insert(computationId);
                } else {
                    YT_LOG_INFO("Computation has full coverage (ComputationId: %v)",
                        computationId);
                }
            }
            flowView->EphemeralState->TraverseUncoveredComputations = std::move(uncoveredComputations);
        }
        if (!flowView->EphemeralState->TraverseUncoveredComputations.empty()) {
            YT_LOG_INFO("Not all computations have full coverage, interrupted traverse data aggregation");
            return;
        }

        YT_LOG_INFO("Aggregating traverse data");

        THashMap<TComputationId, THashMap<TPartitionId, TNodeTraverseDataPtr>> groupedTraverses;
        for (const auto& [computationId, controller] : ComputationControllers_) {
            groupedTraverses[computationId] = {}; // Sometimes some source computations may not have partitions.
        }
        for (const auto& [computationId, partitions] : groupedPartitions) {
            for (const auto& partitionId : partitions) {
                try {
                    auto partition = GetOrCrash(layout->Partitions, partitionId);
                    auto partitionCodicilGuard = TErrorCodicils::MakeGuard("partition", [&] () -> std::string {
                        return ConvertToYsonString(partition, NYson::EYsonFormat::Text).ToString();
                    });
                    if (partition->State == EPartitionState::Completed) {
                        auto nodeTraverse = MakeCompletedPartitionTraverseData(
                            flowView->State->Epoch,
                            flowView->State->CurrentTimestamp,
                            GetOrCrash(extendedSpec->Computations, computationId));
                        ValidateFromPartitionTraverseData(
                            nodeTraverse,
                            GetOrCrash(spec->Computations, computationId),
                            GetOrCrash(extendedSpec->Computations, computationId));
                        groupedTraverses[computationId][partitionId] = nodeTraverse->Node;
                    } else if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
                        auto& partitionJobStatus = GetOrCrash(currentPartitionJobStatuses, partitionId);
                        ValidateFromPartitionTraverseData(
                            partitionJobStatus->LastTraverseData,
                            GetOrCrash(spec->Computations, computationId),
                            GetOrCrash(extendedSpec->Computations, computationId));
                        groupedTraverses[computationId][partitionId] = partitionJobStatus->LastTraverseData->Node;
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION(ex)
                        << TErrorAttribute("computation_id", computationId)
                        << TErrorAttribute("partition_id", partitionId);
                }
            }
        }

        std::vector<TStreamTraverseDataPtr> inputStreams;
        std::vector<TStreamTraverseDataPtr> sourceStreams;
        std::vector<TStreamTraverseDataPtr> timerStreams;
        std::vector<TStreamTraverseDataPtr> keyVisitorStreams;
        std::vector<TStreamTraverseDataPtr> outputStreams;
        THashMap<TStreamId, TStreamTraverseDataPtr> streamsTraverseData;
        for (const auto& [computationId, nodes] : groupedTraverses) {
            auto computationSpec = GetOrCrash(spec->Computations, computationId);
            auto& current = traverseData->Computations[computationId];
            try {
                auto processTraverseDataResult = GetOrCrash(ComputationControllers_, computationId)->ProcessPartitionTraverseData(nodes, flowView);
                flowView->EphemeralState->StreamTraverseDataMetrics[computationId] = processTraverseDataResult->StreamMetrics;
                current = AdvanceNodeTraverseData(current, processTraverseDataResult->MergedTraverseData);
            } catch (const std::exception& ex) {
                // One failing computation must not abort the whole aggregation - that freezes the
                // traverse (stream metrics, watermarks) of the entire pipeline. Reuse the previous
                // node data: only this computation's streams stay behind.
                if (!current) {
                    THROW_ERROR_EXCEPTION(ex)
                        << TErrorAttribute("computation_id", computationId);
                }
                YT_LOG_EVENT(
                    PublicControllerLogger,
                    NLogging::ELogLevel::Warning,
                    "Failed to process partition traverse data, reusing the previous one "
                    "(ComputationId: %v, Error: %v)",
                    computationId,
                    TError(ex));
            }
            try {
                for (const auto& streamId : computationSpec->InputStreamIds) {
                    auto stream = GetOrCrash(current->Streams, streamId);
                    inputStreams.push_back(stream);
                }
                for (const auto& streamId : GetKeys(computationSpec->SourceStreams)) {
                    auto stream = GetOrCrash(current->Streams, streamId);
                    sourceStreams.push_back(stream);
                    auto globalStreamId = MakeGlobalStreamId(computationId, streamId, computationSpec);
                    EmplaceOrCrash(streamsTraverseData, globalStreamId, stream);
                }
                for (const auto& streamId : GetKeys(computationSpec->TimerStreams)) {
                    auto stream = GetOrCrash(current->Streams, streamId);
                    timerStreams.push_back(stream);
                    auto globalStreamId = MakeGlobalStreamId(computationId, streamId, computationSpec);
                    EmplaceOrCrash(streamsTraverseData, globalStreamId, stream);
                }
                for (const auto& streamId : GetKeys(computationSpec->KeyVisitorStreams)) {
                    auto stream = GetOrCrash(current->Streams, streamId);
                    keyVisitorStreams.push_back(stream);
                    auto globalStreamId = MakeGlobalStreamId(computationId, streamId, computationSpec);
                    EmplaceOrCrash(streamsTraverseData, globalStreamId, stream);
                }
                for (const auto& streamId : computationSpec->OutputStreamIds) {
                    auto stream = GetOrCrash(current->Streams, streamId);
                    outputStreams.push_back(stream);
                    EmplaceOrCrash(streamsTraverseData, streamId, stream);
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(ex)
                    << TErrorAttribute("computation_id", computationId);
            }
        }
        traverseData->Streams = std::move(streamsTraverseData);

        YT_LOG_INFO("TraverseData: %v",
            NYson::ConvertToYsonString(traverseData, NYson::EYsonFormat::Text));
        std::vector<TStreamTraverseDataPtr> completedStream = {MakeCompletedStreamTraverseData(flowView->State->ExecutionSpec->GetEpoch(), flowView->State->CurrentTimestamp)};

        traverseData->UnitedSourceStream = MergeStreamTraverseData(ConcatVectors(sourceStreams, completedStream), EInflightMerge::Sum, /*allowPartial*/ true);
        traverseData->UnitedTimerStream = MergeStreamTraverseData(ConcatVectors(timerStreams, completedStream), EInflightMerge::Sum, /*allowPartial*/ true);
        traverseData->UnitedKeyVisitorStream = MergeStreamTraverseData(ConcatVectors(keyVisitorStreams, completedStream), EInflightMerge::Sum, /*allowPartial*/ true);
        traverseData->UnitedOutputStream = MergeStreamTraverseData(ConcatVectors(outputStreams, completedStream), EInflightMerge::Sum, /*allowPartial*/ true);
        auto unitedInputStream = MergeStreamTraverseData(ConcatVectors(inputStreams, completedStream), EInflightMerge::None);

        if (unitedInputStream->Epoch == flowView->State->ExecutionSpec->GetEpoch() && traverseData->UnitedOutputStream->Epoch == flowView->State->ExecutionSpec->GetEpoch()) {
            auto newWatermark = std::min(traverseData->UnitedOutputStream->SystemWatermark, unitedInputStream->SystemWatermark);
            if (traverseData->InputSystemWatermark > newWatermark) {
                YT_LOG_WARNING("Possible data loss: system has events below InputSystemWatermark. (InputSystemWatermark: %v, ComputedWatermark: %v)",
                    traverseData->InputSystemWatermark,
                    newWatermark);
            }
            traverseData->InputSystemWatermark = std::max(newWatermark, traverseData->InputSystemWatermark);
        }

        traverseData->UnitedStream = MergeStreamTraverseData(
            ConcatVectors(sourceStreams, timerStreams, keyVisitorStreams, outputStreams, completedStream),
            EInflightMerge::Sum,
            /*allowPartial*/ true);

        if (!UnitedStreamLastEpoch_.has_value() || UnitedStreamLastEpoch_->first != traverseData->UnitedStream->Epoch) {
            UnitedStreamLastEpoch_ = {traverseData->UnitedStream->Epoch, TInstant::Now()};
        }
    }

    void UpdateInputStreamsTraverse(const TFlowViewPtr& flowView) override
    {
        flowView->State->ExecutionSpec->InputStreamsTraverse->SetValue(flowView->State->TraverseData->Streams);
    }

    void UpdateWatermarkState(const TFlowViewPtr& flowView) override
    {
        const auto& traverseData = flowView->State->TraverseData;
        auto watermarkState = New<TWatermarkState>();
        watermarkState->CurrentTimestamp = flowView->State->CurrentTimestamp;
        for (const auto& [streamId, streamTraverse] : traverseData->Streams) {
            auto watermarks = New<TWatermarks>();
            watermarks->SystemWatermark = streamTraverse->SystemWatermark;
            watermarks->EventWatermark = streamTraverse->EventWatermark;
            watermarkState->Streams[streamId] = watermarks;
        }
        for (const auto& [_, computationSpec] : flowView->State->ExecutionSpec->PipelineSpec->GetValue()->Computations) {
            if (computationSpec->WatermarkStrategy->WatermarkAlignment) {
                const auto& group = computationSpec->WatermarkStrategy->WatermarkAlignment->GroupName;
                if (!watermarkState->AlignmentGroups.contains(group)) {
                    auto watermarks = New<TWatermarks>();
                    watermarks->SystemWatermark = InfinitySystemTimestamp;
                    watermarks->EventWatermark = InfinitySystemTimestamp;
                    watermarkState->AlignmentGroups[group] = watermarks;
                }

                for (const auto& streamId : computationSpec->OutputStreamIds) {
                    auto& watermarks = watermarkState->AlignmentGroups[group];
                    watermarks->SystemWatermark = std::min(
                        watermarks->SystemWatermark,
                        watermarkState->GetSystemWatermark(streamId));
                    watermarks->EventWatermark = std::min(
                        watermarks->EventWatermark,
                        watermarkState->GetEventWatermark(streamId));
                }
            }
        }
        flowView->State->ExecutionSpec->WatermarkState->SetValue(watermarkState);
    }

    void CheckCompletedPartitions(const TFlowViewPtr& flowView) override
    {
        YT_LOG_INFO("Looking for new completed partitions (PartitionCount: %v)",
            flowView->Feedback->PartitionJobStatuses.size());
        ui64 foundOk = 0;
        ui64 foundGracefulRebalance = 0;
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        for (const auto& [partitionId, partitionJobStatus] : flowView->Feedback->PartitionJobStatuses) {
            auto& currentJobStatus = partitionJobStatus->CurrentJobStatus;
            if (!currentJobStatus || !currentJobStatus->IsFinished) {
                continue;
            }

            // Graceful-shutdown finish: try to move the partition to the pending target
            // worker. If no target is set or the target has gone, fall through — the old
            // job is then dropped by RemoveFailedJobs below and the balancer reschedules.
            if (currentJobStatus->Error.FindMatching(NFlow::EErrorCode::GracefulShutdown)) {
                auto jobPtr = layout->Jobs.FindPtr(currentJobStatus->JobId);
                if (!jobPtr) {
                    continue;
                }
                const auto& partition = GetOrCrash(layout->Partitions, (*jobPtr)->PartitionId);

                auto* eph = flowView->EphemeralState->Partitions.FindPtr(partition->PartitionId);
                if (!eph || !(*eph)->PendingGracefulRebalanceWorkerAddress.has_value()) {
                    continue;
                }
                auto targetWorkerIt = flowView->State->Workers.find(*(*eph)->PendingGracefulRebalanceWorkerAddress);
                if (targetWorkerIt == flowView->State->Workers.end()) {
                    continue;
                }

                (*eph)->PendingGracefulRebalanceWorkerAddress = std::nullopt;
                if ((*eph)->DynamicPartitionSpec) {
                    auto spec = ConvertTo<TUniversalComputationDynamicPartitionSpecPtr>((*eph)->DynamicPartitionSpec);
                    spec->FinishAfterCurrentEpoch = false;
                    (*eph)->DynamicPartitionSpec = ConvertToNode(spec)->AsMap();
                }

                YT_LOG_INFO("Graceful rebalance: recreating job on target worker "
                    "(JobId: %v, PartitionId: %v, ComputationId: %v, TargetWorkerAddress: %v)",
                    currentJobStatus->JobId,
                    partition->PartitionId,
                    partition->ComputationId,
                    targetWorkerIt->second->RpcAddress);

                layout->RemoveJob(currentJobStatus->JobId, EJobFinishReason::Rebalanced);

                auto newJob = New<TJob>();
                newJob->JobId = TJobId(TGuid::Create());
                newJob->WorkerAddress = targetWorkerIt->second->RpcAddress;
                newJob->WorkerIncarnationId = targetWorkerIt->second->IncarnationId;
                newJob->PartitionId = partition->PartitionId;
                layout->CreateJob(newJob);
                ++foundGracefulRebalance;
                continue;
            }

            if (!currentJobStatus->Error.IsOK()) {
                // Other errors are handled by RemoveFailedJobs below.
                continue;
            }

            ++foundOk;
            if (auto jobPtr = layout->Jobs.FindPtr(currentJobStatus->JobId)) {
                const auto& job = *jobPtr;
                if (auto partitionPtr = layout->Partitions.FindPtr(job->PartitionId)) {
                    const auto& partition = *partitionPtr;
                    EPartitionState newState;
                    if (partition->State == EPartitionState::Executing) {
                        newState = EPartitionState::Completing;
                    } else if (partition->State == EPartitionState::Completing) {
                        newState = EPartitionState::Completed;
                    } else if (partition->State == EPartitionState::Interrupting) {
                        newState = EPartitionState::Interrupted;
                    } else {
                        THROW_ERROR_EXCEPTION("Partition is already completed (PartitionId: %v, ComputationId: %v, Status: %v)",
                            partition->PartitionId,
                            partition->ComputationId,
                            partition->State);
                    }
                    YT_LOG_EVENT(
                        PublicControllerLogger,
                        NLogging::ELogLevel::Info,
                        "Job completed (JobId: %v, PartitionId: %v, ComputationId: %v)",
                        currentJobStatus->JobId,
                        partitionId,
                        partition->ComputationId);
                    layout->UpdatePartition(job->PartitionId, newState, currentJobStatus->Epoch, currentJobStatus->UpdateTime);
                }
            }
        }

        YT_LOG_INFO("Check completed partitions (New: %v, GracefulRebalance: %v)", foundOk, foundGracefulRebalance);
        SucceededJobsCounter_.Increment(foundOk);
    }

    void RemoveFailedJobs(const TFlowViewPtr& flowView) override
    {
        YT_LOG_INFO("Looking for failed jobs (CheckedPartitionsCount: %v)",
            flowView->Feedback->PartitionJobStatuses.size());
        ui64 foundError = 0;
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        for (const auto& [partitionId, partitionJobStatus] : flowView->Feedback->PartitionJobStatuses) {
            auto& currentJobStatus = partitionJobStatus->CurrentJobStatus;
            if (!currentJobStatus || !currentJobStatus->IsFinished || !layout->Jobs.contains(currentJobStatus->JobId)) {
                continue;
            }
            if (!currentJobStatus->Error.IsOK()) {
                if (currentJobStatus->Error.FindMatching(NFlow::EErrorCode::GracefulShutdown)) {
                    // A graceful-shutdown finish that CheckCompletedPartitions did not re-create
                    // (e.g. the move target is gone, or the pending target was already cleared).
                    // This is not a failure: drop the finished job so the balancer reschedules it,
                    // without logging an error, counting a failure or applying the failure backoff.
                    YT_LOG_INFO("Removing gracefully finished job (JobId: %v, PartitionId: %v, ComputationId: %v)",
                        currentJobStatus->JobId,
                        partitionId,
                        GetOrCrash(layout->Partitions, partitionId)->ComputationId);
                    layout->RemoveJob(currentJobStatus->JobId, EJobFinishReason::Rebalanced);
                    continue;
                }
                foundError += 1;
                YT_LOG_EVENT(
                    PublicControllerLogger,
                    NLogging::ELogLevel::Error,
                    currentJobStatus->Error,
                    "Job failed (JobId: %v, PartitionId: %v, ComputationId: %v)",
                    currentJobStatus->JobId,
                    partitionId,
                    GetOrCrash(layout->Partitions, partitionId)->ComputationId);
                layout->RemoveJob(currentJobStatus->JobId, EJobFinishReason::Failed);

                auto partitionState = flowView->EphemeralState->GetPartitionState(partitionId);
                partitionState->PreviousJobFailInstant = partitionJobStatus->CurrentJobStatusUpdateTime;
                partitionState->PreviousJobFailError = currentJobStatus->Error;
            }
        }

        YT_LOG_INFO("Finished removing failed jobs (Failed: %v, CheckedPartitionsCount: %v)",
            foundError,
            flowView->Feedback->PartitionJobStatuses.size());
        FailedJobsCounter_.Increment(foundError);
    }

    bool CheckPipelineStopped(const TFlowViewPtr& flowView) override
    {
        return flowView->State->ExecutionSpec->GetEpoch() == flowView->State->TraverseData->UnitedStream->Epoch && flowView->State->TraverseData->UnitedStream->State >= EStreamState::Drained;
    }

    std::optional<TDuration> GetTimeSinceSynced(const TFlowViewPtr& flowView)
    {
        if (!UnitedStreamLastEpoch_.has_value()) {
            return std::nullopt;
        }

        if (UnitedStreamLastEpoch_.value().first != flowView->State->ExecutionSpec->GetEpoch()) {
            return std::nullopt;
        }

        return TInstant::Now() - UnitedStreamLastEpoch_.value().second;
    }

    bool CheckPipelineCompleted(const TFlowViewPtr& flowView) override
    {
        if (flowView->State->ExecutionSpec->GetEpoch() != flowView->State->TraverseData->UnitedStream->Epoch) {
            return false;
        }
        if (flowView->State->TraverseData->UnitedStream->State != EStreamState::Completed) {
            return false;
        }

        for (const auto& [_, partition] : flowView->State->ExecutionSpec->Layout->Partitions) {
            if (partition->State != EPartitionState::Completed && partition->State != EPartitionState::Interrupted) {
                return false;
            }
        }

        return true;
    }

    void RemoveLostJobs(const TFlowViewPtr& flowView) override
    {
        YT_LOG_INFO("Looking for lost jobs (CheckedPartitionsCount: %v)",
            flowView->Feedback->PartitionJobStatuses.size());

        ui64 removed = 0;
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        auto removeLostJob = [&] (const TJobPtr& job, const std::string& lostReasonTags) {
            auto jobId = job->JobId;
            auto& partition = GetOrCrash(layout->Partitions, job->PartitionId);
            auto error = TError(
                "Job is lost because worker is lost; "
                "possible reasons: stopping by deploying system, OOM, segfault, assertion failure; "
                "check your deploying system metrics, coredump directory, stderr of worker "
                "(JobId: %v, PartitionId: %v, ComputationId: %v, %v)",
                jobId,
                partition->PartitionId,
                partition->ComputationId,
                lostReasonTags);
            YT_LOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, error);
            removed++;
            layout->RemoveJob(jobId, EJobFinishReason::LostWorker);

            auto partitionState = flowView->EphemeralState->GetPartitionState(job->PartitionId);
            partitionState->PreviousJobFailInstant = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
            partitionState->PreviousJobFailError = std::move(error);
        };

        for (const auto& [partitionId, partitionJobStatus] : flowView->Feedback->PartitionJobStatuses) {
            if (!partitionJobStatus->CurrentJobId) {
                continue;
            }
            const auto& jobId = *partitionJobStatus->CurrentJobId;
            auto jobIt = layout->Jobs.find(jobId);
            if (jobIt == layout->Jobs.end()) {
                YT_LOG_WARNING("Unknown jobId in PartitionJobStatuses (JobId: %v)",
                    jobId);
                continue;
            }
            if (partitionJobStatus->CurrentJobStatusUpdateTime + DynamicSpec_->JobManager->LostJobTimeout < flowView->Feedback->UpdateTime) {
                removeLostJob(jobIt->second, Format("Reason: Timeout, LastJobStatusUpdateTime: %v, Timeout: %v", partitionJobStatus->CurrentJobStatusUpdateTime, DynamicSpec_->JobManager->LostJobTimeout));
            }
        }

        std::vector<TJobPtr> allJobs;
        allJobs.reserve(layout->Jobs.size());
        for (const auto& [jobId, job] : layout->Jobs) {
            allJobs.push_back(job);
        }

        for (const auto& job : allJobs) {
            const auto worker = GetOrDefault(flowView->State->Workers, job->WorkerAddress, nullptr);
            if (!worker) {
                removeLostJob(job, "Reason: UnknownWorker");
            } else if (worker->IncarnationId != job->WorkerIncarnationId) {
                auto reasonTags = Format("Reason: WorkerIncarnationMismatch, WorkerIdentifyingString: %v, JobWorkerIncarnationId: %v",
                    worker->GetIdentifyingString(),
                    job->WorkerIncarnationId);
                removeLostJob(job, reasonTags);
            }
        }

        YT_LOG_INFO("Finished removing lost jobs (Removed: %v, CheckedPartitionsCount: %v)",
            removed,
            flowView->Feedback->PartitionJobStatuses.size());
        LostJobsCounter_.Increment(removed);
    }

    void DoPartitioning(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        ui64 executing = 0;
        ui64 completing = 0;
        ui64 interrupting = 0;
        ui64 total = 0;
        for (const auto& [_, partition] : layout->Partitions) {
            total += 1;
            if (partition->State == EPartitionState::Executing) {
                executing += 1;
            }
            if (partition->State == EPartitionState::Completing) {
                completing += 1;
            }
            if (partition->State == EPartitionState::Interrupting) {
                interrupting += 1;
            }
        }
        YT_LOG_INFO("Started partitioning (Workers: %v, ComputationControllers: %v, Partitions: %v, Executing: %v, Completing: %v, Interrupting: %v)",
            flowView->State->Workers.size(),
            ComputationControllers_.size(),
            total,
            executing,
            completing,
            interrupting);


        ssize_t oldUpdated = layout->GetUpdated();
        {
            THashMap<TComputationId, std::vector<TPartitionId>> partitions;
            for (const auto& [partitionId, partition] : layout->Partitions) {
                if (ComputationControllers_.count(partition->ComputationId) > 0) {
                    partitions[partition->ComputationId].push_back(partitionId);
                } else if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
                    layout->UpdatePartition(partitionId, EPartitionState::Interrupted, flowView->State->ExecutionSpec->GetEpoch(), TInstant::Now());
                }
            }
            for (auto& [computationId, controller] : ComputationControllers_) {
                controller->DoPartitioning(partitions[computationId], flowView);
            }
        }
        ssize_t updated = layout->GetUpdated() - oldUpdated;

        const auto& partitionJobStatuses = flowView->Feedback->PartitionJobStatuses;
        TDuration maxTimeSinceOk = TDuration::Zero();

        auto now = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
        for (const auto& [partitionId, partition] : layout->Partitions) {
            if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
                auto partitionJobStatusIt = partitionJobStatuses.find(partitionId);
                if (partitionJobStatusIt == partitionJobStatuses.end()) {
                    maxTimeSinceOk = std::max(maxTimeSinceOk, now - partition->StateTimestamp);
                    continue;
                }
                auto& ephemeralPartitionState = flowView->EphemeralState->Partitions[partitionId];
                if (!ephemeralPartitionState) {
                    ephemeralPartitionState = New<TPartitionEphemeralState>();
                }
                const auto& status = partitionJobStatusIt->second;
                if (status->CurrentJobStatus && status->CurrentJobStatus->Error.IsOK() && status->CurrentJobStatus->FromPartitionTraverseData) {
                    ephemeralPartitionState->LastOkTime = status->CurrentJobStatusUpdateTime;
                }
                maxTimeSinceOk = std::max(maxTimeSinceOk, now - ephemeralPartitionState->LastOkTime);
            }
        }
        maxTimeSinceOk = std::min(maxTimeSinceOk, LastPartitionOkTimeInf);
        MaxTimeSincePartitionOkGauge_.Update(maxTimeSinceOk.Seconds());

        YT_LOG_INFO("Finished partitioning (Mutations: %v)", updated);
        PartitioningMutationsCounter_.Increment(updated);
    }

    void DistributeJobs(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        ui64 executing = 0;
        ui64 completing = 0;
        ui64 interrupting = 0;
        ui64 total = 0;
        for (const auto& [_, partition] : layout->Partitions) {
            total += 1;
            if (partition->State == EPartitionState::Executing) {
                executing += 1;
            }
            if (partition->State == EPartitionState::Completing) {
                completing += 1;
            }
            if (partition->State == EPartitionState::Interrupting) {
                interrupting += 1;
            }
        }
        YT_LOG_INFO("Started job distributions (Workers: %v, Partitions: %v, Executing: %v, Completing: %v, Interrupting: %v)",
            flowView->State->Workers.size(),
            total,
            executing,
            completing,
            interrupting);

        if (flowView->State->Workers.size() < DynamicSpec_->JobManager->MinimumWorkerCount) {
            YT_LOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, "Too few workers (Count: %v, Required: %v)", flowView->State->Workers.size(), DynamicSpec_->JobManager->MinimumWorkerCount);
            return StopAllJobs(flowView);
        }

        ssize_t stopCount = 0;
        auto stopJob = [&] (const auto& partition) {
            stopCount++;
            layout->RemoveJob(partition->CurrentJobId.value(), EJobFinishReason::Rebalanced);

            auto partitionState = flowView->EphemeralState->GetPartitionState(partition->PartitionId);
            partitionState->PreviousRebalancingInstant = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
        };
        ssize_t gracefulStopCount = 0;
        auto gracefulStopJob = [&] (const auto& partition, const std::string& targetWorkerAddress) {
            // Signal the worker to finish after the current epoch gracefully.
            // The new job will be created in CheckCompletedPartitions once the current job finishes.
            gracefulStopCount++;
            auto partitionState = flowView->EphemeralState->GetPartitionState(partition->PartitionId);
            partitionState->PendingGracefulRebalanceWorkerAddress = targetWorkerAddress;
            partitionState->PreviousRebalancingInstant = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());

            // Build a new DynamicPartitionSpec with FinishAfterCurrentEpoch=true, preserving existing fields.
            auto newSpec = New<TUniversalComputationDynamicPartitionSpec>();
            newSpec->FinishAfterCurrentEpoch = true;
            if (partitionState->DynamicPartitionSpec) {
                auto existing = ConvertTo<TUniversalComputationDynamicPartitionSpecPtr>(partitionState->DynamicPartitionSpec);
                newSpec->ActiveSource = existing->ActiveSource;
                newSpec->BlockedOutputStreams = existing->BlockedOutputStreams;
            }
            partitionState->DynamicPartitionSpec = ConvertToNode(newSpec)->AsMap();

            YT_LOG_INFO("Graceful rebalance initiated (PartitionId: %v, TargetWorkerAddress: %v)",
                partition->PartitionId,
                targetWorkerAddress);
        };
        ssize_t createCount = 0;
        auto createJob = [&] (const auto& partitionId, const auto& worker) {
            createCount++;
            // A partition without a dynamic partition spec never reaches the worker: the heartbeat
            // response omits its job, which then dies by the status timeout with a misleading
            // "worker is lost" error. Name the real problem instead.
            const auto* partitionState = flowView->EphemeralState->Partitions.FindPtr(partitionId);
            if (!partitionState || !(*partitionState)->DynamicPartitionSpec) {
                YT_LOG_EVENT(
                    PublicControllerLogger,
                    NLogging::ELogLevel::Error,
                    "Creating job for a partition without dynamic partition spec; the job cannot start "
                    "until the spec appears (PartitionId: %v, ComputationId: %v)",
                    partitionId,
                    GetOrCrash(layout->Partitions, partitionId)->ComputationId);
            }
            auto job = New<TJob>();
            job->JobId = TJobId(TGuid::Create());
            job->WorkerAddress = worker->RpcAddress;
            job->WorkerIncarnationId = worker->IncarnationId;
            job->PartitionId = partitionId;
            layout->CreateJob(job);
        };

        for (const auto& [jobId, job] : layout->Jobs) {
            auto partition = GetOrCrash(layout->Partitions, job->PartitionId);
            auto worker = GetOrDefault(flowView->State->Workers, job->WorkerAddress, nullptr);
            if (!worker) {
                THROW_ERROR_EXCEPTION("Worker not found")
                    << TErrorAttribute("job_id", jobId)
                    << TErrorAttribute("partition_id", job->PartitionId)
                    << TErrorAttribute("worker_address", job->WorkerAddress)
                    << TErrorAttribute("worker_incarnation_id", job->WorkerIncarnationId);
            }
            if (worker->IncarnationId != job->WorkerIncarnationId) {
                THROW_ERROR_EXCEPTION("Worker incarnation mismatch")
                    << TErrorAttribute("job_id", jobId)
                    << TErrorAttribute("partition_id", job->PartitionId)
                    << TErrorAttribute("worker_address", job->WorkerAddress)
                    << TErrorAttribute("worker_incarnation_id", job->WorkerIncarnationId)
                    << TErrorAttribute("actual_worker_incarnation_id", worker->IncarnationId);
            }
        }

        const auto jobManagerSpec = DynamicSpec_->JobManager;
        for (const auto& [workerGroup, balanceSynchronizer] : BalanceSynchronizers_) {
            const auto groupJobBalancerSpec = GetOrDefault(jobManagerSpec->WorkerGroupOverride, workerGroup, jobManagerSpec);
            auto rebalanceResult = balanceSynchronizer->DoBalance(
                flowView,
                ComputationControllers_,
                groupJobBalancerSpec,
                GetTimeSinceSynced(flowView),
                DynamicSpec_->TargetState);
            if (rebalanceResult.SequenceId) {
                flowView->EphemeralState->MaxAppliedBalancerSequenceIds[workerGroup] = *rebalanceResult.SequenceId;
            }

            // Partitions to be moved gracefully.
            THashMap<TPartitionId, std::string> gracefulMovePartitions;
            if (groupJobBalancerSpec->GracefulMove) {
                // Build a map of partitions being moved: partitionId -> target worker address.
                // A partition is being moved when there is both a Del and an Add action for it.
                for (const auto& rebalanceAction : rebalanceResult.Actions) {
                    if (rebalanceAction.Type == NBalancer::ERebalanceActionType::Add) {
                        const auto& partition = flowView->State->ExecutionSpec->Layout->Partitions.at(rebalanceAction.PartitionId);
                        if (partition->CurrentJobId.has_value()) {
                            // There is an existing job for this partition, so this Add is paired with a Del.
                            gracefulMovePartitions[rebalanceAction.PartitionId] = rebalanceAction.WorkerAddress;
                        }
                    }
                }
            }

            for (const auto& rebalanceAction : rebalanceResult.Actions) {
                if (rebalanceAction.Type == NBalancer::ERebalanceActionType::Add) {
                    if (gracefulMovePartitions.contains(rebalanceAction.PartitionId)) {
                        // This Add is handled gracefully: the new job will be created in CheckCompletedPartitions.
                        continue;
                    }
                    YT_ASSERT(flowView->State->Workers.contains(rebalanceAction.WorkerAddress));
                    createJob(rebalanceAction.PartitionId, flowView->State->Workers.at(rebalanceAction.WorkerAddress));
                } else {
                    const auto& partition = flowView->State->ExecutionSpec->Layout->Partitions.at(rebalanceAction.PartitionId);
                    auto it = gracefulMovePartitions.find(rebalanceAction.PartitionId);
                    if (it != gracefulMovePartitions.end()) {
                        gracefulStopJob(partition, it->second);
                    } else {
                        stopJob(partition);
                    }
                }
            }

            for (const auto& preloadAction : rebalanceResult.PreloadResourceActions) {
                auto* existingSpec = layout->WorkerSpecs.FindPtr(preloadAction.WorkerAddress);
                auto workerSpec = existingSpec ? CloneYsonStruct(*existingSpec) : New<TWorkerSpec>();
                if (preloadAction.Type == NBalancer::ERebalanceActionType::Add) {
                    workerSpec->PreloadResources.insert(preloadAction.ResourceId);
                } else {
                    workerSpec->PreloadResources.erase(preloadAction.ResourceId);
                }
                layout->WorkerSpecs.insert_or_assign(preloadAction.WorkerAddress, std::move(workerSpec));
            }
        }

        YT_LOG_INFO("Finished job distributions (Mutations: %v, GracefulStops: %v)", createCount + stopCount + gracefulStopCount, gracefulStopCount);
        DistributingMutationsCounter_.Increment(createCount + stopCount + gracefulStopCount);
    }

    void StopAllJobs(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        // Actually it seems to be safe to iterate over layout->Jobs and remove jobs right in the loop.
        // But on the other hand it's hard to prove, so better to extract IDs before removal.
        // Unfortunately GetKeys does not work here, so extract manually.
        std::vector<TJobId> jobIds;
        jobIds.reserve(layout->Jobs.size());
        for (const auto& [jobId, job] : layout->Jobs) {
            jobIds.emplace_back(jobId);
        }
        for (const auto& jobId : jobIds) {
            layout->RemoveJob(jobId, EJobFinishReason::Stopped);
        }
    }

    TJobManagerStatePtr GetState() override
    {
        for (const auto& [computationId, controller] : ComputationControllers_) {
            controller->Sync();
        }
        StateManager_->Sync();
        return CloneYsonStruct(State_);
    }

    void Commit(const TFlowViewPtr& flowView) override
    {
        for (const auto& [computationId, controller] : ComputationControllers_) {
            controller->UpdateWatermarkState(flowView->State->ExecutionSpec->WatermarkState->GetValue());
            controller->Commit();
        }
    }

private:
    const TJobManagerContextPtr Context_;
    const TPipelineSpecPtr Spec_;
    const TJobManagerStatePtr State_;
    const TStateManagerPtr StateManager_;
    const IPipelineAuthenticatorPtr PipelineAuthenticator_;

    TDynamicPipelineSpecPtr DynamicSpec_;

    const IResourceManagerPtr ResourceManager_;

    THashMap<TComputationId, IComputationControllerPtr> ComputationControllers_;

    NProfiling::TProfiler Profiler_;
    NProfiling::TCounter FailedJobsCounter_;
    NProfiling::TCounter SucceededJobsCounter_;
    NProfiling::TCounter LostJobsCounter_;
    NProfiling::TCounter PartitioningMutationsCounter_;
    NProfiling::TCounter DistributingMutationsCounter_;
    NProfiling::TGauge MaxTimeSincePartitionOkGauge_;

    THashMap<TWorkerGroupId, NBalancer::IBalanceAsyncSynchronizerPtr> BalanceSynchronizers_;

    std::optional<std::pair<i64, TInstant>> UnitedStreamLastEpoch_;

private:
    TResourceManagerContextPtr CreateResourceManagerContext()
    {
        auto context = New<TResourceManagerContext>();
        context->PipelineAuthenticator = PipelineAuthenticator_;
        context->Logger = Logger().WithTag("ResourceManager");
        context->Invoker = Context_->MainCycleInvoker;
        context->Profiler = WithPipelineRelatedTags(ControllerProfiler());
        context->StatusProfiler = Context_->StatusProfiler->WithPrefix("/resource_manager");
        context->IsController = true;
        context->Computations = Spec_->Computations;
        return context;
    }

    NProfiling::TProfiler WithPipelineRelatedTags(NProfiling::TProfiler profiler)
    {
        return profiler
            .WithRequiredTag("pipeline_path", Context_->PipelinePath.GetPath())
            .WithRequiredTag("pipeline_cluster", *Context_->PipelinePath.GetCluster());
    }
};

IJobManagerPtr CreateJobManager(TJobManagerContextPtr context, TPipelineSpecPtr spec, TDynamicPipelineSpecPtr dynamicSpec, TJobManagerStatePtr initialState, IPipelineAuthenticatorPtr pipelineAuthenticator)
{
    return New<TJobManager>(std::move(context), std::move(spec), std::move(dynamicSpec), std::move(initialState), std::move(pipelineAuthenticator));
}

} // namespace NYT::NFlow::NController
