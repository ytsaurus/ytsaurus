#include "job_tracker.h"

#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "operation.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/job_report.h>

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NControllerAgent {

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "JobTracker");
static const auto JobTrackerProfiler = ControllerAgentProfiler.WithPrefix("/job_tracker");
static const auto NodeHeartbeatProfiler = JobTrackerProfiler.WithPrefix("/node_heartbeat");

////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NScheduler;
using namespace NNodeTrackerClient;
using namespace NTracing;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

namespace {

EJobStage JobStageFromJobState(EJobState jobState) noexcept
{
    switch (jobState) {
        case EJobState::Running:
        case EJobState::Waiting:
            return EJobStage::Running;
        case EJobState::Failed:
        case EJobState::Aborted:
        case EJobState::Completed:
            return EJobStage::Finished;
        default:
            YT_ABORT();
    }
}

std::vector<TJobId> CreateJobIdSampleForLogging(const  std::vector<TStartedAllocationInfo>& allocations, int maxSampleSize)
{
    std::vector<TJobId> result;

    result.reserve(size(allocations));
    for (const auto& allocationInfo : allocations) {
        if (allocationInfo.StartedJobInfo) {
            result.push_back(allocationInfo.StartedJobInfo->JobId);

            if (ssize(result) == maxSampleSize) {
                break;
            }
        }
    }

    return result;
}

std::vector<TJobId> CreateJobIdSampleForLogging(const  std::vector<TJobToRelease>& jobs, int maxSampleSize)
{
    std::vector<TJobId> result;

    result.reserve(size(jobs));
    for (const auto& jobInfo : jobs) {
        result.push_back(jobInfo.JobId);

        if (ssize(result) == maxSampleSize) {
            break;
        }
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////

class TJobTracker::TJobTrackerNodeOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerNodeOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        return std::ssize(JobTracker_->RegisteredNodes_);
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            if (std::ssize(keys) >= limit) {
                break;
            }

            keys.push_back(nodeInfo.NodeAddress);
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto nodeIdIt = JobTracker_->NodeAddressToNodeId_.find(key);
        if (nodeIdIt == std::end(JobTracker_->NodeAddressToNodeId_)) {
            return nullptr;
        }

        auto nodeId = nodeIdIt->second;

        YT_VERIFY(JobTracker_->RegisteredNodes_.contains(nodeId));

        auto producer = TYsonProducer(BIND([
            jobTracker = JobTracker_,
            nodeId
        ] (IYsonConsumer* consumer) {
            auto nodeIt = jobTracker->RegisteredNodes_.find(nodeId);

            if (nodeIt == std::end(jobTracker->RegisteredNodes_)) {
                return;
            }

            const auto& nodeJobs = nodeIt->second.Jobs;

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("jobs").DoMap([&] (TFluentMap fluent) {
                        for (const auto& [allocationId, allocationInfo] : nodeJobs.Allocations) {
                            if (const auto& runningJob = allocationInfo.GetRunningJob();
                                runningJob && runningJob->Confirmed) {
                                fluent
                                    .Item(ToString(runningJob->JobId)).BeginMap()
                                    .Item("operation_id").Value(allocationInfo.OperationId)
                                    .Item("stage").Value(EJobStage::Running)
                                .EndMap();
                            }

                            for (const auto& [jobId, _] : allocationInfo.GetFinishedJobs()) {
                                fluent
                                    .Item(ToString(jobId)).BeginMap()
                                    .Item("operation_id").Value(allocationInfo.OperationId)
                                    .Item("stage").Value(EJobStage::Finished)
                                .EndMap();
                            }
                        }
                    })
                    .Item("jobs_waiting_for_confirmation").DoMap([&] (TFluentMap fluent) {
                        for (const auto& [allocationId, allocationInfo] : nodeJobs.Allocations) {
                            if (const auto& runningJob = allocationInfo.GetRunningJob();
                                runningJob && !runningJob->Confirmed) {
                                fluent
                                    .Item(ToString(runningJob->JobId)).BeginMap()
                                        .Item("operation_id").Value(allocationInfo.OperationId)
                                    .EndMap();
                            }
                        }
                    })
                    .Item("jobs_to_release").DoMapFor(nodeJobs.JobsToRelease, [] (TFluentMap fluent, const auto& pair) {
                        const auto& [jobId, releaseFlags] = pair;

                        fluent
                            .Item(ToString(jobId)).BeginMap()
                                .Item("release_flags").Value(ToString(releaseFlags))
                            .EndMap();
                    })
                    .Item("jobs_to_abort").DoMapFor(nodeJobs.JobsToAbort, [] (TFluentMap fluent, const auto& pair) {
                        const auto& [jobId, abortReason] = pair;

                        fluent
                            .Item(ToString(jobId)).BeginMap()
                                .Item("abort_reason").Value(abortReason)
                            .EndMap();
                    })
                .EndMap();
        }));

        return IYPathService::FromProducerLazy(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

class TJobTracker::TJobTrackerAllocationOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerAllocationOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        i64 size = 0;

        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            const auto& nodeJobs = nodeInfo.Jobs;
            size += ssize(nodeJobs.Allocations);
        }
        return size;
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            const auto& nodeJobs = nodeInfo.Jobs;

            for (const auto& [allocationId, allocationInfo] : nodeJobs.Allocations) {
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
                keys.push_back(ToString(allocationId));
            }
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto allocationId = TAllocationId(TGuid::FromString(key));

        auto nodeId = NodeIdFromAllocationId(allocationId);

        auto nodeInfoIt = JobTracker_->RegisteredNodes_.find(nodeId);
        if (nodeInfoIt == std::end(JobTracker_->RegisteredNodes_)) {
            return nullptr;
        }

        TYsonString jobYson;

        const auto& nodeInfo = nodeInfoIt->second;
        const auto& nodeJobs = nodeInfo.Jobs;

        if (const auto* allocation = nodeJobs.FindAllocation(allocationId)) {
            jobYson = BuildYsonStringFluently().BeginMap()
                .Item("allocation_id").Value(allocation->AllocationId)
                .Item("operation_id").Value(allocation->OperationId)
                .Item("node_address").Value(nodeInfo.NodeAddress)
                .Item("finished").Value(allocation->IsFinished())
                .Item("jobs").BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        if (const auto& runningJob = allocation->GetRunningJob()) {
                            fluent
                                .Item(ToString(runningJob->JobId)).BeginMap()
                                    .Item("stage").Value(
                                        runningJob->Confirmed
                                        ? "running"
                                        : "confirmation")
                                .EndMap();
                        }

                        for (const auto& [jobId, finishedJobInfo] : allocation->GetFinishedJobs()) {
                            fluent.Item(ToString(jobId)).BeginMap()
                                .Item("stage").Value("finished")
                            .EndMap();
                        }
                    })
                .EndMap()
                .Item("postponed_allocation_event").Value(ToString(allocation->GetPostponedEvent()))
            .EndMap();
        }

        if (!jobYson) {
            return nullptr;
        }

        auto producer = TYsonProducer(BIND([yson = std::move(jobYson)] (IYsonConsumer* consumer) {
            consumer->OnRaw(yson);
        }));

        return IYPathService::FromProducer(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

class TJobTracker::TJobTrackerJobOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerJobOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        i64 size = 0;

        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            const auto& nodeJobs = nodeInfo.Jobs;
            size += nodeJobs.GetJobCount() +
                nodeJobs.GetJobToConfirmCount() +
                std::ssize(nodeJobs.JobsToRelease) +
                std::ssize(nodeJobs.JobsToAbort);
        }
        return size;
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            const auto& nodeJobs = nodeInfo.Jobs;

            for (const auto& [_, allocationInfo] : nodeJobs.Allocations) {
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
                if (const auto& runningJob = allocationInfo.GetRunningJob()) {
                    keys.push_back(ToString(runningJob->JobId));
                }

                for (const auto& [jobId, _] : allocationInfo.GetFinishedJobs()) {
                    if (std::ssize(keys) >= limit) {
                        return keys;
                    }
                    keys.push_back(ToString(jobId));
                }
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToAbort) {
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
                keys.push_back(ToString(jobId));
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToRelease) {
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
                keys.push_back(ToString(jobId));
            }
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto jobId = TJobId(TGuid::FromString(key));

        auto nodeId = NodeIdFromJobId(jobId);

        auto nodeInfoIt = JobTracker_->RegisteredNodes_.find(nodeId);
        if (nodeInfoIt == std::end(JobTracker_->RegisteredNodes_)) {
            return nullptr;
        }

        TYsonString jobYson;

        const auto& [
            nodeJobs,
            _,
            registrationId,
            nodeAddress,
            sequenceNumber
        ] = nodeInfoIt->second;

        if (const auto* allocation = nodeJobs.FindAllocation(jobId)) {
            std::optional<TString> jobStageString;
            if (allocation->HasRunningJob(jobId)) {
                jobStageString = allocation->GetRunningJob()->Confirmed ? FormatEnum(EJobStage::Running) : "confirmation";
            } else if (allocation->GetFinishedJobs().contains(jobId)) {
                jobStageString = FormatEnum(EJobStage::Finished);
            }

            if (jobStageString) {
                jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value(*jobStageString)
                    .Item("operation_id").Value(allocation->OperationId)
                    .Item("allocation_id").Value(allocation->AllocationId)
                    .Item("node_address").Value(nodeAddress)
                    .Item("postponed_allocation_event").Value(ToString(allocation->GetPostponedEvent()))
                .EndMap();
            }
        } else if (auto jobToAbortIt = nodeJobs.JobsToAbort.find(jobId);
            jobToAbortIt != std::end(nodeJobs.JobsToAbort))
        {
            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value("aborting")
                    .Item("abort_reason").Value(jobToAbortIt->second)
                    .Item("node_address").Value(nodeAddress)
                    .Item("allocation_id").Value(AllocationIdFromJobId(jobId))
                .EndMap();
        } else if (auto jobToReleaseIt = nodeJobs.JobsToRelease.find(jobId);
            jobToReleaseIt != std::end(nodeJobs.JobsToRelease))
        {
            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value("releasing")
                    .Item("release_flags").Value(ToString(jobToReleaseIt->second))
                    .Item("node_address").Value(nodeAddress)
                    .Item("allocation_id").Value(AllocationIdFromJobId(jobId))
                .EndMap();
        }

        if (!jobYson) {
            return nullptr;
        }

        auto producer = TYsonProducer(BIND([yson = std::move(jobYson)] (IYsonConsumer* consumer) {
            consumer->OnRaw(yson);
        }));

        return IYPathService::FromProducer(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

class TJobTracker::TJobTrackerOperationOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerOperationOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        return std::ssize(JobTracker_->RegisteredOperations_);
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [operationId, operationInfo] : JobTracker_->RegisteredOperations_) {
            if (std::ssize(keys) >= limit) {
                break;
            }

            keys.push_back(ToString(operationId));
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto operationId = TOperationId(TGuid::FromString(key));
        auto operationInfoIt = JobTracker_->RegisteredOperations_.find(operationId);
        if (operationInfoIt == std::end(JobTracker_->RegisteredOperations_)) {
            return nullptr;
        }

        const auto& operationInfo = operationInfoIt->second;

        auto traceContextGuard = CreateOperationTraceContextGuard(
            "JobTrackerOperationOrchidService",
            operationId);

        auto producer = TYsonProducer(BIND([
            jobsReady = operationInfo.JobsReady,
            operationId,
            jobTracker = JobTracker_
        ] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("jobs_ready").Value(jobsReady)
                    .Item("allocations").Do([&] (TFluentAny innerFluent) {
                        const auto& operationIt = jobTracker->RegisteredOperations_.find(operationId);

                        if (operationIt == std::end(jobTracker->RegisteredOperations_)) {
                            return;
                        }

                        innerFluent.DoListFor(operationIt->second.TrackedAllocationIds, [] (TFluentList fluent, TAllocationId allocationId) {
                            fluent
                                .Item().Value(allocationId);
                        });
                    })
                .EndMap();
        }));

        return IYPathService::FromProducerLazy(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

////////////////////////////////////////////////////////////////////

TJobTracker::TAllocationInfo*
TJobTracker::TNodeJobs::FindAllocation(TAllocationId allocationId)
{
    YT_VERIFY(allocationId);

    auto allocationIt = Allocations.find(allocationId);
    return allocationIt != end(Allocations) ? &allocationIt->second : nullptr;
}

TJobTracker::TAllocationInfo*
TJobTracker::TNodeJobs::FindAllocation(TJobId jobId)
{
    return FindAllocation(AllocationIdFromJobId(jobId));
}

const TJobTracker::TAllocationInfo*
TJobTracker::TNodeJobs::FindAllocation(TAllocationId allocationId) const
{
    YT_VERIFY(allocationId);

    auto allocationIt = Allocations.find(allocationId);
    return allocationIt != end(Allocations) ? &allocationIt->second : nullptr;
}

const TJobTracker::TAllocationInfo*
TJobTracker::TNodeJobs::FindAllocation(TJobId jobId) const
{
    return FindAllocation(AllocationIdFromJobId(jobId));
}

i64 TJobTracker::TNodeJobs::GetJobCount() const
{
    i64 count = 0;

    for (const auto& [_, allocationInfo] : Allocations) {
        count += ssize(allocationInfo.GetFinishedJobs()) + allocationInfo.GetRunningJob().has_value();
    }

    return count;
}

i64 TJobTracker::TNodeJobs::GetJobToConfirmCount() const
{
    i64 count = 0;

    for (const auto& [_, allocation] : Allocations) {
        if (const auto& runningJob = allocation.GetRunningJob();
            runningJob && !runningJob->Confirmed) {
            ++count;
        }
    }

    return count;
}

bool TJobTracker::TNodeInfo::CheckHeartbeatSequenceNumber(ui64 sequenceNumber)
{
    if (sequenceNumber == 0) {
        LastHeartbeatSequenceNumber = 0;
        return true;
    }

    if (sequenceNumber > LastHeartbeatSequenceNumber) {
        LastHeartbeatSequenceNumber = sequenceNumber;
        return true;
    }

    return false;
}

TJobTracker::TAllocationInfo::TAllocationInfo(
    TOperationId operationId,
    TAllocationId allocationId)
    : OperationId(operationId)
    , AllocationId(allocationId)
{
    YT_VERIFY(AllocationId);
    YT_VERIFY(OperationId);
}

bool TJobTracker::TAllocationInfo::IsEmpty() const noexcept
{
    return !RunningJob_ && FinishedJobs_.empty();
}

bool TJobTracker::TAllocationInfo::HasJob(TJobId jobId) const noexcept
{
    YT_VERIFY(jobId);

    return HasRunningJob(jobId) || FinishedJobs_.contains(jobId);
}

bool TJobTracker::TAllocationInfo::HasRunningJob(TJobId jobId) const noexcept
{
    YT_VERIFY(jobId);

    return RunningJob_.value_or(TRunningJobInfo{}).JobId == jobId;
}

std::optional<EJobStage> TJobTracker::TAllocationInfo::GetJobStage(TJobId jobId) const
{
    YT_VERIFY(jobId);

    if (HasRunningJob(jobId)) {
        return EJobStage::Running;
    }

    if (FinishedJobs_.contains(jobId)) {
        return EJobStage::Finished;
    }

    return std::nullopt;
}

bool TJobTracker::TAllocationInfo::ShouldBeRemoved() const noexcept
{
    return IsEmpty() && Finished_;
}

const std::optional<TJobTracker::TRunningJobInfo>& TJobTracker::TAllocationInfo::GetRunningJob() const
{
    return RunningJob_;
}

const THashMap<TJobId, TJobTracker::TFinishedJobInfo>& TJobTracker::TAllocationInfo::GetFinishedJobs() const
{
    return FinishedJobs_;
}

bool TJobTracker::TAllocationInfo::IsFinished() const noexcept
{
    return Finished_;
}

const std::optional<TSchedulerToAgentAllocationEvent>& TJobTracker::TAllocationInfo::GetPostponedEvent() const noexcept
{
    return PostponedAllocationEvent_;
}

void TJobTracker::TAllocationInfo::SetDisappearedFromNodeSince(TInstant from) noexcept
{
    YT_VERIFY(RunningJob_);

    RunningJob_->DisappearedFromNodeSince = from;
}

TJobTracker::TRequestedActionInfo& TJobTracker::TAllocationInfo::GetMutableRunningJobRequestedActionInfo() noexcept
{
    YT_VERIFY(RunningJob_);

    return RunningJob_->RequestedActionInfo;
}

void TJobTracker::TAllocationInfo::StartJob(TJobId jobId, bool confirmed)
{
    YT_VERIFY(jobId);
    YT_VERIFY(!RunningJob_);
    YT_VERIFY(!Finished_);

    RunningJob_.emplace(TRunningJobInfo{
        .JobId = jobId,
        .Confirmed = confirmed,
    });
}

bool TJobTracker::TAllocationInfo::ConfirmRunningJob() noexcept
{
    YT_VERIFY(RunningJob_);

    return !std::exchange(RunningJob_->Confirmed, true);
}

void TJobTracker::TAllocationInfo::FinishRunningJob() noexcept
{
    YT_VERIFY(RunningJob_);

    EmplaceOrCrash(
        FinishedJobs_,
        RunningJob_->JobId,
        TFinishedJobInfo());

    RunningJob_.reset();
}

void TJobTracker::TAllocationInfo::EraseRunningJobOrCrash()
{
    YT_VERIFY(RunningJob_);

    RunningJob_.reset();
}

bool TJobTracker::TAllocationInfo::EraseFinishedJob(TJobId jobId)
{
    return FinishedJobs_.erase(jobId);
}

void TJobTracker::TAllocationInfo::FinishAndClearJobs() noexcept
{
    RunningJob_.reset();
    FinishedJobs_.clear();
    Finished_ = true;
}

void TJobTracker::TAllocationInfo::Finish(TSchedulerToAgentAllocationEvent&& event)
{
    YT_LOG_FATAL_IF(
        Finished_ || PostponedAllocationEvent_,
        "Event happened to already finished allocation (AllocationId: %v, Finished: %v, CurrentPostponedEvent: %v, NodeId: %v, NewEvent: %v)",
        AllocationId,
        Finished_,
        PostponedAllocationEvent_,
        NodeIdFromAllocationId(AllocationId),
        event);

    Finished_ = true;
    PostponedAllocationEvent_ = std::move(event);
}

TSchedulerToAgentAllocationEvent TJobTracker::TAllocationInfo::ConsumePostponedEventOrCrash()
{
    YT_VERIFY(PostponedAllocationEvent_);

    auto movedEvent = std::move(*PostponedAllocationEvent_);
    PostponedAllocationEvent_.reset();

    return movedEvent;
}

template <class TEventType>
TEventType TJobTracker::TAllocationInfo::ConsumePostponedEventOrCrash()
{
    auto event = ConsumePostponedEventOrCrash();

    return std::move(GetEventOrCrash<TEventType>(event));
}

template <class TEvent>
TEvent& TJobTracker::TAllocationInfo::GetEventOrCrash(TSchedulerToAgentAllocationEvent& event)
{
    auto* typedEvent = std::get_if<TEvent>(&event.EventSummary);
    YT_LOG_FATAL_UNLESS(
        typedEvent,
        "Unexpected allocation event type (Event: %v)",
        event);
    return *typedEvent;
}

////////////////////////////////////////////////////////////////////

TJobTracker::TJobTracker(TBootstrap* bootstrap, TJobReporterPtr jobReporter)
    : Bootstrap_(bootstrap)
    , JobReporter_(std::move(jobReporter))
    , Config_(bootstrap->GetConfig()->ControllerAgent->JobTracker)
    , JobEventsControllerQueue_(bootstrap->GetConfig()->ControllerAgent->JobEventsControllerQueue)
    , HeartbeatStatisticsBytes_(NodeHeartbeatProfiler.WithHot().Counter("/statistics_bytes"))
    , HeartbeatDataStatisticsBytes_(NodeHeartbeatProfiler.WithHot().Counter("/data_statistics_bytes"))
    , HeartbeatJobResultBytes_(NodeHeartbeatProfiler.WithHot().Counter("/job_result_bytes"))
    , HeartbeatProtoMessageBytes_(NodeHeartbeatProfiler.WithHot().Counter("/proto_message_bytes"))
    , HeartbeatEnqueuedControllerEvents_(NodeHeartbeatProfiler.WithHot().GaugeSummary("/enqueued_controller_events"))
    , HeartbeatCount_(NodeHeartbeatProfiler.WithHot().Counter("/count"))
    , StaleHeartbeatCount_(NodeHeartbeatProfiler.WithHot().Counter("/stale_count"))
    , ReceivedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/job_count"))
    , ReceivedUnknownOperationCount_(NodeHeartbeatProfiler.WithHot().Counter("/unknown_operation_count"))
    , ReceivedRunningJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/running_job_count"))
    , ReceivedStaleRunningJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/stale_running_job_count"))
    , ReceivedFinishedJobCount_(NodeHeartbeatProfiler.Counter("/finished_job_count"))
    , ReceivedDuplicatedFinishedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/duplicated_finished_job_count"))
    , ReceivedUnknownJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/unknown_job_count"))
    , UnconfirmedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/unconfirmed_job_count"))
    , ConfirmedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/confirmed_job_count"))
    , DisappearedFromNodeJobAbortCount_(NodeHeartbeatProfiler.WithHot().Counter("/disappeared_from_node_job_count"))
    , JobAbortRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_abort_request_count"))
    , JobReleaseRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_release_request_count"))
    , JobInterruptionRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_interruption_request_count"))
    , JobFailureRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_failure_request_count"))
    , NodeRegistrationCount_(JobTrackerProfiler.WithHot().Counter("/node_registration_count"))
    , NodeUnregistrationCount_(JobTrackerProfiler.WithHot().Counter("/node_unregistration_count"))
    , ThrottledRunningJobEventCount_(NodeHeartbeatProfiler.WithHot().Counter("/throttled_running_job_event_count"))
    , ThrottledHeartbeatCount_(NodeHeartbeatProfiler.WithHot().Counter("/throttled_heartbeat_count"))
    , ThrottledOperationCount_(NodeHeartbeatProfiler.WithHot().Counter("/throttled_operation_count"))
    , WrongIncarnationRequestCount_(JobTrackerProfiler.WithHot().Counter("/wrong_incarnation_request_count"))
    , JobTrackerQueue_(New<NConcurrency::TActionQueue>("JobTracker"))
    , ExecNodes_(New<TRefCountedExecNodeDescriptorMap>())
    , OrchidService_(CreateOrchidService())
{ }

TFuture<void> TJobTracker::Initialize()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    auto cancelableInvoker = Bootstrap_->GetControllerAgent()->CreateCancelableInvoker(
        GetInvoker());

    // It is called in serialized invoker to prevent reordering with DoCleanup.
    return BIND(
        &TJobTracker::DoInitialize,
        MakeStrong(this),
        Passed(std::move(cancelableInvoker)))
        .AsyncVia(GetInvoker())
        .Run();
}

void TJobTracker::OnSchedulerConnected(TIncarnationId incarnationId)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    GetCancelableInvoker()->Invoke(BIND(
        &TJobTracker::SetIncarnationId,
        MakeStrong(this),
        incarnationId));
}

void TJobTracker::Cleanup()
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetInvoker()->Invoke(BIND(
        &TJobTracker::DoCleanup,
        MakeStrong(this)));
}

void TJobTracker::ProcessHeartbeat(const TJobTracker::TCtxHeartbeatPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* request = &context->Request();

    auto incarnationId = FromProto<TIncarnationId>(request->controller_agent_incarnation_id());
    auto nodeDescriptor = FromProto<TNodeDescriptor>(request->node_descriptor());

    auto nodeId = FromProto<TNodeId>(request->node_id());

    if (nodeId == InvalidNodeId) {
        THROW_ERROR_EXCEPTION(
            "Invalid node id; node is likely offline");
    }

    if (!incarnationId) {
        WrongIncarnationRequestCount_.Increment();
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::AgentDisconnected,
            "Controller agent disconnected");
    }

    ProfileHeartbeatRequest(request);
    THashMap<TOperationId, std::vector<std::unique_ptr<TJobSummary>>> groupedJobSummaries;
    for (auto& job : *request->mutable_jobs()) {
        auto operationId = FromProto<TOperationId>(job.operation_id());

        auto traceContextGuard = CreateOperationTraceContextGuard(
            "ProcessHeartbeat",
            operationId);

        auto jobSummary = ParseJobSummary(&job, Logger());
        groupedJobSummaries[operationId].push_back(std::move(jobSummary));
    }

    THashSet<TAllocationId> allocationIdsRunningOnNode;
    allocationIdsRunningOnNode.reserve(std::size(request->allocations()));

    for (const auto& allocationInfoProto : request->allocations()) {
        allocationIdsRunningOnNode.insert(
            FromProto<TAllocationId>(allocationInfoProto.allocation_id()));
    }

    auto unconfirmedJobs = NYT::FromProto<std::vector<TJobId>>(request->unconfirmed_job_ids());

    THeartbeatProcessingContext heartbeatProcessingContext{
        .RpcContext = context,
        .Logger = NControllerAgent::Logger().WithTag(
            "NodeId: %v, NodeAddress: %v",
            nodeId,
            nodeDescriptor.GetDefaultAddress()),
        .NodeAddress = nodeDescriptor.GetDefaultAddress(),
        .NodeId = nodeId,
        .IncarnationId = incarnationId,
        .Request = THeartbeatRequest{
            .GroupedJobSummaries = std::move(groupedJobSummaries),
            .AllocationIdsRunningOnNode = std::move(allocationIdsRunningOnNode),
            .UnconfirmedJobIds = std::move(unconfirmedJobs),
        },
    };

    BIND(
        &TJobTracker::DoProcessHeartbeat,
        Unretained(this),
        Passed(std::move(heartbeatProcessingContext)))
        .AsyncVia(GetCancelableInvokerOrThrow())
        .Run()
        .SubscribeUnique(BIND([this, context] (TErrorOr<THeartbeatProcessingResult>&& heartbeatProcessingResultOrError) {
            if (!heartbeatProcessingResultOrError.IsOK()) {
                context->Reply(std::move(heartbeatProcessingResultOrError));
                return;
            }
            auto heartbeatProcessingResult = std::move(heartbeatProcessingResultOrError.Value());
            ProfileHeartbeatProperties(heartbeatProcessingResult.Counters);

            heartbeatProcessingResult.Context.RpcContext->Reply();
        })
            .Via(GetCurrentInvoker()));
}

void TJobTracker::SettleJob(const TJobTracker::TCtxSettleJobPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* request = &context->Request();
    auto* response = &context->Response();

    auto incarnationId = FromProto<TIncarnationId>(request->controller_agent_incarnation_id());
    auto nodeDescriptor = FromProto<TNodeDescriptor>(request->node_descriptor());

    auto nodeId = FromProto<TNodeId>(request->node_id());
    auto allocationId = FromProto<TAllocationId>(request->allocation_id());
    auto operationId = FromProto<TOperationId>(request->operation_id());

    if (nodeId == InvalidNodeId) {
        THROW_ERROR_EXCEPTION(
            "Invalid node id; node is likely offline");
    }

    THROW_ERROR_EXCEPTION_IF(
        !incarnationId,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    auto Logger = NControllerAgent::Logger().WithTag(
        "NodeId: %v, NodeAddress: %v, OperationId: %v, AllocationId: %v",
        nodeId,
        nodeDescriptor.GetDefaultAddress(),
        operationId,
        allocationId);

    SwitchTo(GetCancelableInvokerOrThrow());

    THROW_ERROR_EXCEPTION_IF(
        !IncarnationId_,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    if (incarnationId != IncarnationId_) {
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::IncarnationMismatch,
            "Controller agent incarnation mismatch: expected %v, got %v)",
            IncarnationId_,
            incarnationId);
    }

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        YT_LOG_INFO("Node is not registered in job tracker; skip settle job request");

        THROW_ERROR_EXCEPTION("Node is not registered in job tracker", allocationId)
            << TErrorAttribute("incarnation_id", IncarnationId_);
    }

    auto* allocationInfo = nodeInfo->Jobs.FindAllocation(allocationId);
    if (!allocationInfo) {
        YT_LOG_INFO("Allocation is unknown; skip settle job request");

        THROW_ERROR_EXCEPTION("No such allocation %v", allocationId);
    }

    if (allocationInfo->IsFinished()) {
        YT_LOG_INFO("Allocation is already finished; skip settle job request");

        THROW_ERROR_EXCEPTION("Allocation %v is already finished", allocationId);
    }

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == std::end(RegisteredOperations_)) {
        YT_LOG_INFO("Operation is not registered in job tracker; skip settle job request");

        THROW_ERROR_EXCEPTION("No such operation %v", operationId);
    }

    const auto& operationInfo = operationIt->second;

    auto operationController = operationInfo.OperationController.Lock();
    if (!operationController) {
        YT_LOG_INFO("Operation controller is already reset, skip settle job request");

        THROW_ERROR_EXCEPTION("Operation %v controller is already reset", operationId);
    }

    if (!operationInfo.JobsReady) {
        YT_LOG_INFO("Operation jobs are not ready yet, skip settle job request");

        THROW_ERROR_EXCEPTION("Operation %v jobs are not ready yet", operationId);
    }

    auto asyncJobInfo = BIND(
        &IOperationController::SettleJob,
        operationController,
        allocationId)
        .AsyncVia(operationController->GetCancelableInvoker(EOperationControllerQueue::GetJobSpec))
        .Run();

    auto jobInfoOrError = WaitFor(
        asyncJobInfo,
        NRpc::TDispatcher::Get()->GetHeavyInvoker());

    // NB(pogorelov): Allocation may finish concurrently.
    allocationInfo = nodeInfo->Jobs.FindAllocation(allocationId);
    if (!allocationInfo) {
        YT_LOG_INFO("Allocation is unknown; skip settle job request");

        THROW_ERROR_EXCEPTION("No such allocation %v", allocationId);
    }

    if (allocationInfo->IsFinished()) {
        YT_LOG_INFO("Allocation is already finished; skip settle job request");

        THROW_ERROR_EXCEPTION("Allocation %v is already finished", allocationId);
    }

    if (!jobInfoOrError.IsOK() || !jobInfoOrError.Value().JobSpecBlob) {
        auto error = !jobInfoOrError.IsOK()
            ? static_cast<TError>(jobInfoOrError)
            : TError("Controller returned empty job spec (has controller crashed?)");
        YT_LOG_INFO(
            error,
            "Failed to extract job spec");

        ToProto(response->mutable_error(), error);

        context->Reply();

        return;
    }

    auto& jobInfo = jobInfoOrError.Value();
    ToProto(response->mutable_job_info()->mutable_job_id(), jobInfo.JobId);
    response->Attachments().push_back(std::move(jobInfo.JobSpecBlob));

    context->Reply();
}

TJobTrackerOperationHandlerPtr TJobTracker::RegisterOperation(
    TOperationId operationId,
    TWeakPtr<IOperationController> operationController)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto cancelableInvoker = GetCancelableInvoker();

    cancelableInvoker->Invoke(BIND(
        &TJobTracker::DoRegisterOperation,
        MakeStrong(this),
        operationId,
        Passed(std::move(operationController))));

    return New<TJobTrackerOperationHandler>(this, std::move(cancelableInvoker), operationId);
}

void TJobTracker::UnregisterOperation(
    TOperationId operationId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetCancelableInvoker()->Invoke(BIND(
        &TJobTracker::DoUnregisterOperation,
        MakeStrong(this),
        operationId));
}

void TJobTracker::UpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetCancelableInvoker()->Invoke(BIND(
        &TJobTracker::DoUpdateExecNodes,
        MakeStrong(this),
        Passed(std::move(newExecNodes))));
}

void TJobTracker::UpdateConfig(const TControllerAgentConfigPtr& config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetInvoker()->Invoke(BIND(
        &TJobTracker::DoUpdateConfig,
        MakeStrong(this),
        config));
}

NYTree::IYPathServicePtr TJobTracker::GetOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OrchidService_;
}

void TJobTracker::ProfileHeartbeatRequest(const NProto::TReqHeartbeat* request)
{
    i64 totalJobStatisticsSize = 0;
    i64 totalJobDataStatisticsSize = 0;
    i64 totalJobResultSize = 0;
    for (const auto& job : request->jobs()) {
        if (job.has_statistics()) {
            totalJobStatisticsSize += std::size(job.statistics());
        }
        if (job.has_result()) {
            totalJobResultSize += job.result().ByteSizeLong();
        }
        for (const auto& dataStatistics : job.output_data_statistics()) {
            totalJobDataStatisticsSize += dataStatistics.ByteSizeLong();
        }
        totalJobStatisticsSize += job.total_input_data_statistics().ByteSizeLong();
    }

    HeartbeatProtoMessageBytes_.Increment(request->ByteSizeLong());
    HeartbeatStatisticsBytes_.Increment(totalJobStatisticsSize);
    HeartbeatDataStatisticsBytes_.Increment(totalJobDataStatisticsSize);
    HeartbeatJobResultBytes_.Increment(totalJobResultSize);
    HeartbeatCount_.Increment();
    ReceivedJobCount_.Increment(request->jobs_size());
}

void TJobTracker::AccountEnqueuedControllerEvent(int delta)
{
    auto newValue = EnqueuedControllerEventCount_.fetch_add(delta) + delta;
    HeartbeatEnqueuedControllerEvents_.Update(newValue);
}

void TJobTracker::ProfileHeartbeatProperties(const THeartbeatCounters& heartbeatCounters)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ReceivedRunningJobCount_.Increment(heartbeatCounters.RunningJobCount);
    ReceivedStaleRunningJobCount_.Increment(heartbeatCounters.StaleRunningJobCount);
    ReceivedFinishedJobCount_.Increment(heartbeatCounters.FinishedJobCount);
    ReceivedDuplicatedFinishedJobCount_.Increment(heartbeatCounters.DuplicatedFinishedJobCount);
    ReceivedUnknownJobCount_.Increment(heartbeatCounters.UnknownJobCount);
    UnconfirmedJobCount_.Increment(heartbeatCounters.UnconfirmedJobCount);
    ConfirmedJobCount_.Increment(heartbeatCounters.ConfirmedJobCount);
    DisappearedFromNodeJobAbortCount_.Increment(heartbeatCounters.DisappearedFromNodeJobAbortCount);
    JobAbortRequestCount_.Increment(heartbeatCounters.JobAbortRequestCount);
    JobReleaseRequestCount_.Increment(heartbeatCounters.JobReleaseRequestCount);
    JobInterruptionRequestCount_.Increment(heartbeatCounters.JobInterruptionRequestCount);
    JobFailureRequestCount_.Increment(heartbeatCounters.JobFailureRequestCount);
    ThrottledRunningJobEventCount_.Increment(heartbeatCounters.ThrottledRunningJobEventCount);
    ThrottledHeartbeatCount_.Increment(heartbeatCounters.ThrottledRunningJobEventCount > 0);
    ThrottledOperationCount_.Increment(heartbeatCounters.ThrottledOperationCount);
}

void TJobTracker::TOperationUpdatesProcessingContext::AddAllocationEvent(TSchedulerToAgentAllocationEvent&& event)
{
    if (auto* abortedEvent = std::get_if<TAbortedAllocationSummary>(&event.EventSummary)) {
        AbortedAllocations.push_back(std::move(*abortedEvent));
    } else {
        auto& finishedEvent = TAllocationInfo::GetEventOrCrash<TFinishedAllocationSummary>(event);
        FinishedAllocations.push_back(std::move(finishedEvent));
    }
}

IInvokerPtr TJobTracker::GetInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobTrackerQueue_->GetInvoker();
}

IInvokerPtr TJobTracker::TryGetCancelableInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableInvoker_.Acquire();
}

IInvokerPtr TJobTracker::GetCancelableInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto invoker = TryGetCancelableInvoker();
    YT_VERIFY(invoker);

    return invoker;
}

IInvokerPtr TJobTracker::GetCancelableInvokerOrThrow() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto invoker = TryGetCancelableInvoker();
    THROW_ERROR_EXCEPTION_IF(
        !invoker,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Job tracker disconnected");

    return invoker;
}

NYTree::IYPathServicePtr TJobTracker::CreateOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto service = New<TCompositeMapService>();

    service->AddChild("nodes", New<TJobTrackerNodeOrchidService>(this));

    service->AddChild("allocations", New<TJobTrackerAllocationOrchidService>(this));

    service->AddChild("jobs", New<TJobTrackerJobOrchidService>(this));

    service->AddChild("operations", New<TJobTrackerOperationOrchidService>(this));

    return service->Via(GetInvoker());
}

void TJobTracker::DoUpdateConfig(const TControllerAgentConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    Config_ = config->JobTracker;
    JobEventsControllerQueue_ = config->JobEventsControllerQueue;
}

void TJobTracker::DoUpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    std::swap(newExecNodes, ExecNodes_);

    NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
        BIND([oldExecNodesToDestroy = std::move(newExecNodes)] {}));
}

TJobTracker::THeartbeatProcessingResult TJobTracker::DoProcessHeartbeat(
    THeartbeatProcessingContext heartbeatProcessingContext)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    THROW_ERROR_EXCEPTION_IF(
        !IncarnationId_,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    if (heartbeatProcessingContext.IncarnationId != IncarnationId_) {
        WrongIncarnationRequestCount_.Increment();
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::IncarnationMismatch,
            "Controller agent incarnation mismatch: expected %v, got %v)",
            IncarnationId_,
            heartbeatProcessingContext.IncarnationId);
    }

    auto* request = &heartbeatProcessingContext.RpcContext->Request();

    TForbidContextSwitchGuard guard;

    auto& nodeInfo = UpdateOrRegisterNode(
        heartbeatProcessingContext.NodeId,
        heartbeatProcessingContext.NodeAddress);

    if (Config_->CheckNodeHeartbeatSequenceNumber && request->has_sequence_number()) {
        if (!nodeInfo.CheckHeartbeatSequenceNumber(request->sequence_number())) {
            StaleHeartbeatCount_.Increment();
        }
    }

    THeartbeatProcessingResult heartbeatProcessingResult{};
    THashMap<TOperationId, TOperationUpdatesProcessingContext> operationIdToUpdatesProcessingContext;
    operationIdToUpdatesProcessingContext.reserve(std::size(heartbeatProcessingContext.Request.GroupedJobSummaries));

    DoProcessJobInfosInHeartbeat(
        operationIdToUpdatesProcessingContext,
        nodeInfo,
        heartbeatProcessingContext,
        heartbeatProcessingResult);

    DoProcessAllocationsInHeartbeat(
        operationIdToUpdatesProcessingContext,
        nodeInfo,
        heartbeatProcessingContext,
        heartbeatProcessingResult);

    heartbeatProcessingResult.Context = std::move(heartbeatProcessingContext);

    ProcessOperationContexts(std::move(operationIdToUpdatesProcessingContext));

    return heartbeatProcessingResult;
}

void TJobTracker::DoProcessUnconfirmedJobsInHeartbeat(
    THashMap<TOperationId, TOperationUpdatesProcessingContext>& operationIdToUpdatesProcessingContext,
    TNodeInfo& nodeInfo,
    THeartbeatProcessingContext& heartbeatProcessingContext,
    THeartbeatProcessingResult& heartbeatProcessingResult)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto& heartbeatCounters = heartbeatProcessingResult.Counters;
    auto& nodeJobs = nodeInfo.Jobs;

    for (auto jobId : heartbeatProcessingContext.Request.UnconfirmedJobIds) {
        auto allocationIt = nodeJobs.Allocations.find(AllocationIdFromJobId(jobId));
        if (allocationIt == std::end(nodeJobs.Allocations)) {
            continue;
        }

        auto& allocation = allocationIt->second;

        if (allocation.HasRunningJob(jobId) && !allocation.GetRunningJob()->Confirmed) {
            auto operationId = allocation.OperationId;

            auto& context = AddOperationUpdatesProcessingContext(
                operationIdToUpdatesProcessingContext,
                heartbeatProcessingContext,
                heartbeatProcessingResult,
                operationId);

            const auto& Logger = context.OperationLogger;

            YT_LOG_INFO(
                "Job unconfirmed; aborting it (JobId: %v, AllocationId: %v)",
                jobId,
                allocation.AllocationId);

            if (context.OperationController) {
                context.JobsToAbort.push_back(TJobToAbort{
                    .JobId = jobId,
                    .AbortReason = EAbortReason::Unconfirmed,
                });
            }

            allocation.EraseRunningJobOrCrash();

            if (auto allocationInfo = EraseAllocationIfNeeded(nodeJobs, allocationIt);
                allocationInfo && allocationInfo->GetPostponedEvent())
            {
                YT_LOG_INFO(
                    "Processing postponed allocation event (AllocationId: %v, AllocationEvent: %v)",
                    allocationInfo->AllocationId,
                    allocationInfo->GetPostponedEvent());

                if (context.OperationController) {
                    context.AddAllocationEvent(allocationInfo->ConsumePostponedEventOrCrash());
                }
            }
        }
    }

    heartbeatCounters.UnconfirmedJobCount = std::ssize(heartbeatProcessingContext.Request.UnconfirmedJobIds);
}

THashSet<TJobId> TJobTracker::DoProcessAbortedAndReleasedJobsInHeartbeat(
    TNodeInfo& nodeInfo,
    THeartbeatProcessingContext& heartbeatProcessingContext,
    THeartbeatProcessingResult& heartbeatProcessingResult)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto* response = &heartbeatProcessingContext.RpcContext->Response();
    auto& heartbeatCounters = heartbeatProcessingResult.Counters;
    auto& nodeJobs = nodeInfo.Jobs;

    THashSet<TJobId> jobsToSkip;
    jobsToSkip.reserve(std::size(nodeJobs.JobsToAbort) + std::size(nodeJobs.JobsToRelease));

    for (auto [jobId, abortReason] : nodeJobs.JobsToAbort) {
        EmplaceOrCrash(jobsToSkip, jobId);

        YT_LOG_INFO(
            "Request node to abort job (JobId: %v)",
            jobId);
        NProto::ToProto(
            response->add_jobs_to_abort(),
            TJobToAbort{
                .JobId = jobId,
                .AbortReason = abortReason
            });
        ++heartbeatCounters.JobAbortRequestCount;
    }

    nodeJobs.JobsToAbort.clear();

    {
        std::vector<TJobId> releasedJobs;
        releasedJobs.reserve(std::size(nodeJobs.JobsToRelease));
        for (const auto& [jobId, releaseFlags] : nodeJobs.JobsToRelease) {
            // NB(pogorelov): Sometimes we abort job and release it immediately. Node might release such jobs by itself, but it is less flexible.
            if (auto [it, inserted] = jobsToSkip.emplace(jobId); !inserted) {
                continue;
            }

            releasedJobs.push_back(jobId);

            YT_LOG_INFO(
                "Request node to remove job (JobId: %v, ReleaseFlags: %v)",
                jobId,
                releaseFlags);
            ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId, releaseFlags});
            ++heartbeatCounters.JobReleaseRequestCount;
        }

        for (auto jobId : releasedJobs) {
            EraseOrCrash(nodeJobs.JobsToRelease, jobId);
        }
    }

    return jobsToSkip;
}

void TJobTracker::DoProcessJobInfosInHeartbeat(
    THashMap<TOperationId, TOperationUpdatesProcessingContext>& operationIdToUpdatesProcessingContext,
    TNodeInfo& nodeInfo,
    THeartbeatProcessingContext& heartbeatProcessingContext,
    THeartbeatProcessingResult& heartbeatProcessingResult)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto* response = &heartbeatProcessingContext.RpcContext->Response();
    auto& heartbeatCounters = heartbeatProcessingResult.Counters;
    auto& nodeJobs = nodeInfo.Jobs;

    DoProcessUnconfirmedJobsInHeartbeat(
        operationIdToUpdatesProcessingContext,
        nodeInfo,
        heartbeatProcessingContext,
        heartbeatProcessingResult);

    auto jobsToSkip = DoProcessAbortedAndReleasedJobsInHeartbeat(
        nodeInfo,
        heartbeatProcessingContext,
        heartbeatProcessingResult);

    for (auto& [operationId, jobSummaries] : heartbeatProcessingContext.Request.GroupedJobSummaries) {
        auto traceContextGuard = CreateOperationTraceContextGuard(
            "ProcessJobSummaries",
            operationId);

        auto& operationUpdatesProcessingContext = AddOperationUpdatesProcessingContext(
            operationIdToUpdatesProcessingContext,
            heartbeatProcessingContext,
            heartbeatProcessingResult,
            operationId);
        if (!operationUpdatesProcessingContext.OperationController) {
            continue;
        }

        operationUpdatesProcessingContext.JobSummaries.reserve(std::size(jobSummaries));
        operationUpdatesProcessingContext.JobsToAbort.reserve(
            std::size(nodeJobs.Allocations) +
            std::size(operationUpdatesProcessingContext.JobsToAbort));

        bool shouldSkipRunningJobEvents = operationUpdatesProcessingContext.OperationController->ShouldSkipRunningJobEvents();
        bool throttledAnyEvents = false;

        for (auto& jobSummary : jobSummaries) {
            auto jobId = jobSummary->Id;

            if (jobsToSkip.contains(jobId)) {
                continue;
            }

            auto Logger = operationUpdatesProcessingContext.OperationLogger.WithTag(
                "JobId: %v",
                jobId);

            auto newJobStage = JobStageFromJobState(jobSummary->State);

            if (auto allocationIt = nodeJobs.Allocations.find(AllocationIdFromJobId(jobId));
                allocationIt != end(nodeJobs.Allocations))
            {
                auto& allocation = allocationIt->second;
                if (auto currentJobStage = allocation.GetJobStage(jobId)) {
                    bool wasJobEventThrottled = !HandleJobInfo(
                        allocationIt,
                        *currentJobStage,
                        operationUpdatesProcessingContext,
                        response,
                        jobSummary,
                        Logger,
                        heartbeatCounters,
                        shouldSkipRunningJobEvents);
                    throttledAnyEvents = wasJobEventThrottled;

                    continue;
                }
            }

            // Remove or abort unknown job.

            bool shouldAbortJob = newJobStage == EJobStage::Running;

            YT_LOG_INFO(
                "Request node to %v unknown job (JobState: %v)",
                shouldAbortJob ? "abort" : "remove",
                jobSummary->State);

            ++heartbeatCounters.UnknownJobCount;

            if (shouldAbortJob) {
                ++heartbeatCounters.JobAbortRequestCount;
                NProto::ToProto(
                    response->add_jobs_to_abort(),
                    TJobToAbort{
                        .JobId = jobId,
                        .AbortReason = EAbortReason::Unknown,
                    });
                ReportUnknownJobInArchive(jobId, operationId, nodeInfo.NodeAddress);
            } else {
                ++heartbeatCounters.JobReleaseRequestCount;
                ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId});
            }
        }

        heartbeatCounters.ThrottledOperationCount += throttledAnyEvents;
    }
}

void TJobTracker::DoProcessAllocationsInHeartbeat(
    THashMap<TOperationId, TOperationUpdatesProcessingContext>& operationIdToUpdatesProcessingContext,
    TNodeInfo& nodeInfo,
    THeartbeatProcessingContext& heartbeatProcessingContext,
    THeartbeatProcessingResult& heartbeatProcessingResult)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto* response = &heartbeatProcessingContext.RpcContext->Response();
    auto& heartbeatCounters = heartbeatProcessingResult.Counters;
    auto& nodeJobs = nodeInfo.Jobs;

    auto now = TInstant::Now();

    THashMap<TOperationInfo*, std::vector<TNodeJobs::TAllocationIterator>> operationInfoToAllocationsToTryToErase;
    for (auto allocationIt = begin(nodeJobs.Allocations); allocationIt != end(nodeJobs.Allocations); ++allocationIt) {
        auto& [allocationId, allocation] = *allocationIt;

        bool shouldTryErase = false;
        auto addAllocationToTryToErase = [&](TNodeJobs::TAllocationIterator allocationIt, TOperationInfo* operationInfo) {
            if (!std::exchange(shouldTryErase, true)) {
                operationInfoToAllocationsToTryToErase[operationInfo].push_back(allocationIt);
            }
        };

        if (const auto& runningJob = allocation.GetRunningJob()) {
            // Check for jobs disappeared from nodes.
            [&, allocationId = allocationId, &allocation = allocation] {
                auto& jobInfo = *runningJob;

                //! Disapeared confirming jobs will be aborted in AbortUnconfirmedJobs.
                if (!jobInfo.Confirmed) {
                    return;
                }

                if (heartbeatProcessingContext.Request.AllocationIdsRunningOnNode.contains(allocationId)) {
                    return;
                }

                if (!jobInfo.DisappearedFromNodeSince) {
                    allocation.SetDisappearedFromNodeSince(now);
                    return;
                }

                if (now - jobInfo.DisappearedFromNodeSince < Config_->DurationBeforeJobConsideredDisappearedFromNode) {
                    return;
                }

                YT_LOG_INFO(
                    "Job disappeared from node, aborting it (JobId: %v, OperationId: %v, Since: %v)",
                    jobInfo.JobId,
                    allocation.OperationId,
                    jobInfo.DisappearedFromNodeSince);

                ++heartbeatCounters.DisappearedFromNodeJobAbortCount;

                auto& context = AddOperationUpdatesProcessingContext(
                    operationIdToUpdatesProcessingContext,
                    heartbeatProcessingContext,
                    heartbeatProcessingResult,
                    allocation.OperationId);

                context.JobsToAbort.push_back(TJobToAbort{
                    .JobId = jobInfo.JobId,
                    .AbortReason = EAbortReason::DisappearedFromNode,
                });

                allocation.EraseRunningJobOrCrash();
                addAllocationToTryToErase(allocationIt,context.OperationInfo);
            }();
        }

        if (const auto& runningJob = allocation.GetRunningJob();
            runningJob && !runningJob->Confirmed) {
            auto jobId = runningJob->JobId;

            YT_LOG_DEBUG(
                "Request node to confirm job (JobId: %v)",
                jobId);
            ToProto(response->add_jobs_to_confirm(), TJobToConfirm{jobId});
        }

        if (const auto& postponedEvent = allocation.GetPostponedEvent()) {
            if (auto* abortedAllocationInfo = std::get_if<TAbortedAllocationSummary>(&postponedEvent->EventSummary)) {
                auto& context = AddOperationUpdatesProcessingContext(
                    operationIdToUpdatesProcessingContext,
                    heartbeatProcessingContext,
                    heartbeatProcessingResult,
                    allocation.OperationId);

                auto& Logger = context.OperationLogger;

                if (const auto& runningJob = allocation.GetRunningJob(); runningJob && runningJob->Confirmed) {
                    YT_LOG_INFO(
                        "Aborting job since allocation aborted (JobId: %v, AllocationId: %v, AbortReason: %v",
                        runningJob->JobId,
                        AllocationIdFromJobId(runningJob->JobId),
                        abortedAllocationInfo->AbortReason);

                    context.JobsToAbort.push_back(TJobToAbort{
                        .JobId = runningJob->JobId,
                        .AbortReason = abortedAllocationInfo->AbortReason,
                    });

                    ToProto(
                        response->add_jobs_to_abort(),
                        TJobToAbort{
                            .JobId = runningJob->JobId,
                            .AbortReason = abortedAllocationInfo->AbortReason,
                        });

                    allocation.EraseRunningJobOrCrash();
                }

                if (auto& runningJob = allocation.GetRunningJob(); runningJob && !runningJob->Confirmed) {
                    YT_LOG_DEBUG(
                        "Aborted allocation event postponed again since allocation has confirming jobs (AllocationId: %v)",
                        allocationId);
                } else {
                    YT_LOG_INFO(
                        "Processing postponed allocation abort (AllocationId: %v, AbortReason: %v)",
                        allocationId,
                        abortedAllocationInfo->AbortReason);

                    YT_VERIFY(!allocation.GetRunningJob());

                    context.AbortedAllocations.push_back(allocation.ConsumePostponedEventOrCrash<TAbortedAllocationSummary>());
                    addAllocationToTryToErase(allocationIt,context.OperationInfo);
                }
            } else {
                if (const auto& runningJob = allocation.GetRunningJob()) {
                    YT_LOG_DEBUG(
                        "Finished allocation event processing postponed again since allocation has running or confirming jobs "
                        "(AllocationId: %v, JobId: %v, JobConfirmed: %v, OperationId: %v)",
                        allocationId,
                        runningJob->JobId,
                        runningJob->Confirmed,
                        allocation.OperationId);
                } else {
                    auto& context = AddOperationUpdatesProcessingContext(
                        operationIdToUpdatesProcessingContext,
                        heartbeatProcessingContext,
                        heartbeatProcessingResult,
                        allocation.OperationId);
                    auto& Logger = context.OperationLogger;
                    YT_LOG_INFO(
                        "Processing finished allocation event (AllocationId: %v)",
                        allocationId);

                    context.FinishedAllocations.push_back(allocation.ConsumePostponedEventOrCrash<TFinishedAllocationSummary>());
                    addAllocationToTryToErase(allocationIt,context.OperationInfo);
                }
            }
        }

        YT_VERIFY(shouldTryErase || !allocation.ShouldBeRemoved());
    }

    for (const auto& [operationInfo, allocationIterators] : operationInfoToAllocationsToTryToErase) {
        for (auto allocationIt : allocationIterators) {
            auto allocationInfo = EraseAllocationIfNeeded(nodeJobs, allocationIt, operationInfo);

            YT_LOG_FATAL_IF(
                allocationInfo && allocationInfo->GetPostponedEvent(),
                "Erased allocation had postponed allocation event (AllocationId: %v, Event: %v)",
                allocationInfo->AllocationId,
                allocationInfo->GetPostponedEvent());
        }
    }
}

TJobTracker::TOperationUpdatesProcessingContext& TJobTracker::AddOperationUpdatesProcessingContext(
    THashMap<TOperationId, TOperationUpdatesProcessingContext>& contexts,
    THeartbeatProcessingContext& heartbeatProcessingContext,
    THeartbeatProcessingResult& heartbeatProcessingResult,
    TOperationId operationId)
{
    auto* response = &heartbeatProcessingContext.RpcContext->Response();

    auto [it, inserted] = contexts.emplace(
        operationId,
        TOperationUpdatesProcessingContext{.OperationId = operationId,});

    auto& operationUpdatesProcessingContext = it->second;
    if (!inserted) {
        return operationUpdatesProcessingContext;
    }

    operationUpdatesProcessingContext.OperationLogger = heartbeatProcessingContext.Logger.WithTag(
        "OperationId: %v",
        operationId);

    const auto& Logger = operationUpdatesProcessingContext.OperationLogger;

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == std::end(RegisteredOperations_)) {
        YT_LOG_INFO("Operation is not registered at job tracker; skip handling job infos from node");

        ToProto(response->add_unknown_operation_ids(), operationId);

        ++heartbeatProcessingResult.Counters.UnknownOperationCount;

        return operationUpdatesProcessingContext;
    }

    auto& operationInfo = operationIt->second;

    operationUpdatesProcessingContext.OperationInfo = &operationInfo;

    if (!operationInfo.JobsReady) {
        YT_LOG_INFO("Operation jobs are not ready yet; skip handling job infos from node");

        return operationUpdatesProcessingContext;
    }

    auto operationController = operationInfo.OperationController.Lock();
    if (!operationController) {
        YT_LOG_INFO("Operation controller is already reset; skip handling job infos from node");

        return operationUpdatesProcessingContext;
    }

    operationUpdatesProcessingContext.OperationController = std::move(operationController);

    return operationUpdatesProcessingContext;
}

bool TJobTracker::HandleJobInfo(
    TNodeJobs::TAllocationIterator allocationIt,
    EJobStage currentJobStage,
    TOperationUpdatesProcessingContext& operationUpdatesProcessingContext,
    TCtxHeartbeat::TTypedResponse* response,
    std::unique_ptr<TJobSummary>& jobSummary,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters,
    bool shouldSkipRunningJobEvents)
{
    switch (currentJobStage) {
        case EJobStage::Running:
            return HandleRunningJobInfo(
                allocationIt,
                operationUpdatesProcessingContext,
                response,
                jobSummary,
                Logger,
                heartbeatCounters,
                shouldSkipRunningJobEvents);
        case EJobStage::Finished:
            return HandleFinishedJobInfo(
                allocationIt,
                operationUpdatesProcessingContext,
                response,
                jobSummary,
                Logger,
                heartbeatCounters);
        default:
            YT_ABORT();
    }
}

bool TJobTracker::HandleRunningJobInfo(
    TNodeJobs::TAllocationIterator allocationIt,
    TOperationUpdatesProcessingContext& operationUpdatesProcessingContext,
    TCtxHeartbeat::TTypedResponse* response,
    std::unique_ptr<TJobSummary>& jobSummary,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters,
    bool shouldSkipRunningJobEvents)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto& allocation = allocationIt->second;

    auto newJobStage = JobStageFromJobState(jobSummary->State);
    auto jobId = jobSummary->Id;

    YT_VERIFY(allocation.GetRunningJob());

    const auto& runningJob = *allocation.GetRunningJob();

    if (allocation.ConfirmRunningJob()) {
        YT_LOG_DEBUG(
            "Job confirmed (JobStage: %v)",
            newJobStage);

        ++heartbeatCounters.ConfirmedJobCount;
    }

    const auto& requestedActionInfo = runningJob.RequestedActionInfo;

    if (newJobStage == EJobStage::Running) {
        ++heartbeatCounters.RunningJobCount;

        if (const auto& postponedEvent = allocation.GetPostponedEvent();
            postponedEvent && std::holds_alternative<TAbortedAllocationSummary>(postponedEvent->EventSummary))
        {
            // NB(pogorelov): Such job will be aborted in DoProcessAllocationsInHeartbeat.
            return /*wasHandled*/ true;
        }

        Visit(
            requestedActionInfo,
            [] (TNoActionRequested) {},
            [&] (const TInterruptionRequestOptions& requestOptions) {
                ProcessInterruptionRequest(response, requestOptions, jobId, Logger, heartbeatCounters);
            },
            [&] (const TGracefulAbortRequestOptions& requestOptions) {
                ProcessGracefulAbortRequest(response, requestOptions, jobId, Logger, heartbeatCounters);
            });

        if (shouldSkipRunningJobEvents) {
            YT_LOG_INFO("Skipping running job summary because operation controller invoker is overloaded");
            ++heartbeatCounters.ThrottledRunningJobEventCount;

            return /*wasHandled*/ false;
        }

        operationUpdatesProcessingContext.JobSummaries.push_back(std::move(jobSummary));

        return /*wasHandled*/ true;
    }

    YT_VERIFY(newJobStage == EJobStage::Finished);

    ++heartbeatCounters.FinishedJobCount;

    Visit(
        requestedActionInfo,
        [&] (TNoActionRequested) {
            YT_LOG_DEBUG("Received finished job info");
        },
        [&] (const TInterruptionRequestOptions& /*requestOptions*/) {
            YT_LOG_DEBUG("Job is already finished; interruption request ignored");
        },
        [&] (const TGracefulAbortRequestOptions& /*requestOptions*/) {
            YT_LOG_DEBUG("Job is already finished; graceful abort request ignored");
        });

    ToProto(
        response->add_jobs_to_store(),
        TJobToStore{
            .JobId = jobId
        });

    allocation.FinishRunningJob();

    operationUpdatesProcessingContext.JobSummaries.push_back(std::move(jobSummary));

    return /*wasHandled*/ true;
}

bool TJobTracker::HandleFinishedJobInfo(
    TNodeJobs::TAllocationIterator /*allocationIt*/,
    TOperationUpdatesProcessingContext& /*operationUpdatesProcessingContext*/,
    TCtxHeartbeat::TTypedResponse* response,
    std::unique_ptr<TJobSummary>& jobSummary,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto newJobStage = JobStageFromJobState(jobSummary->State);
    auto jobId = jobSummary->Id;

    if (newJobStage < EJobStage::Finished) {
        ++heartbeatCounters.StaleRunningJobCount;

        YT_LOG_DEBUG(
            "Stale job info received (CurrentJobStage: %v, ReceivedJobState: %v)",
            EJobStage::Finished,
            newJobStage);

        return /*wasHandled*/ true;
    }

    ++heartbeatCounters.DuplicatedFinishedJobCount;

    ToProto(
        response->add_jobs_to_store(),
        TJobToStore{
            .JobId = jobId,
        });

    YT_LOG_DEBUG(
        "Finished job info received again, do not process it in operation controller");

    return /*wasHandled*/ true;
}

void TJobTracker::ProcessInterruptionRequest(
    TCtxHeartbeat::TTypedResponse* response,
    const TInterruptionRequestOptions& requestOptions,
    TJobId jobId,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_INFO(
        "Request node to interrupt job (InterruptionReason: %v, InterruptionTimeout: %v)",
        requestOptions.Reason,
        requestOptions.Timeout);

    ++heartbeatCounters.JobInterruptionRequestCount;

    auto* protoJobToInterrupt = response->add_jobs_to_interrupt();
    ToProto(protoJobToInterrupt->mutable_job_id(), jobId);
    protoJobToInterrupt->set_timeout(ToProto<i64>(requestOptions.Timeout));
    protoJobToInterrupt->set_reason(ToProto<i32>(requestOptions.Reason));
}

void TJobTracker::ProcessGracefulAbortRequest(
    TCtxHeartbeat::TTypedResponse* response,
    const TGracefulAbortRequestOptions& requestOptions,
    TJobId jobId,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_INFO("Request node to gracefully abort job");

    ++heartbeatCounters.JobFailureRequestCount;

    if (Config_->EnableGracefulAbort) {
        NProto::ToProto(
            response->add_jobs_to_abort(),
            TJobToAbort{
                .JobId = jobId,
                .AbortReason = requestOptions.Reason,
                .Graceful = true,
            });
    } else {
        auto* protoJobToFail = response->add_jobs_to_fail();
        ToProto(protoJobToFail->mutable_job_id(), jobId);
    }
}

void TJobTracker::DoRegisterOperation(
    TOperationId operationId,
    TWeakPtr<IOperationController> operationController)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_INFO(
        "Registering operation (OperationId: %v)",
        operationId);

    EmplaceOrCrash(
        RegisteredOperations_,
        operationId,
        TOperationInfo{
            .OperationId = operationId,
            .JobsReady = false,
            .OperationController = std::move(operationController)
        });
}

void TJobTracker::DoUnregisterOperation(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_INFO(
        "Unregistering operation (OperationId: %v)",
        operationId);

    auto operationIt = GetIteratorOrCrash(RegisteredOperations_, operationId);
    YT_VERIFY(operationIt != std::end(RegisteredOperations_));

    auto& operation = operationIt->second;

    for (auto allocationId : operation.TrackedAllocationIds) {
        auto nodeId = NodeIdFromAllocationId(allocationId);

        auto& nodeInfo = GetOrCrash(RegisteredNodes_, nodeId);
        auto allocationIt = GetIteratorOrCrash(nodeInfo.Jobs.Allocations, allocationId);

        auto& allocation = allocationIt->second;
        YT_LOG_FATAL_IF(
            !allocation.IsEmpty(),
            "Non empty allocation on allocation unregistration (AllocationId: %v, OperationId: %v)",
            allocationId,
            operationId);

        nodeInfo.Jobs.Allocations.erase(allocationIt);
    }

    RegisteredOperations_.erase(operationIt);
}

void TJobTracker::DoRegisterAllocation(TStartedAllocationInfo allocationInfo, TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromAllocationId(allocationInfo.AllocationId);
    auto& nodeInfo = GetOrRegisterNode(nodeId, allocationInfo.NodeAddress);

    YT_LOG_INFO(
        "Allocation registered (AllocationId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
        allocationInfo.AllocationId,
        operationId,
        nodeId,
        nodeInfo.NodeAddress);

    EmplaceOrCrash(
        nodeInfo.Jobs.Allocations,
        allocationInfo.AllocationId,
        TAllocationInfo(operationId, allocationInfo.AllocationId));

    auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);
    EmplaceOrCrash(operationInfo.TrackedAllocationIds, allocationInfo.AllocationId);
}

void TJobTracker::DoRegisterJob(TStartedJobInfo jobInfo, TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobInfo.JobId);
    auto* node = FindNodeInfo(nodeId);
    if (!node) {
        YT_LOG_INFO(
            "Trying to register job on unknown node; ignored (JobId: %v, NodeId: %v, NodeAddress: %v)",
            jobInfo.JobId,
            nodeId,
            GetNodeAddressForLogging(nodeId));
        return;
    }

    auto* allocation = node->Jobs.FindAllocation(AllocationIdFromJobId(jobInfo.JobId));
    if (!allocation) {
        YT_LOG_INFO(
            "Trying to register job in unknown allocation; ignored (JobId: %v, AllocationId: %v, NodeId: %v, NodeAddress: %v)",
            jobInfo.JobId,
            AllocationIdFromJobId(jobInfo.JobId),
            nodeId,
            node->NodeAddress);
        return;
    }

    if (allocation->IsFinished()) {
        YT_LOG_INFO(
            "Trying to register job in finished allocation; ignored (JobId: %v, AllocationId: %v, NodeId: %v, NodeAddress: %v)",
            jobInfo.JobId,
            AllocationIdFromJobId(jobInfo.JobId),
            nodeId,
            node->NodeAddress);
        return;
    }

    YT_LOG_FATAL_IF(
        allocation->GetRunningJob(),
        "Trying to register job in allocation that already has running job (JobId: %v, PreviousJobId: %v, AllocationId: %v, NodeId: %v, NodeAddress: %v)",
        jobInfo.JobId,
        allocation->GetRunningJob()->JobId,
        AllocationIdFromJobId(jobInfo.JobId),
        nodeId,
        node->NodeAddress);

    YT_LOG_INFO(
        "Job registered (JobId: %v, AllocationId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
        jobInfo.JobId,
        AllocationIdFromJobId(jobInfo.JobId),
        operationId,
        nodeId,
        node->NodeAddress);

    allocation->StartJob(
        jobInfo.JobId,
        /*confirmed*/ true);
}

void TJobTracker::DoRevive(
    TOperationId operationId,
    std::vector<TStartedAllocationInfo> allocations)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    TForbidContextSwitchGuard guard;

    auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

    operationInfo.JobsReady = true;

    {
        int loggingJobSampleMaxSize = Config_->LoggingJobSampleSize;

        auto jobIdSample = CreateJobIdSampleForLogging(allocations, loggingJobSampleMaxSize);

        YT_LOG_INFO(
            "Reviving jobs (OperationId: %v, JobCount: %v, JobIdSample: %v, SampleMaxSize: %v)",
            operationId,
            std::size(allocations),
            jobIdSample,
            loggingJobSampleMaxSize);
    }

    std::vector<TJobId> jobIds;
    jobIds.reserve(size(allocations));
    THashSet<TAllocationId> trackedAllocationIds;
    trackedAllocationIds.reserve(size(allocations));

    for (auto& allocationInfo : allocations) {
        auto nodeId = NodeIdFromAllocationId(allocationInfo.AllocationId);
        auto& nodeJobs = GetOrRegisterNode(nodeId, allocationInfo.NodeAddress).Jobs;

        auto allocationIt = EmplaceOrCrash(
            nodeJobs.Allocations,
            allocationInfo.AllocationId,
            TAllocationInfo(operationId, allocationInfo.AllocationId));

        auto& allocation = allocationIt->second;

        if (allocationInfo.StartedJobInfo) {
            auto jobId = allocationInfo.StartedJobInfo->JobId;
            allocation.StartJob(jobId, /*confirmed*/ false);

            jobIds.push_back(jobId);
        }

        EmplaceOrCrash(trackedAllocationIds, allocationInfo.AllocationId);
    }

    YT_LOG_FATAL_IF(
        !std::empty(operationInfo.TrackedAllocationIds),
        "Reviving jobs of operation that already has allocations (OperationId: %v, RegisteredJobIds: %v, NewJobIds: %v)",
        operationId,
        operationInfo.TrackedAllocationIds,
        jobIds);

    operationInfo.TrackedAllocationIds = std::move(trackedAllocationIds);

    TDelayedExecutor::Submit(
        BIND(
            &TJobTracker::AbortUnconfirmedJobs,
            MakeWeak(this),
            operationId,
            Passed(std::move(jobIds))),
        Config_->JobConfirmationTimeout,
        GetCancelableInvoker());
}

void TJobTracker::DoReleaseJobs(
    TOperationId operationId,
    const std::vector<TJobToRelease>& jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (empty(jobs)) {
        return;
    }

    {
        int loggingJobSampleMaxSize = Config_->LoggingJobSampleSize;

        std::vector<TJobId> jobIdSample = CreateJobIdSampleForLogging(jobs, loggingJobSampleMaxSize);

        YT_LOG_DEBUG(
            "Add jobs to release (OperationId: %v, JobCount: %v, JobIdSample: %v, SampleMaxSize: %v)",
            operationId,
            std::size(jobs),
            jobIdSample,
            loggingJobSampleMaxSize);
    }

    THashMap<TNodeId, THashMap<TAllocationId, std::vector<TJobToRelease>>> grouppedJobsToRelease;
    grouppedJobsToRelease.reserve(std::size(jobs));

    for (const auto& job : jobs) {
        auto nodeId = NodeIdFromJobId(job.JobId);
        auto allocationId = AllocationIdFromJobId(job.JobId);
        grouppedJobsToRelease[nodeId][allocationId].push_back(job);
    }

    auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

    for (const auto& [nodeId, allocationIdToJobsToRelease] : grouppedJobsToRelease) {
        auto* nodeInfo = FindNodeInfo(nodeId);
        if (!nodeInfo) {
            YT_LOG_DEBUG(
                "Skip jobs to release for node that is not connected (OperationId: %v, NodeId: %v, NodeAddress: %v, ReleasedJobCount: %v)",
                operationId,
                nodeId,
                GetNodeAddressForLogging(nodeId),
                std::size(jobs));
            continue;
        }

        auto& nodeJobs = nodeInfo->Jobs;

        for (const auto& [allocationId, jobsToRelease] : allocationIdToJobsToRelease) {
            auto allocationIt = nodeJobs.Allocations.find(allocationId);

            bool jobErased = false;
            for (const auto& jobToRelease : jobsToRelease) {
                EmplaceOrCrash(nodeJobs.JobsToRelease, jobToRelease.JobId, jobToRelease.ReleaseFlags);

                if (allocationIt != end(nodeJobs.Allocations)) {
                    auto& allocation = allocationIt->second;
                    if (allocation.EraseFinishedJob(jobToRelease.JobId)) {
                        jobErased = true;
                    } else {
                        YT_VERIFY(!allocation.HasRunningJob(jobToRelease.JobId));
                    }
                }
            }

            if (allocationIt != end(nodeJobs.Allocations)) {
                auto& allocation = allocationIt->second;
                YT_VERIFY(allocation.OperationId == operationId);

                if (jobErased) {
                    // NB(pogorelov): No postponed allocation event expected here,
                    // since events are postponed only for allocations with running jobs.
                    auto allocationInfo = EraseAllocationIfNeeded(nodeJobs, allocationIt, &operationInfo);

                    YT_VERIFY(!allocationInfo || !allocationInfo->GetPostponedEvent());
                }
            }
        }
    }
}

void TJobTracker::RequestJobAbortion(TJobId jobId, TOperationId operationId, EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobId);

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        YT_LOG_INFO(
            "Node is not registered, skip job abortion request (JobId: %v, NodeId: %v, NodeAddress: %v, Reason: %v, OperationId: %v)",
            jobId,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            reason,
            operationId);
        return;
    }

    auto& nodeJobs = nodeInfo->Jobs;
    const auto& nodeAddress = nodeInfo->NodeAddress;

    TOperationUpdatesProcessingContext context{.OperationId = operationId,};
    context.OperationLogger = Logger().WithTag("OperationId: %v", operationId);
    context.OperationInfo = &GetOrCrash(RegisteredOperations_, operationId);
    context.OperationController = context.OperationInfo->OperationController.Lock();

    auto& Logger = context.OperationLogger;

    if (auto allocationIt = nodeJobs.Allocations.find(AllocationIdFromJobId(jobId));
        allocationIt != end(nodeJobs.Allocations))
    {
        auto& allocation = allocationIt->second;
        if (allocation.HasJob(jobId)) {
            if (allocation.HasRunningJob(jobId)) {
                bool jobConfirmed = allocation.GetRunningJob()->Confirmed;
                allocation.EraseRunningJobOrCrash();
                YT_LOG_INFO(
                    "Running job abort requested (AllocationId: %v, JobId: %v, JobConfirmed: %v, AbortReason: %v, NodeId: %v, NodeAddress: %v)",
                    AllocationIdFromJobId(jobId),
                    jobId,
                    jobConfirmed,
                    reason,
                    nodeId,
                    nodeAddress);

                if (auto allocationInfo = EraseAllocationIfNeeded(nodeJobs, allocationIt);
                    allocationInfo && allocationInfo->GetPostponedEvent())
                {
                    YT_LOG_INFO("Processing postponed allocation event (AllocationId: %v)", allocation.AllocationId);

                    context.AddAllocationEvent(allocationInfo->ConsumePostponedEventOrCrash());
                }
            } else {
                YT_LOG_DEBUG(
                    "Requested to abort already finished job (AllocationId: %v, JobId: %v)",
                    AllocationIdFromJobId(jobId),
                    jobId);
            }
        } else {
            YT_LOG_DEBUG(
                "Requested to abort unknown job (AllocationId: %v, JobId: %v)",
                AllocationIdFromJobId(jobId),
                jobId);
        }
    } else {
        YT_LOG_DEBUG(
            "Requested to abort job from unknown allocation; ignored (AllocationId: %v, JobId: %v)",
            AllocationIdFromJobId(jobId),
            jobId);
    }

    // NB(pogorelov): AbortJobOnNode may be called twice on operation finishing.
    nodeJobs.JobsToAbort.emplace(jobId, reason);
}

std::optional<TJobTracker::TAllocationInfo> TJobTracker::EraseAllocationIfNeeded(
    TNodeJobs& nodeJobs,
    THashMap<TAllocationId, TAllocationInfo>::iterator allocationIt,
    TOperationInfo* operationInfo)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto& [allocationId, allocation] = *allocationIt;

    auto operationId = allocation.OperationId;

    if (allocation.ShouldBeRemoved()) {
        YT_LOG_INFO(
            "Removing allocation (OperationId: %v, AllocationId: %v, PostponedEvent: %v)",
            operationId,
            allocationId,
            allocation.GetPostponedEvent()
                ? ToString(*allocation.GetPostponedEvent())
                : "None");

        auto result = std::move(allocation);

        if (!operationInfo) {
            operationInfo = &GetOrCrash(RegisteredOperations_, operationId);
        }

        EraseOrCrash(operationInfo->TrackedAllocationIds, allocationId);
        nodeJobs.Allocations.erase(allocationIt);

        return result;
    }

    return std::nullopt;
}

template <class TAction>
void TJobTracker::TryRequestJobAction(
    TJobId jobId,
    TOperationId operationId,
    TAction action,
    TStringBuf actionName)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobId);

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        YT_LOG_INFO(
            "Node is not registered, skip action request "
            "(ActionName: %v, JobId: %v, NodeId: %v, NodeAddress: %v, OperationId: %v)",
            actionName,
            jobId,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            operationId);
        return;
    }

    auto& nodeJobs = nodeInfo->Jobs;
    const auto& nodeAddress = nodeInfo->NodeAddress;

    auto* allocation = nodeJobs.FindAllocation(jobId);

    if (allocation && allocation->HasRunningJob(jobId)) {
        YT_LOG_INFO(
            "Requesting action (ActionName: %v, JobId: %v, JobConfirmed: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
            actionName,
            jobId,
            allocation->GetRunningJob()->Confirmed,
            operationId,
            nodeId,
            nodeAddress);

        action(allocation->GetMutableRunningJobRequestedActionInfo());
    } else {
        YT_LOG_DEBUG(
            "Requesting action to job that is not running; ignored "
            "(ActionName: %v, JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
            actionName,
            jobId,
            operationId,
            nodeId,
            nodeAddress);
    }
}

void TJobTracker::RequestJobInterruption(
    TJobId jobId,
    TOperationId operationId,
    EInterruptReason reason,
    TDuration timeout)
{
    TryRequestJobAction(
        jobId,
        operationId,
        [&] (TRequestedActionInfo& requestedActionInfo) {
            DoRequestJobInterruption(requestedActionInfo, jobId, operationId, reason, timeout);
        },
        /*actionName*/ "interruption");
}

void TJobTracker::DoRequestJobInterruption(
    TRequestedActionInfo& requestedActionInfo,
    TJobId jobId,
    TOperationId operationId,
    NScheduler::EInterruptReason reason,
    TDuration timeout)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    Visit(
        requestedActionInfo,
        [&] (TNoActionRequested) {
            requestedActionInfo = TInterruptionRequestOptions{
                .Reason = reason,
                .Timeout = timeout,
            };
        },
        [&] (const TInterruptionRequestOptions& requestOptions) {
            if (timeout < requestOptions.Timeout) {
                YT_LOG_INFO(
                    "Updating interruption request "
                    "(JobId: %v, OperationId: %v, OldTimeout: %v, OldReason: %v, NewTimeout: %v, NewReason: %v)",
                    jobId,
                    operationId,
                    requestOptions.Timeout,
                    requestOptions.Reason,
                    timeout,
                    reason);

                requestedActionInfo = TInterruptionRequestOptions{
                    .Reason = reason,
                    .Timeout = timeout,
                };

                return;
            }

            YT_LOG_DEBUG(
                "Job interruption is already requested with lower timeout; skip new request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);
        },
        [&] (TGracefulAbortRequestOptions& /*requestOptions*/) {
            YT_LOG_FATAL(
                "Unexpected interruption request after graceful abort request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);
        });
}


void TJobTracker::RequestJobGracefulAbort(
    TJobId jobId,
    TOperationId operationId,
    EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    TryRequestJobAction(
        jobId,
        operationId,
        [&] (TRequestedActionInfo& requestedActionInfo) {
            DoRequestJobGracefulAbort(requestedActionInfo, jobId, operationId, reason);
        },
        /*actionName*/ "graceful abort");
}

void TJobTracker::DoRequestJobGracefulAbort(
    TRequestedActionInfo& requestedActionInfo,
    TJobId jobId,
    TOperationId operationId,
    EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    Visit(
        requestedActionInfo,
        [&] (TNoActionRequested) {
            requestedActionInfo = TGracefulAbortRequestOptions{
                .Reason = reason,
            };
        },
        [&] (const TInterruptionRequestOptions& /*requestOptions*/) {
            YT_LOG_INFO(
                "Request job graceful abort despite interruption request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);

            requestedActionInfo = TGracefulAbortRequestOptions{
                .Reason = reason,
            };
        },
        [&] (TGracefulAbortRequestOptions& /*requestOptions*/) { });
}

void TJobTracker::ReportUnknownJobInArchive(TJobId jobId, TOperationId operationId, const TString& nodeAddress)
{
    VERIFY_THREAD_AFFINITY_ANY();

    JobReporter_->HandleJobReport(
        TControllerJobReport()
            .OperationId(operationId)
            .JobId(jobId)
            .Address(nodeAddress)
            .ControllerState(EJobState::Aborted));
}

TJobTracker::TNodeInfo& TJobTracker::GetOrRegisterNode(TNodeId nodeId, const TString& nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt != std::end(RegisteredNodes_)) {
        return nodeIt->second;
    }

    return RegisterNode(nodeId, nodeAddress);
}

TJobTracker::TNodeInfo& TJobTracker::RegisterNode(TNodeId nodeId, TString nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto registrationId = TGuid::Create();

    YT_LOG_INFO(
        "Register node (NodeId: %v, NodeAddress: %v, RegistrationId: %v, DisconnectionTimeout: %v)",
        nodeId,
        nodeAddress,
        registrationId,
        Config_->NodeDisconnectionTimeout);

    if (auto nodeIdIt = NodeAddressToNodeId_.find(nodeAddress); nodeIdIt != std::end(NodeAddressToNodeId_)) {
        auto oldNodeId = nodeIdIt->second;
        YT_LOG_WARNING(
            "Node with the same address is already registered, unregister old node and register new (NodeAddress: %v, NewNodeId: %v, OldNodeId: %v)",
            nodeAddress,
            nodeId,
            nodeIdIt->second);

        UnregisterNode(oldNodeId, nodeAddress);
    }

    NodeRegistrationCount_.Increment();

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeDisconnectionTimeout,
        BIND_NO_PROPAGATE(&TJobTracker::OnNodeHeartbeatLeaseExpired, MakeWeak(this), registrationId, nodeId, nodeAddress)
            .Via(GetCancelableInvoker()));

    EmplaceOrCrash(NodeAddressToNodeId_, nodeAddress, nodeId);

    auto emplaceIt = EmplaceOrCrash(
        RegisteredNodes_,
        nodeId,
        TNodeInfo{
            .Jobs = {},
            .Lease = std::move(lease),
            .RegistrationId = registrationId,
            .NodeAddress = std::move(nodeAddress),
        });
    return emplaceIt->second;
}

TJobTracker::TNodeInfo& TJobTracker::UpdateOrRegisterNode(TNodeId nodeId, const TString& nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt == std::end(RegisteredNodes_)) {
        return RegisterNode(nodeId, nodeAddress);
    } else {
        auto& nodeInfo = nodeIt->second;
        auto& savedAddress = nodeInfo.NodeAddress;
        if (savedAddress != nodeAddress) {
            YT_LOG_WARNING(
                "Node address has changed, unregister old node and register new (OldAddress: %v, NewAddress: %v)",
                savedAddress,
                nodeAddress);

            UnregisterNode(nodeId, savedAddress);
            return RegisterNode(nodeId, nodeAddress);
        }

        YT_LOG_DEBUG(
            "Updating node lease (NodeId: %v, NodeAddress: %v, RegistrationId: %v, DisconnectionTimeout: %v)",
            nodeId,
            nodeAddress,
            nodeInfo.RegistrationId,
            Config_->NodeDisconnectionTimeout);

        TLeaseManager::RenewLease(nodeInfo.Lease, Config_->NodeDisconnectionTimeout);

        return nodeIt->second;
    }
}

void TJobTracker::UnregisterNode(TNodeId nodeId, const TString& nodeAddress, TGuid maybeNodeRegistrationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    TForbidContextSwitchGuard guard;

    auto nodeIt = RegisteredNodes_.find(nodeId);
    if (nodeIt == std::end(RegisteredNodes_)) {
        YT_LOG_DEBUG(
            "Node is already unregistered (NodeId: %v, NodeAddress: %v)",
            nodeId,
            nodeAddress);

        return;
    }

    auto& nodeInfo = nodeIt->second;

    if (maybeNodeRegistrationId) {
        if (maybeNodeRegistrationId != nodeInfo.RegistrationId) {
            YT_LOG_DEBUG(
                "Node unregistration skipped because of registration id mismatch "
                "(NodeId: %v, OldRegistrationId: %v, OldAddress: %v, ActualRegistrationId: %v, ActualAddress: %v)",
                nodeId,
                maybeNodeRegistrationId,
                nodeAddress,
                nodeInfo.RegistrationId,
                nodeInfo.NodeAddress);

            return;
        }
    }

    TLeaseManager::CloseLease(nodeInfo.Lease);

    auto& nodeJobs = nodeInfo.Jobs;
    YT_VERIFY(nodeAddress == nodeInfo.NodeAddress);

    YT_LOG_INFO(
        "Unregistering node (NodeId: %v, NodeAddress: %v, RegistrationId: %v)",
        nodeId,
        nodeAddress,
        nodeInfo.RegistrationId);

    {
        THashMap<TOperationId, TOperationUpdatesProcessingContext> operationIdToContext;

        for (auto allocationIt = begin(nodeJobs.Allocations); !empty(nodeJobs.Allocations); allocationIt = begin(nodeJobs.Allocations)) {
            auto& [allocationId, allocation] = *allocationIt;

            auto [it, inserted] = operationIdToContext.emplace(allocation.OperationId, TOperationUpdatesProcessingContext{.OperationId = allocation.OperationId});
            if (inserted) {
                auto& context = it->second;
                context.OperationInfo = &GetOrCrash(RegisteredOperations_, allocation.OperationId);
                context.OperationLogger = Logger().WithTag("OperationId: %v", allocation.OperationId);
                context.OperationController = context.OperationInfo->OperationController.Lock();
            }

            auto& context = it->second;
            const auto& Logger = context.OperationLogger;

            if (const auto& runningJob = allocation.GetRunningJob()) {
                auto jobId = runningJob->JobId;
                YT_LOG_INFO(
                    "Aborting job since node is unregistering (AllocationId: %v, JobId: %v, JobConfirmed: %v, NodeId: %v, NodeAddress: %v)",
                    AllocationIdFromJobId(jobId),
                    jobId,
                    runningJob->Confirmed,
                    nodeId,
                    nodeAddress);
                context.JobsToAbort.push_back(
                    TJobToAbort{
                        .JobId = jobId,
                        .AbortReason = EAbortReason::NodeOffline,
                    });
            }

            if (allocation.GetPostponedEvent()) {
                context.AddAllocationEvent(allocation.ConsumePostponedEventOrCrash());
            } else {
                context.FinishedAllocations.push_back(
                    TFinishedAllocationSummary{
                        .OperationId = allocation.OperationId,
                        .Id = allocationId,
                        .FinishTime = TInstant::Now(),
                    });
            }

            allocation.FinishAndClearJobs();

            YT_VERIFY(EraseAllocationIfNeeded(nodeJobs, allocationIt));
        }

        ProcessOperationContexts(std::move(operationIdToContext));
    }

    NodeUnregistrationCount_.Increment();

    EraseOrCrash(NodeAddressToNodeId_, nodeAddress);

    RegisteredNodes_.erase(nodeIt);
}

TJobTracker::TNodeInfo* TJobTracker::FindNodeInfo(TNodeId nodeId)
{
    YT_VERIFY(nodeId);

    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt != std::end(RegisteredNodes_)) {
        return &nodeIt->second;
    }

    return nullptr;
}

// NB(pogorelov): Sometimes nodeId or address may change.
// So we use registrationId to prevent new node unregistration on old lease expiration (CloseLease is racy).
void TJobTracker::OnNodeHeartbeatLeaseExpired(TGuid registrationId, TNodeId nodeId, const TString& nodeAddress)
{
    YT_LOG_DEBUG(
        "Node heartbeat lease expired, unregister node (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);

    UnregisterNode(nodeId, nodeAddress, registrationId);
}

void TJobTracker::ProcessAllocationEvents(
    TOperationId operationId,
    std::vector<TFinishedAllocationSummary> finishedAllocations,
    std::vector<TAbortedAllocationSummary> abortedAllocations)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (std::empty(abortedAllocations) && std::empty(finishedAllocations)) {
        return;
    }

    TOperationUpdatesProcessingContext context{.OperationId = operationId};
    context.OperationLogger = NControllerAgent::Logger().WithTag("OperationId: %v", operationId);
    const auto& Logger = context.OperationLogger;

    auto logOperationIsNotRunningEvent = [&] (const auto& operationStatus) {
        YT_LOG_INFO(
            "Received allocation events of operation that is %v; ignore it"
            " (OperationId: %v, IncarnationId: %v, FinishedAllocationCount: %v, AbortedAllocationCount: %v)",
            operationStatus,
            operationId,
            IncarnationId_,
            std::size(finishedAllocations),
            std::size(abortedAllocations));
    };

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == std::end(RegisteredOperations_)) {
        logOperationIsNotRunningEvent("not running");

        return;
    }

    auto& operationInfo = operationIt->second;
    context.OperationInfo = &operationInfo;

    context.OperationController = operationInfo.OperationController.Lock();
    if (!context.OperationController) {
        logOperationIsNotRunningEvent("already finished");

        return;
    }

    YT_LOG_FATAL_UNLESS(
        operationInfo.JobsReady,
        "Unexpected allocation events during revival (IncarnationId: %v, AllocationIds: %v)",
        IncarnationId_,
        [&] {
            std::vector<TAllocationId> allocationIds;
            for (const auto& abortedAllocationSummary : abortedAllocations) {
                allocationIds.push_back(abortedAllocationSummary.Id);
            }

            for (const auto& finishedAllocationSummary : finishedAllocations) {
                allocationIds.push_back(finishedAllocationSummary.Id);
            }

            return allocationIds;
        }());

    // NB(pogorelov): We postpone non-empty allocation event processing until the next node heartbeat to not loose job result and respect job revival.
    ProcessFinishedAllocations(std::move(finishedAllocations), context);
    ProcessAbortedAllocations(std::move(abortedAllocations), context);

    ProcessOperationContext(std::move(context));
}

template <
    class TAllocationEvent,
    CInvocable<void(
        TStringBuf reason,
        TAllocationEvent event,
        TJobTracker::TNodeInfo* nodeInfo,
        std::optional<TJobTracker::TNodeJobs::TAllocationIterator> maybeAllocationIterator)> TProcessEventCallback>
void TJobTracker::ProcessAllocationEvent(
    TAllocationEvent allocationEvent,
    const TProcessEventCallback& skipAllocationEvent,
    const NLogging::TLogger& Logger)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto allocationId = allocationEvent.Id;
    auto nodeId = NodeIdFromAllocationId(allocationId);

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        skipAllocationEvent("Node is not registered", std::move(allocationEvent), nullptr, std::nullopt);
        return;
    }

    auto allocationIt = nodeInfo->Jobs.Allocations.find(allocationEvent.Id);

    if (allocationIt == end(nodeInfo->Jobs.Allocations)) {
        skipAllocationEvent("Allocation is unknown", std::move(allocationEvent), nodeInfo, std::nullopt);
        return;
    }

    auto& allocation = allocationIt->second;

    allocation.Finish(TSchedulerToAgentAllocationEvent{std::move(allocationEvent)});

    if (!allocation.GetRunningJob()) {
        skipAllocationEvent("Event happened on empty allocation", allocation.template ConsumePostponedEventOrCrash<TAllocationEvent>(), nodeInfo, allocationIt);
        return;
    }

    YT_LOG_INFO(
        "Event happened to allocation on online node; postpone event processing until node heartbeat"
        " (AllocationId: %v, NodeId: %v, NodeAddress: %v, Event: %v)",
        allocationEvent.Id,
        nodeId,
        GetNodeAddressForLogging(nodeId),
        allocationEvent);
}

void TJobTracker::ProcessFinishedAllocations(
    std::vector<TFinishedAllocationSummary> finishedAllocations,
    TOperationUpdatesProcessingContext& operationUpdatesProcessingContext)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    const auto& Logger = operationUpdatesProcessingContext.OperationLogger;

    operationUpdatesProcessingContext.FinishedAllocations.reserve(std::size(finishedAllocations));

    for (auto& finishedAllocationSummary : finishedAllocations) {
        auto nodeId = NodeIdFromAllocationId(finishedAllocationSummary.Id);

        auto processAllocationEvent = [&] (
            TStringBuf message,
            TFinishedAllocationSummary event,
            TNodeInfo* nodeInfo,
            std::optional<TNodeJobs::TAllocationIterator> maybeAllocationIt)
        {
            YT_LOG_INFO(
                "%v; send finished allocation event to operation controller"
                " (AllocationId: %v, NodeId: %v, NodeAddress: %v)",
                message,
                finishedAllocationSummary.Id,
                nodeId,
                GetNodeAddressForLogging(nodeId));
            operationUpdatesProcessingContext.FinishedAllocations.push_back(std::move(event));

            if (nodeInfo && maybeAllocationIt) {
                auto allocationInfo = EraseAllocationIfNeeded(
                    nodeInfo->Jobs,
                    *maybeAllocationIt,
                    operationUpdatesProcessingContext.OperationInfo);
                YT_VERIFY(!allocationInfo || !allocationInfo->GetPostponedEvent());
            }
        };

        ProcessAllocationEvent(std::move(finishedAllocationSummary), processAllocationEvent, Logger);
    }
}

void TJobTracker::ProcessAbortedAllocations(
    std::vector<TAbortedAllocationSummary> abortedAllocations,
    TOperationUpdatesProcessingContext& operationUpdatesProcessingContext)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    const auto& Logger = operationUpdatesProcessingContext.OperationLogger;

    operationUpdatesProcessingContext.AbortedAllocations.reserve(std::size(abortedAllocations));

    for (auto& abortedAllocationSummary : abortedAllocations) {
        auto nodeId = NodeIdFromAllocationId(abortedAllocationSummary.Id);

        auto processAllocationEvent = [&] (
            TStringBuf message,
            TAbortedAllocationSummary event,
            TNodeInfo* nodeInfo,
            std::optional<TNodeJobs::TAllocationIterator> maybeAllocationIt)
        {
            YT_LOG_INFO(
                "%v; send aborted allocation event to operation controller"
                " (AllocationId: %v, NodeId: %v, NodeAddress: %v, AbortReason: %v, AbortionError: %v)",
                message,
                abortedAllocationSummary.Id,
                nodeId,
                GetNodeAddressForLogging(nodeId),
                abortedAllocationSummary.AbortReason,
                abortedAllocationSummary.Error);

            operationUpdatesProcessingContext.AbortedAllocations.push_back(std::move(event));

            if (nodeInfo && maybeAllocationIt) {
                auto allocationInfo = EraseAllocationIfNeeded(
                    nodeInfo->Jobs,
                    *maybeAllocationIt,
                    operationUpdatesProcessingContext.OperationInfo);

                YT_VERIFY(!allocationInfo || !allocationInfo->GetPostponedEvent());
            }
        };

        ProcessAllocationEvent(std::move(abortedAllocationSummary), processAllocationEvent, Logger);
    }
}

const TString& TJobTracker::GetNodeAddressForLogging(TNodeId nodeId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = ExecNodes_->find(nodeId); nodeIt == std::end(*ExecNodes_)) {
        static const TString NotReceivedAddress{"<address not received>"};

        return NotReceivedAddress;
    } else {
        return nodeIt->second->Address;
    }
}

void TJobTracker::AbortUnconfirmedJobs(TOperationId operationId, std::vector<TJobId> jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == end(RegisteredOperations_)) {
        YT_LOG_DEBUG(
            "Operation is already finished, skip unconfirmed jobs abortion (OperationId: %v)",
            operationId);

        return;
    }

    THashMap<TOperationId, TOperationUpdatesProcessingContext> operationIdToContext;

    for (auto jobId : jobs) {
        auto nodeId = NodeIdFromJobId(jobId);

        auto* nodeInfo = FindNodeInfo(nodeId);
        if (!nodeInfo) {
            YT_LOG_DEBUG(
                "Node is already disconnected, skip unconfirmed jobs abortion (JobId: %v, NodeId: %v, NodeAddress: %v)",
                jobId,
                nodeId,
                GetNodeAddressForLogging(nodeId));

            continue;
        }

        auto& nodeJobs = nodeInfo->Jobs;
        const auto& nodeAddress = nodeInfo->NodeAddress;

        auto allocationIt = nodeJobs.Allocations.find(AllocationIdFromJobId(jobId));
        if (allocationIt == end(nodeJobs.Allocations)) {
            continue;
        }

        auto& allocation = allocationIt->second;

        if (allocation.HasRunningJob(jobId) && !allocation.GetRunningJob()->Confirmed) {
            auto operationId = allocation.OperationId;

            allocation.EraseRunningJobOrCrash();

            // NB(pogorelov): Do not process postponed allocation event here, it will be processed in the next heartbeat.

            YT_LOG_INFO(
                "Job was not confirmed within timeout, abort it (JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
                jobId,
                operationId,
                nodeId,
            nodeAddress);

            auto [it, inserted] = operationIdToContext.emplace(allocation.OperationId, TOperationUpdatesProcessingContext{.OperationId = allocation.OperationId});
            auto& context = it->second;
            if (inserted) {
                context.OperationInfo = &GetOrCrash(RegisteredOperations_, allocation.OperationId);
                context.OperationLogger = Logger().WithTag("OperationId: %v", allocation.OperationId);
                context.OperationController = context.OperationInfo->OperationController.Lock();
            }

            context.JobsToAbort.push_back({
                .JobId = jobId,
                .AbortReason = EAbortReason::RevivalConfirmationTimeout,
            });

            if (auto allocationInfo = EraseAllocationIfNeeded(nodeJobs, allocationIt, context.OperationInfo);
                allocationInfo && allocationInfo->GetPostponedEvent())
            {
                context.AddAllocationEvent(allocationInfo->ConsumePostponedEventOrCrash());
            }
        }
    }

    ProcessOperationContexts(std::move(operationIdToContext));
}

void TJobTracker::ProcessOperationContext(TOperationUpdatesProcessingContext context)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    const auto& Logger = context.OperationLogger;

    if (!context.OperationInfo || !context.OperationController) {
        YT_LOG_INFO(
            "Operation is not running, skip event processing");

        return;
    }

    AccountEnqueuedControllerEvent(+1);
    // Raw pointer is OK since the job tracker never dies.
    auto discountGuard = Finally(std::bind(&TJobTracker::AccountEnqueuedControllerEvent, this, -1));
    auto* invoker = context.OperationController->GetCancelableInvoker(JobEventsControllerQueue_).Get();

    invoker->Invoke(
        BIND([
                operationUpdatesProcessingContext = std::move(context),
                discountGuard = std::move(discountGuard)
            ] () mutable {
                const auto& Logger = operationUpdatesProcessingContext.OperationLogger;
                for (auto& jobSummary : operationUpdatesProcessingContext.JobSummaries) {
                    YT_VERIFY(jobSummary);

                    auto jobId = jobSummary->Id;
                    auto jobState = jobSummary->State;

                    try {
                        operationUpdatesProcessingContext.OperationController->OnJobInfoReceivedFromNode(std::move(jobSummary));
                    } catch (const std::exception& ex) {
                        YT_LOG_FATAL(
                            ex,
                            "Failed to process job info from node (JobId: %v, JobState: %v)",
                            jobId,
                            jobState);
                    }
                }

                for (const auto& jobToAbort : operationUpdatesProcessingContext.JobsToAbort) {
                    auto jobId = jobToAbort.JobId;
                    auto abortReason = jobToAbort.AbortReason;
                    try {
                        operationUpdatesProcessingContext.OperationController->AbortJobByJobTracker(
                            jobToAbort.JobId,
                            jobToAbort.AbortReason);
                    } catch (const std::exception& ex) {
                        YT_LOG_FATAL(
                            ex,
                            "Failed to abort job in operation controller"
                            " (JobId: %v, AbortReason: %v)",
                            jobId,
                            abortReason);
                    }
                }

                for (auto& abortedAllocation : operationUpdatesProcessingContext.AbortedAllocations) {
                    auto abortReason = abortedAllocation.AbortReason;
                    auto allocationId = abortedAllocation.Id;
                    try {
                        operationUpdatesProcessingContext.OperationController->OnAllocationAborted(
                            std::move(abortedAllocation));
                    } catch (const std::exception& ex) {
                        YT_LOG_FATAL(
                            ex,
                            "Failed to abort allocation in operation controller"
                            " (AllocationId: %v, AbortReason: %v)",
                            allocationId,
                            abortReason);
                    }
                }

                for (auto& finishedAllocation : operationUpdatesProcessingContext.FinishedAllocations) {
                    auto allocationId = finishedAllocation.Id;

                    try {
                        operationUpdatesProcessingContext.OperationController->OnAllocationFinished(
                            std::move(finishedAllocation));
                    } catch (const std::exception& ex) {
                        YT_LOG_FATAL(
                            ex,
                            "Failed to process finished allocation in operation controller"
                            " (AllocationId: %v)",
                            allocationId);
                    }
                }
            }));
}

void TJobTracker::ProcessOperationContexts(THashMap<TOperationId, TOperationUpdatesProcessingContext> contexts)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    for (auto& [operationId, operationUpdatesProcessingContext] : contexts) {
        ProcessOperationContext(std::move(operationUpdatesProcessingContext));
    }
}

void TJobTracker::DoInitialize(IInvokerPtr cancelableInvoker)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    YT_LOG_INFO("Initialize state");

    YT_VERIFY(!CancelableInvoker_.Exchange(cancelableInvoker));
}

void TJobTracker::SetIncarnationId(TIncarnationId incarnationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_VERIFY(!IncarnationId_);

    YT_LOG_INFO("Set new incarnation (IncarnationId: %v)", incarnationId);

    IncarnationId_ = incarnationId;
}

void TJobTracker::DoCleanup()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    YT_LOG_INFO("Cleanup state");

    if (auto invoker = CancelableInvoker_.Exchange(IInvokerPtr{}); !invoker) {
        YT_LOG_INFO("Job tracker is not initialized, skip cleanup");
        return;
    }

    IncarnationId_ = {};

    // No need to cancel leases.
    RegisteredNodes_.clear();

    NodeAddressToNodeId_.clear();

    RegisteredOperations_.clear();
}

////////////////////////////////////////////////////////////////////

TJobTrackerOperationHandler::TJobTrackerOperationHandler(
    TJobTracker* jobTracker,
    IInvokerPtr cancelableInvoker,
    TOperationId operationId)
    : JobTracker_(jobTracker)
    , CancelableInvoker_(std::move(cancelableInvoker))
    , OperationId_(operationId)
    , TraceContext_(CreateTraceContextFromCurrent("JobTrackerOperationHandler"))
    , TraceContextFinishGuard_(TraceContext_)
{
    auto operationIdString = ToString(operationId);
    TraceContext_->SetAllocationTags({{OperationIdTag, operationIdString}});
    TraceContext_->AddProfilingTag(OperationIdTag, operationIdString);
}

void TJobTrackerOperationHandler::RegisterAllocation(TStartedAllocationInfo allocationInfo)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoRegisterAllocation,
        MakeStrong(JobTracker_),
        std::move(allocationInfo),
        OperationId_));
}

void TJobTrackerOperationHandler::RegisterJob(TStartedJobInfo jobInfo)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoRegisterJob,
        MakeStrong(JobTracker_),
        std::move(jobInfo),
        OperationId_));
}

void TJobTrackerOperationHandler::Revive(std::vector<TStartedAllocationInfo> allocations)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoRevive,
        MakeStrong(JobTracker_),
        OperationId_,
        std::move(allocations)));
}

void TJobTrackerOperationHandler::ReleaseJobs(std::vector<TJobToRelease> jobs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoReleaseJobs,
        MakeStrong(JobTracker_),
        OperationId_,
        std::move(jobs)));
}

void TJobTrackerOperationHandler::RequestJobAbortion(
    TJobId jobId,
    EAbortReason reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::RequestJobAbortion,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        reason));
}

void TJobTrackerOperationHandler::RequestJobInterruption(
    TJobId jobId,
    EInterruptReason reason,
    TDuration timeout)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::RequestJobInterruption,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        reason,
        timeout));
}

void TJobTrackerOperationHandler::RequestJobGracefulAbort(
    TJobId jobId,
    EAbortReason reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::RequestJobGracefulAbort,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        reason));
}

void TJobTrackerOperationHandler::ProcessAllocationEvents(
    std::vector<TFinishedAllocationSummary> finishedAllocations,
    std::vector<TAbortedAllocationSummary> abortedAllocations)
{
    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::ProcessAllocationEvents,
        MakeStrong(JobTracker_),
        OperationId_,
        Passed(std::move(finishedAllocations)),
        Passed(std::move(abortedAllocations))));
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
