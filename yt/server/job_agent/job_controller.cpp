#include "job_controller.h"
#include "private.h"
#include "config.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/chunk_cache.h>

#include <yt/server/exec_agent/slot_manager.h>

#include <yt/server/misc/memory_usage_tracker.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/fs.h>

namespace NYT {
namespace NJobAgent {

using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NYTree;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

TJobController::TJobController(
    TJobControllerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(Config_->StatisticsThrottler))
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TJobController::RegisterFactory(EJobType type, TJobFactory factory)
{
    YCHECK(Factories_.insert(std::make_pair(type, factory)).second);
}

TJobFactory TJobController::GetFactory(EJobType type)
{
    auto it = Factories_.find(type);
    YCHECK(it != Factories_.end());
    return it->second;
}

IJobPtr TJobController::FindJob(const TJobId& jobId)
{
    auto it = Jobs_.find(jobId);
    return it == Jobs_.end() ? nullptr : it->second;
}

IJobPtr TJobController::GetJobOrThrow(const TJobId& jobId)
{
    auto job = FindJob(jobId);
    if (!job) {
        THROW_ERROR_EXCEPTION("No such job %v", jobId);
    }
    return job;
}

std::vector<IJobPtr> TJobController::GetJobs()
{
    std::vector<IJobPtr> result;
    for (const auto& pair : Jobs_) {
        result.push_back(pair.second);
    }
    return result;
}

TNodeResources TJobController::GetResourceLimits()
{
    TNodeResources result;

    // If chunk cache is disabled, we disable all sheduler jobs.
    result.set_user_slots(Bootstrap_->GetChunkCache()->IsEnabled() 
        ? Bootstrap_->GetExecSlotManager()->GetSlotCount()
        : 0);

    #define XX(name, Name) \
        result.set_##name(ResourceLimitsOverrides_.has_##name() \
            ? ResourceLimitsOverrides_.name() \
            : Config_->ResourceLimits->Name);
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX

    const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    result.set_memory(std::min(
        tracker->GetLimit(EMemoryCategory::Jobs),
        // NB: The sum of per-category limits can be greater than the total memory limit.
        // Therefore we need bound memory limit by actually available memory.
        tracker->GetUsed(EMemoryCategory::Jobs) + tracker->GetTotalFree()));

    return result;
}

TNodeResources TJobController::GetResourceUsage(bool includeWaiting)
{
    auto result = ZeroNodeResources();
    for (const auto& pair : Jobs_) {
        const auto& job = pair.second;
        if (includeWaiting || job->GetState() != EJobState::Waiting) {
            result += job->GetResourceUsage();
        }
    }
    return result;
}

void TJobController::StartWaitingJobs()
{
    auto* tracker = Bootstrap_->GetMemoryUsageTracker();

    bool resourcesUpdated = false;

    {
        auto usedResources = GetResourceUsage(false);
        auto memoryToRelease = tracker->GetUsed(EMemoryCategory::Jobs) - usedResources.memory();
        if (memoryToRelease > 0) {
            tracker->Release(EMemoryCategory::Jobs, memoryToRelease);
            resourcesUpdated = true;
        }
    }

    for (const auto& pair : Jobs_) {
        auto job = pair.second;
        if (job->GetState() != EJobState::Waiting)
            continue;

        auto jobResources = job->GetResourceUsage();
        auto usedResources = GetResourceUsage(false);
        if (!HasEnoughResources(jobResources, usedResources)) {
            LOG_DEBUG("Not enough resources to start waiting job (JobId: %v, JobResources: %v, UsedResources: %v)",
                job->GetId(),
                FormatResources(jobResources),
                FormatResources(usedResources));
            continue;
        }

        if (jobResources.memory() > 0) {
            auto error = tracker->TryAcquire(EMemoryCategory::Jobs, jobResources.memory());
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Not enough memory to start waiting job (JobId: %v)",
                    job->GetId());
                continue;
            }
        }

        LOG_INFO("Starting job (JobId: %v)", job->GetId());

        job->SubscribeResourcesUpdated(
            BIND(&TJobController::OnResourcesUpdated, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetControlInvoker()));

        job->Start();

        resourcesUpdated = true;
    }

    if (resourcesUpdated) {
        ResourcesUpdated_.Fire();
    }

    StartScheduled_ = false;
}

IJobPtr TJobController::CreateJob(
    const TJobId& jobId,
    const TOperationId& operationId,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    auto type = EJobType(jobSpec.type());

    auto factory = GetFactory(type);

    auto job = factory.Run(
        jobId,
        operationId,
        resourceLimits,
        std::move(jobSpec));

    LOG_INFO("Job created (JobId: %v, OperationId: %v, JobType: %v)",
        jobId,
        operationId,
        type);

    YCHECK(Jobs_.insert(std::make_pair(jobId, job)).second);
    ScheduleStart();

    return job;
}

void TJobController::ScheduleStart()
{
    if (!StartScheduled_) {
        Bootstrap_->GetControlInvoker()->Invoke(BIND(
            &TJobController::StartWaitingJobs,
            MakeWeak(this)));
        StartScheduled_ = true;
    }
}

void TJobController::AbortJob(IJobPtr job)
{
    LOG_INFO("Job abort requested (JobId: %v)",
        job->GetId());

    job->Abort(TError(NExecAgent::EErrorCode::AbortByScheduler, "Job aborted by scheduler"));
}

void TJobController::RemoveJob(IJobPtr job)
{
    LOG_INFO("Job removed (JobId: %v)", job->GetId());

    YCHECK(job->GetPhase() > EJobPhase::Cleanup);
    YCHECK(job->GetResourceUsage() == ZeroNodeResources());
    YCHECK(Jobs_.erase(job->GetId()) == 1);
}

void TJobController::OnResourcesUpdated(TWeakPtr<IJob> job, const TNodeResources& resourceDelta)
{
    if (!CheckResourceUsageDelta(resourceDelta)) {
        auto job_ = job.Lock();
        if (job_) {
            job_->Abort(TError(
                NExecAgent::EErrorCode::ResourceOverdraft,
                "Failed to increase resource usage (ResourceDelta: %v)",
                FormatResources(resourceDelta)));
        }
        return;
    }

    if (!Dominates(resourceDelta, ZeroNodeResources())) {
        // Some resources decreased.
        ScheduleStart();
    }
}

bool TJobController::CheckResourceUsageDelta(const TNodeResources& delta)
{
    // Do this check in the first place in order to avoid weird behavior
    // when decreasing resource usage leads to job abortion because of 
    // other memory consuming subsystems (e.g. ChunkMeta),
    if (Dominates(ZeroNodeResources(), delta)) {
        return true;
    }

    if (!Dominates(GetResourceLimits(), GetResourceUsage(false) + delta)) {
        return false;
    }

    if (delta.memory() > 0) {
        auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        auto error = tracker->TryAcquire(EMemoryCategory::Jobs, delta.memory());
        if (!error.IsOK()) {
            return false;
        }
    }

    return true;
}

bool TJobController::HasEnoughResources(
    const TNodeResources& jobResources,
    const TNodeResources& usedResources)
{
    auto totalResources = GetResourceLimits();
    auto spareResources = MakeNonnegative(totalResources - usedResources);
    if (usedResources.replication_slots() == 0) {
        spareResources.set_replication_data_size(InfiniteNodeResources().replication_data_size());
    }
    if (usedResources.repair_slots() == 0) {
        spareResources.set_repair_data_size(InfiniteNodeResources().repair_data_size());
    }
    return Dominates(spareResources, jobResources);
}

void TJobController::PrepareHeartbeatRequest(
    TCellTag cellTag,
    EObjectType jobObjectType,
    TReqHeartbeat* request)
{
    auto masterConnector = Bootstrap_->GetMasterConnector();
    request->set_node_id(masterConnector->GetNodeId());
    ToProto(request->mutable_node_descriptor(), masterConnector->GetLocalDescriptor());
    *request->mutable_resource_limits() = GetResourceLimits();
    *request->mutable_resource_usage() = GetResourceUsage();

    // A container for all jobs that are candidate to send statistics. This set contains
    // only the runnning jobs since all completed/aborted/failed jobs always send 
    // their statistics.
    std::vector<std::pair<IJobPtr, TJobStatus*>> runningJobs;

    i64 totalStatisticsSize = 0;

    for (const auto& pair : Jobs_) {
        const auto& jobId = pair.first;
        const auto& job = pair.second;
        if (CellTagFromId(jobId) != cellTag)
            continue;
        if (TypeFromId(jobId) != jobObjectType)
            continue;

        auto* jobStatus = request->add_jobs();
        FillJobStatus(jobStatus, job);
        switch (job->GetState()) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = job->GetResourceUsage();
                if (job->ShouldSendStatistics()) {
                    runningJobs.emplace_back(job, jobStatus);
                }
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed:
                *jobStatus->mutable_result() = job->GetResult();
                if (job->ShouldSendStatistics()) {
                    auto statistics = job->GetStatistics();
                    if (statistics) {
                        StatisticsThrottler_->Acquire(statistics->Data().size());
                        totalStatisticsSize += statistics->Data().size();
                        job->ResetStatisticsLastSendTime();
                        jobStatus->set_statistics((*statistics).Data());
                    }
                }
                break;

            default:
                break;
        }
    }
    
    std::sort(
        runningJobs.begin(), 
        runningJobs.end(),
        [] (const auto& lhs, const auto& rhs) {
            return lhs.first->GetStatisticsLastSendTime() < rhs.first->GetStatisticsLastSendTime(); 
        });
    
    for (const auto& pair : runningJobs) {
        const auto& job = pair.first;
        auto* jobStatus = pair.second;
        auto statistics = job->GetStatistics();
        if (statistics && StatisticsThrottler_->TryAcquire(statistics->Data().size())) {
            totalStatisticsSize += statistics->Data().size();
            job->ResetStatisticsLastSendTime();
            jobStatus->set_statistics((*statistics).Data());
        }
    }

    LOG_DEBUG("Total size of statistics to send is %v bytes", totalStatisticsSize);
}

void TJobController::ProcessHeartbeatResponse(TRspHeartbeat* response)
{
    for (const auto& protoJobId : response->jobs_to_remove()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            RemoveJob(job);
        } else {
            LOG_WARNING("Requested to remove a non-existing job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_abort()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            AbortJob(job);
        } else {
            LOG_WARNING("Requested to abort a non-existing job (JobId: %v)",
                jobId);
        }
    }

    for (auto& info : *response->mutable_jobs_to_start()) {
        auto jobId = FromProto<TJobId>(info.job_id());
        auto operationId = FromProto<TJobId>(info.operation_id());
        const auto& resourceLimits = info.resource_limits();
        auto& spec = *info.mutable_spec();
        CreateJob(jobId, operationId, resourceLimits, std::move(spec));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NJobAgent
