#include "job_controller.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/object_client/helpers.h>

#include <server/scheduler/job_resources.h>

#include <server/data_node/master_connector.h>

#include <server/exec_agent/slot_manager.h>

#include <server/cell_node/bootstrap.h>

#include <server/misc/memory_usage_tracker.h>

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
    result.set_user_slots(Bootstrap_->GetExecSlotManager()->GetSlotCount());
    result.set_cpu(Config_->ResourceLimits->Cpu);
    result.set_network(Config_->ResourceLimits->Network);
    result.set_replication_slots(Config_->ResourceLimits->ReplicationSlots);
    result.set_removal_slots(Config_->ResourceLimits->RemovalSlots);
    result.set_repair_slots(Config_->ResourceLimits->RepairSlots);
    result.set_seal_slots(Config_->ResourceLimits->SealSlots);

    const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    result.set_memory(tracker->GetUsed(EMemoryCategory::Jobs) + tracker->GetTotalFree());

    return result;
}

TNodeResources TJobController::GetResourceUsage(bool includeWaiting)
{
    auto result = ZeroNodeResources();
    for (const auto& pair : Jobs_) {
        if (includeWaiting || pair.second->GetState() != EJobState::Waiting) {
            auto usage = pair.second->GetResourceUsage();
            result += usage;
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

        auto usedResources = GetResourceUsage(false);
        auto spareResources = GetResourceLimits() - usedResources;
        auto jobResources = job->GetResourceUsage();

        if (!DominatesNonnegative(spareResources, jobResources)) {
            LOG_DEBUG("Not enough resources to start waiting job (JobId: %v, SpareResources: {%v}, JobResources: {%v})",
                job->GetId(),
                FormatResources(spareResources),
                FormatResources(jobResources));
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
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    auto type = EJobType(jobSpec.type());

    auto factory = GetFactory(type);

    auto job = factory.Run(
        jobId,
        resourceLimits,
        std::move(jobSpec));

    LOG_INFO("Job created (JobId: %v, JobType: %v)",
        jobId,
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
                "Failed to increase resource usage (ResourceDelta: {%v})",
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

    for (const auto& pair : Jobs_) {
        const auto& jobId = pair.first;
        auto job = pair.second;
        if (CellTagFromId(jobId) != cellTag)
            continue;
        if (TypeFromId(jobId) != jobObjectType)
            continue;

        auto jobType = EJobType(job->GetSpec().type());
        auto state = job->GetState();
        auto* jobStatus = request->add_jobs();
        ToProto(jobStatus->mutable_job_id(), job->GetId());
        jobStatus->set_job_type(static_cast<int>(jobType));
        jobStatus->set_state(static_cast<int>(state));
        jobStatus->set_phase(static_cast<int>(job->GetPhase()));
        jobStatus->set_progress(job->GetProgress());
        switch (state) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = job->GetResourceUsage();
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed:
                *jobStatus->mutable_result() = job->GetResult();
                break;

            default:
                break;
        }
    }
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
        const auto& resourceLimits = info.resource_limits();
        auto& spec = *info.mutable_spec();
        CreateJob(jobId, resourceLimits, std::move(spec));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NJobAgent
