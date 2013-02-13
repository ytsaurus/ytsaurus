#include "stdafx.h"
#include "job_manager.h"
#include "config.h"
#include "slot.h"
#include "job.h"
#include "bootstrap.h"
#include "private.h"
#include "environment.h"
#include "environment_manager.h"

#include <ytlib/misc/fs.h>

#include <server/job_proxy/config.h>

#include <server/chunk_holder/chunk.h>
#include <server/chunk_holder/location.h>
#include <server/chunk_holder/chunk_cache.h>

#include <server/scheduler/job_resources.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NYTree;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TJobManager::TJobManager(
    TJobManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , StartScheduled(false)
    , ResourcesUpdatedFlag(false)
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TJobManager::Initialize()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap->GetControlInvoker(), ControlThread);

    // Init job slots.
    for (int slotIndex = 0; slotIndex < Config->ResourceLimits->Slots; ++slotIndex) {
        auto slotName = ToString(slotIndex);
        auto slotPath = NFS::CombinePaths(Config->SlotLocation, slotName);

        int uid = Bootstrap->IsJobControlEnabled()
            ? Config->StartUserId + slotIndex
            : EmptyUserId;
        Slots.push_back(New<TSlot>(slotPath, slotIndex, uid));
        Slots.back()->Clean();
    }
}

TJobPtr TJobManager::FindJob(const TJobId& jobId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto it = Jobs.find(jobId);
    return it == Jobs.end() ? NULL : it->second;
}

TJobPtr TJobManager::GetJob(const TJobId& jobId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto job = FindJob(jobId);
    if (!job) {
        THROW_ERROR_EXCEPTION("No such job: %s", ~jobId.ToString());
    }
    return job;
}

std::vector<TJobPtr> TJobManager::GetJobs()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TJobPtr> result;
    FOREACH (const auto& pair, Jobs) {
        result.push_back(pair.second);
    }
    return result;
}

TNodeResources TJobManager::GetResourceLimits()
{
    TNodeResources result;
    result.set_slots(Config->ResourceLimits->Slots);
    result.set_cpu(Config->ResourceLimits->Cpu);
    result.set_network(Config->ResourceLimits->Network);

    const auto& tracker = Bootstrap->GetMemoryUsageTracker();
    result.set_memory(tracker.GetFree() + tracker.GetUsed(EMemoryConsumer::Job));

    return result;
}

TNodeResources TJobManager::GetResourceUsage(bool includeWaiting)
{
    auto result = ZeroNodeResources();
    FOREACH (const auto& pair, Jobs) {
        if (!includeWaiting && pair.second->GetState() == EJobState::Waiting) {
            continue;
        }

        auto usage = pair.second->GetResourceUsage();
        result += usage;
    }

    return result;
}

void TJobManager::StartWaitingJobs()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& tracker = Bootstrap->GetMemoryUsageTracker();

    FOREACH (const auto& pair, Jobs) {
        auto job = pair.second;
        if (job->GetState() != EJobState::Waiting)
            continue;

        auto usedResources = GetResourceUsage(false);
        {
            auto memoryToRelease = tracker.GetUsed(EMemoryConsumer::Job) - usedResources.memory();
            YCHECK(memoryToRelease >= 0);
            tracker.Release(EMemoryConsumer::Job, memoryToRelease);
        }

        auto spareResources = GetResourceLimits() - usedResources;
        auto jobResources = job->GetResourceUsage();

        if (Dominates(spareResources, jobResources)) {
            auto error = tracker.TryAcquire(EMemoryConsumer::Job, jobResources.memory());

            if (error.IsOK()) {
                LOG_INFO("Starting job (JobId: %s)", ~job->GetId().ToString());
                auto slot = GetFreeSlot();
                job->SubscribeResourcesReleased(BIND(
                    &TJobManager::OnResourcesReleased,
                    MakeWeak(this)).Via(Bootstrap->GetControlInvoker()));
                slot->Acquire();
                job->Start(Bootstrap->GetEnvironmentManager(), slot);

                // This job is done :)
                continue;
            } else {
                LOG_DEBUG(
                    error,
                    "Not enough memory to start waiting job (JobId: %s)",
                    ~job->GetId().ToString());
            }
        } else {
            LOG_DEBUG(
                "Not enough resources to start waiting job (JobId: %s, SpareResources: %s, JobResources: %s)",
                ~job->GetId().ToString(),
                ~FormatResources(spareResources),
                ~FormatResources(jobResources));
        }
    }

    if (ResourcesUpdatedFlag) {
        ResourcesUpdatedFlag = false;
        ResourcesUpdated_.Fire();
    }

    StartScheduled = false;
}

void TJobManager::CreateJob(
    const TJobId& jobId,
    const NScheduler::NProto::TNodeResources& resourceLimits,
    NScheduler::NProto::TJobSpec& jobSpec)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto slot = GetFreeSlot();

    LOG_INFO("Creating job (JobId: %s)", ~jobId.ToString());

    auto job = New<TJob>(
        jobId,
        resourceLimits,
        std::move(jobSpec),
        Bootstrap);

    YCHECK(Jobs.insert(std::make_pair(jobId, job)).second);
    ScheduleStart();
}

void TJobManager::ScheduleStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!StartScheduled) {
        Bootstrap->GetControlInvoker()->Invoke(BIND(
            &TJobManager::StartWaitingJobs,
            MakeWeak(this)));
        StartScheduled = true;
    }
}

TSlotPtr TJobManager::GetFreeSlot()
{
    FOREACH (auto slot, Slots) {
        if (slot->IsFree()) {
            return slot;
        }
    }

    LOG_FATAL("All slots are busy");
    YUNREACHABLE();
}

void TJobManager::AbortJob(const TJobId& jobId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Job abort requested (JobId: %s)", ~jobId.ToString());

    auto job = GetJob(jobId);
    job->Abort(TError("Abort requested by scheduler"));
}

void TJobManager::RemoveJob(const TJobId& jobId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Job removal requested (JobId: %s)", ~jobId.ToString());
    auto job = FindJob(jobId);
    if (job) {
        YCHECK(job->GetPhase() > EJobPhase::Cleanup);
        YCHECK(job->GetResourceUsage() == ZeroNodeResources());
        YCHECK(Jobs.erase(jobId) == 1);
    } else {
        LOG_WARNING("Requested to remove an unknown job (JobId: %s)", ~jobId.ToString());
    }
}

void TJobManager::OnResourcesReleased()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ResourcesUpdatedFlag = true;
    ScheduleStart();
}

void TJobManager::UpdateResourceUsage(const TJobId& jobId, const TNodeResources& usage)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto job = GetJob(jobId);
    auto oldUsage = job->GetResourceUsage();
    auto delta = usage - oldUsage;

    if (!Dominates(GetResourceLimits(), GetResourceUsage(false) + delta)) {
        job->Abort(TError(
            "Failed to increase resource usage (OldUsage: {%s}, NewUsage: {%s})",
            ~FormatResources(oldUsage),
            ~FormatResources(usage)));
        return;
    }

    if (delta.memory() > 0) {
        auto& tracker = Bootstrap->GetMemoryUsageTracker();
        auto error = tracker.TryAcquire(EMemoryConsumer::Job, delta.memory());
        if (!error.IsOK()) {
            job->Abort(TError(
                "Failed to increase resource usage (OldUsage: {%s}, NewUsage: {%s})",
                ~FormatResources(oldUsage),
                ~FormatResources(usage))
                << error);
            return;
        }
    }

    job->SetResourceUsage(usage);

    if (!Dominates(delta, ZeroNodeResources())) {
        OnResourcesReleased();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
