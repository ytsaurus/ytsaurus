#include "stdafx.h"
#include "job_manager.h"
#include "config.h"
#include "slot.h"
#include "job.h"
#include "bootstrap.h"
#include "private.h"
#include "environment_manager.h"

#include <ytlib/misc/fs.h>

#include <server/job_proxy/config.h>

#include <server/chunk_holder/chunk_cache.h>

#include <server/scheduler/job_resources.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TJobManager::TJobManager(
    TJobManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YASSERT(config);
    YASSERT(bootstrap);
}

void TJobManager::Initialize()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap->GetControlInvoker(), ControlThread);

    // Init job slots.
    for (int slotIndex = 0; slotIndex < Config->ResourceLimits->Slots; ++slotIndex) {
        auto slotName = ToString(slotIndex);
        auto slotPath = NFS::CombinePaths(Config->SlotLocation, slotName);
        Slots.push_back(New<TSlot>(slotPath, slotIndex));
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
        // TODO(babenko): error code
        THROW_ERROR_EXCEPTION("No such job %s", ~jobId.ToString());
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

    result.set_memory(Bootstrap->GetMemoryUsageTracker().GetTotalMemory());
    return result;
}

TNodeResources TJobManager::GetResourceUtilization()
{
    auto totalUtilization = ZeroResources();
    FOREACH (const auto& pair, Jobs) {
        auto jobUtilization = pair.second->GetResourceUtilization();
        AddResources(&totalUtilization, jobUtilization);
    }

    totalUtilization.set_memory(Bootstrap->GetMemoryUsageTracker().GetUsedMemory());
    return totalUtilization;
}

TJobPtr TJobManager::StartJob(
    const TJobId& jobId,
    NScheduler::NProto::TJobSpec& jobSpec)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto slot = GetFreeSlot();

    LOG_DEBUG("Job is starting (JobId: %s)", ~jobId.ToString());

    auto job = New<TJob>(
        jobId,
        MoveRV(jobSpec),
        Bootstrap->GetJobProxyConfig(),
        Bootstrap->GetChunkCache(),
        slot);

    auto error = Bootstrap->GetMemoryUsageTracker().TryAcquire(
        NCellNode::EMemoryConsumer::Job, 
        jobSpec.resource_utilization().memory());

    if (error.IsOK()) {
        job->SubscribeFinished(BIND(&TJobManager::OnJobFinished, MakeWeak(this), job));
        job->Start(Bootstrap->GetEnvironmentManager());
    } else {
        job->Abort(error);
    }

    YCHECK(Jobs.insert(MakePair(jobId, job)).second);

    return job;
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

    LOG_DEBUG("Job abort requested (JobId: %s)", ~jobId.ToString());

    auto job = GetJob(jobId);
    job->Abort();
}

void TJobManager::RemoveJob(const TJobId& jobId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Job removal requested (JobId: %s)", ~jobId.ToString());
    auto job = FindJob(jobId);
    if (job) {
        YASSERT(job->GetPhase() > EJobPhase::Cleanup);
        YCHECK(Jobs.erase(jobId) == 1);
    } else {
        LOG_WARNING("Requested to remove an unknown job (JobId: %s)", ~jobId.ToString());
    }
}

void TJobManager::OnJobFinished(TJobPtr job)
{
    Bootstrap->GetMemoryUsageTracker().Release(
        NCellNode::EMemoryConsumer::Job, 
        job->GetSpec().resource_utilization().memory());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
