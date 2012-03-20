#include "stdafx.h"
#include "job_manager.h"
#include "config.h"
#include "slot.h"
#include "job.h"
#include "bootstrap.h"
#include "private.h"
#include "environment_manager.h"

#include <ytlib/misc/fs.h>

//#include <yt/tallyman.h>
//#include <system_error>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TJobManager::TJobManager(
    TJobManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    // make special "scheduler channel" that asks master for scheduler 
    //, SchedulerProxy(~NRpc::CreateBusChannel(Config->SchedulerAddress))
{
    YASSERT(config);
    YASSERT(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetControlInvoker(), ControlThread);

    /*
    using namespace std;
    // TODO(babenko): fix tallyman startup
    try {
        tallyman_start( "/export/home/yeti/projects/wallet" );
    }
    catch ( const system_error& err ) {
        LOG_DEBUG("Failed to start tallyman: %s", err.what());
    }
    catch ( const runtime_error& err ) {
        LOG_DEBUG("Failed to start tallyman: %s", err.what());
    }*/

    // Init job slots.
    for (int slotIndex = 0; slotIndex < Config->SlotCount; ++slotIndex) {
        auto slotName = Sprintf("slot.%d", slotIndex);
        auto slotPath = NFS::CombinePaths(Config->SlotLocation, slotName);
        Slots.push_back(New<TSlot>(slotPath, slotName));
    }
}

/*
TJobManager::~TJobManager()
{
// TODO(babenko): fix tallyman finalization
//    tallyman_stop();
}
*/

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
        ythrow yexception() << Sprintf("No such job %s", ~jobId.ToString());
    }
    return job;
}

std::vector<TJobPtr> TJobManager::GetAllJobs()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TJobPtr> result;
    FOREACH (const auto& pair, Jobs) {
        result.push_back(pair.second);
    }
    return result;
}

int TJobManager::GetTotalSlotCount()
{
    return Config->SlotCount;
}

int TJobManager::GetFreeSlotCount()
{
    int count = 0;
    FOREACH(auto slot, Slots) {
        if (slot->IsEmpty())
            ++count;
    }
    return count;
}

void TJobManager::StartJob(
    const TJobId& jobId,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TSlotPtr emptySlot;
    FOREACH(auto slot, Slots) {
        if (slot->IsEmpty()) {
            emptySlot = slot;
            break;
        }
    }

    if (!emptySlot) {
        LOG_FATAL("All slots are busy (JobId: %s)", ~jobId.ToString());
    }

    LOG_DEBUG("Found slot for new job (JobId: %s, working directory: %s)", 
        ~jobId.ToString(),
        ~emptySlot->GetWorkingDirectory());

    auto job = New<TJob>(
        jobId,
        jobSpec,
        ~Bootstrap->GetChunkCache(),
        ~emptySlot);

    job->Start(~Bootstrap->GetEnvironmentManager());

    Jobs[jobId] = job;

    LOG_DEBUG("Job created (JobId: %s)", ~jobId.ToString());
}

void TJobManager::StopJob( const TJobId& jobId )
{
    YUNREACHABLE();

}

void TJobManager::RemoveJob( const TJobId& jobId )
{
    YUNREACHABLE();

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
