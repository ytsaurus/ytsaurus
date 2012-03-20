#include "stdafx.h"
#include "job_manager.h"
#include "config.h"
#include "slot.h"
#include "job.h"
#include "bootstrap.h"
#include "private.h"

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

    //using namespace std;
    // TODO(babenko): fix tallyman startup
    //try {
    //    tallyman_start( "/export/home/yeti/projects/wallet" );
    //}
    //catch ( const system_error& err ) {
    //    LOG_DEBUG("Failed to start tallyman: %s", err.what());
    //}
    //catch ( const runtime_error& err ) {
    //    LOG_DEBUG("Failed to start tallyman: %s", err.what());
    //}

    // Init job slots.
    for (int slotIndex = 0; slotIndex < Config->SlotCount; ++slotIndex) {
        auto slotName = Sprintf("slot.%d", slotIndex);
        auto slotPath = NFS::CombinePaths(Config->SlotLocation, slotName);
        Slots.push_back(New<TSlot>(slotPath, slotName));
    }
}

TJobManager::~TJobManager()
{
// TODO(babenko): fix tallyman finalization
//    tallyman_stop();
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
    // TODO(babenko): implement
    return GetTotalSlotCount();
}

void TJobManager::StartJob( const TJobId& jobId, const NScheduler::NProto::TJobSpec& jobSpec )
{
    YUNREACHABLE();
}

void TJobManager::StopJob( const TJobId& jobId )
{
    YUNREACHABLE();

}

void TJobManager::RemoveJob( const TJobId& jobId )
{
    YUNREACHABLE();

}

//
//
//void TJobManager::StartJob(
//    const TJobId& jobId,
//    const NScheduler::NProto::TJobSpec& jobSpec)
//{
//    VERIFY_THREAD_AFFINITY(ControlThread);
//
//    TSlot::TPtr emptySlot;
//    FOREACH(auto slot, Slots) {
//        if (slot->IsEmpty()) {
//            emptySlot = slot;
//            break;
//        }
//    }
//
//    if (!emptySlot) {
//        LOG_WARNING("All slots are busy (JobId: %s)",
//            ~jobId.ToString());
//
//        NScheduler::NProto::TJobResult result;
//        result.set_is_ok(false);
//        result.set_error_message("All slots are busy.");
//
//        OnJobFinished(result, jobId);
//        return;
//    }
//
//    LOG_DEBUG("Found slot for new job (JobId: %s, working directory: %s)", 
//        ~jobId.ToString(),
//        ~emptySlot->GetWorkingDirectory());
//
//    IProxyController* proxyController(NULL);
//    try {
//        proxyController = EnvironmentManager.CreateProxyController(
//            //XXX: type of execution environment must not be directly
//            // selectable by user -- it is more of the global cluster
//            // setting
//            //jobSpec.operation_spec().environment(),
//            "default",
//            jobId,
//            emptySlot->GetWorkingDirectory());
//
//    } catch (yexception& ex) {
//        LOG_DEBUG("Failed to create proxy controller: (JobId: %s, Error: %s)", 
//            ~jobId.ToString(),
//            ex.what());
//
//        NScheduler::NProto::TJobResult result;
//        result.set_is_ok(false);
//        result.set_error_message(Sprintf(
//            "Failed to create proxy controller: %s",
//            ex.what()));
//
//        OnJobFinished(result, jobId);
//        return;
//    }
//
//    // ToDo: handle errors.
//    emptySlot->Acquire();
//
//    auto job = New<TJob>(
//        ~Config->Job,
//        jobId,
//        jobSpec,
//        ~ChunkCache,
//        ~MasterChannel,
//        ~emptySlot,
//        proxyController);
//
//    Jobs[jobId] = job;
//
//    job->SubscribeOnStarted(FromMethod(
//        &TJobManager::OnJobStarted,
//        TPtr(this),
//        jobId)->Via(JobManagerThread->GetInvoker()));
//
//    job->SubscribeOnFinished(FromMethod(
//        &TJobManager::OnJobFinished,
//        TPtr(this),
//        jobId)->Via(JobManagerThread->GetInvoker()));
//
//    LOG_DEBUG("Job created (JobId: %s)", ~jobId.ToString());
//}
//
//void TJobManager::OnJobStarted(const TJobId& jobId)
//{
//    VERIFY_THREAD_AFFINITY(ControlThread);
//
//    auto req = SchedulerProxy.JobStarted();
//    *(req->mutable_job_id()) = jobId.ToProto();
//
//    // ToDo: check results, log errors, do retries.
//    req->Invoke();
//}
//
//void TJobManager::CancelOperation(const TOperationId& operationId)
//{
//    VERIFY_THREAD_AFFINITY(ControlThread);
//    FOREACH(auto& job, Jobs) {
//        if (job.First().OperationId == operationId) {
//            job.Second()->Cancel(TError("Job cancelled by sheduler."));
//        }
//    }
//
//}
//
//void TJobManager::OnJobFinished(
//    NScheduler::NProto::TJobResult jobResult,
//    const TJobId& jobId)
//{
//    VERIFY_THREAD_AFFINITY(ControlThread);
//
//    LOG_DEBUG("Job finished (JobId: %s)", ~jobId.ToString());
//
//    Jobs.erase(jobId);
//
//    auto req = SchedulerProxy.JobFinished();
//    *(req->mutable_job_id()) = jobId.ToProto();
//    *(req->mutable_job_result()) = jobResult;
//
//    // ToDo: check results, log errors, do retries.
//    req->Invoke();
//}
//
//TJob::TPtr TJobManager::GetJob(const TJobId& jobId)
//{
//}
//
//void TJobManager::SetJobResult(
//    const TJobId& jobId, 
//    const NScheduler::NProto::TJobResult& jobResult)
//{
//    auto job = GetJob(jobId);
//
//    job->SetResult(jobResult);
//}
//
//const NScheduler::NProto::TJobSpec& 
//TJobManager::GetJobSpec(const TJobId& jobId)
//{
//    VERIFY_THREAD_AFFINITY(ControlThread);
//
//    auto job = GetJob(jobId);
//    return job->GetSpec();
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
