#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/chunk_holder/chunk.h>
#include <ytlib/chunk_holder/location.h>

namespace NYT {
namespace NExecAgent {

using namespace NChunkHolder;
using namespace NRpc;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    const TJobSpec& jobSpec,
    TChunkCachePtr chunkCache,
    TSlotPtr slot)
    : JobId(jobId)
    , JobSpec(jobSpec)
    , ChunkCache(chunkCache)
    , Slot(slot)
    , JobState(NScheduler::EJobState::Created)
{
    VERIFY_INVOKER_AFFINITY(Slot->GetInvoker(), JobThread);
    Slot->Acquire();
}

void TJob::Start(TEnvironmentManager* environmentManager)
{
    JobState = NScheduler::EJobState::PreparingProxy;
    Slot->GetInvoker()->Invoke(FromMethod(
        &TJob::DoStart,
        MakeWeak(this),
        environmentManager));
}

void TJob::DoStart(TEnvironmentManagerPtr environmentManager)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Stroka environmentType = "default";
    try {
        ProxyController = environmentManager->CreateProxyController(
            //XXX: type of execution environment must not be directly
            // selectable by user -- it is more of the global cluster setting
            //jobSpec.operation_spec().environment(),
            environmentType,
            JobId,
            Slot->GetWorkingDirectory());

    } catch (const std::exception& ex) {
        Stroka msg = Sprintf(
            "Failed to create proxy controller for environment \"%s\" (JobId: %s, Path: %s)", 
            ~environmentType,
            ~JobId.ToString(), 
            ex.what());

        LOG_DEBUG("%s", ~msg);
        *JobResult.mutable_error() = TError(msg).ToProto();
        JobState = NScheduler::EJobState::Failed;
        return;
    }

    JobState = NScheduler::EJobState::PreparingSandbox;

    try {
        Slot->InitSandbox();
    } catch (const std::exception& ex) {
        Stroka msg = Sprintf(
            "Failed to create a sandbox. (JobId: %s, Path: %s)", 
            ~JobId.ToString(), 
            ex.what());

        LOG_WARNING("%s", msg);
        *JobResult.mutable_error() = TError(msg).ToProto();
        JobState = NScheduler::EJobState::Failed;
        return;
    }

    auto awaiter = New<TParallelAwaiter>(~Slot->GetInvoker());
    for (int fileIndex = 0; fileIndex < JobSpec.files_size(); ++fileIndex) {
        auto& fetchedChunk = JobSpec.files(fileIndex);

        awaiter->Await(ChunkCache->DownloadChunk(
            TChunkId::FromProto(fetchedChunk.chunk_id())), 
            FromMethod(
                &TJob::OnChunkDownloaded,
                MakeWeak(this),
                fetchedChunk.file_name(),
                fetchedChunk.executable()));
    }

    awaiter->Complete(FromMethod(
        &TJob::RunJobProxy,
        MakeWeak(this)));
}

void TJob::OnChunkDownloaded(
    NChunkHolder::TChunkCache::TDownloadResult result,
    const Stroka& fileName,
    bool executable)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (!result.IsOK()) {
        Stroka msg = Sprintf(
            "Failed to download file (JobId: %s, FileName: %s, Error: %s)", 
            ~JobId.ToString(),
            ~fileName,
            ~result.GetMessage());

        LOG_WARNING("%s", msg);
        *JobResult.mutable_error() = TError(msg).ToProto();
        JobState = NScheduler::EJobState::Failed;
        return;
    }

    CachedChunks.push_back(result.Value());

    try {
        Slot->MakeLink(
            fileName, 
            CachedChunks.back()->GetFileName(), 
            executable);
    } catch (yexception& ex) {
        Stroka msg = Sprintf(
            "Failed to make symlink (JobId: %s, FileName: %s, Error: %s)", 
            ~JobId.ToString(),
            ~fileName,
            ex.what());

        LOG_WARNING("%s", msg);
        *JobResult.mutable_error() = TError(msg).ToProto();
        JobState = NScheduler::EJobState::Failed;
        return;
    }

    LOG_DEBUG("Successfully downloaded file (JobId: %s, FileName: %s)", 
        ~JobId.ToString(),
        ~fileName);
}

void TJob::RunJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobState == NScheduler::EJobState::Failed) {
        Slot->Clean();
        return;
    }

    try {
        ProxyController->Run();
        /*ProxyController->SubscribeOnExit(~FromMethod(
            &TJob::OnJobExit,
            TPtr(this))->Via(Slot->GetInvoker()));*/

    } catch (const std::exception& ex) {
        //DoCancel(TError(ex.what()));
        JobState = NScheduler::EJobState::Failed;
        return;
    }

    JobState = NScheduler::EJobState::StartedProxy;
}

const TJobId& TJob::GetId() const
{
    return JobId;
}

const TJobSpec& TJob::GetSpec()
{
    return JobSpec;
}

TJobResult TJob::GetResult()
{
    return JobResult;
}

void TJob::SetResult(const NScheduler::NProto::TJobResult& jobResult)
{
    YUNIMPLEMENTED();
}

NScheduler::EJobState TJob::GetState()
{
    return NScheduler::EJobState::Running;
}

//
//void TJob::SubscribeOnStarted(IAction::TPtr callback)
//{
//    OnStarted->Subscribe(callback->ToParamAction<TVoid>());
//}
//
//void TJob::SubscribeOnFinished(
//    IParamAction<NScheduler::NProto::TJobResult>::TPtr callback)
//{
//    OnFinished->Subscribe(callback);
//}
//

//const NScheduler::NProto::TJobSpec& TJob::GetSpec()
//{
//    // ToDo(psushin): make special supervisor call "Ready to start"
//    // to identify moment of start completion more precisely.
//    Slot->GetInvoker()->Invoke(FromMethod(
//        &TJob::StartComplete,
//        MakeStrong(this));
//    return JobSpec;
//}
//
//void TJob::SetResult(const NScheduler::NProto::TJobResult& jobResult)
//{
//    JobResult->Set(jobResult);
//}
//
//void TJob::Cancel(const TError& error)
//{
//    Slot->GetInvoker()->Invoke(FromMethod(
//        &TJob::DoCancel,
//        MakeStrong(this),
//        error));
//}
//
//void TJob::DoCancel(const TError& error)
//{
//    VERIFY_THREAD_AFFINITY(JobThread);
//
//    YASSERT(Error.IsOK());
//    YASSERT(!OnFinished->IsSet());
//
//    Error = error;
//
//    try {
//        LOG_TRACE("Trying to kill job (JobId: %s, error: %s)", 
//            ~JobId.ToString(),
//            ~error.GetMessage());
//
//        ProxyController->Kill(error);
//    } catch (yexception& e) {
//        Error = TError(Sprintf(
//            "Failed to kill job (JobId: %s, error: %s)", 
//            ~JobId.ToString(),
//            e.what()));
//
//        LOG_DEBUG("%s", ~Error.GetMessage());
//    }
//
//    if (!OnFinished->IsSet()) {
//        OnFinished->Set(MakeJobResult(Error));
//    }
//
//    Slot->Clean();
//}
//
//void TJob::StartComplete()
//{
//    VERIFY_THREAD_AFFINITY(JobThread);
//
//    if (!Error.IsOK())
//        return;
//
//    OnStarted->Set(TVoid());
//}


//
//void TJob::OnJobExit(TError error)
//{
//    VERIFY_THREAD_AFFINITY(JobThread);
//
//    if (!Error.IsOK())
//        return;
//
//    if (!error.IsOK()) {
//        DoCancel(error);
//        return;
//    }
//
//    if (!JobResult->IsSet()) {
//        DoCancel(TError(
//            "Job proxy successfully exited but job result has not been set."));
//    } else {
//        OnFinished->Set(JobResult->Get());
//    }
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

