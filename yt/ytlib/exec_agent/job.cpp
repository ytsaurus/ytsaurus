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

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

// ToDo: kill me please.


////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    const TJobSpec& jobSpec,
    NChunkHolder::TChunkCachePtr chunkCache,
    TSlotPtr slot)
    : JobId(jobId)
    , JobSpec(jobSpec)
    , ChunkCache(chunkCache)
    , Slot(slot)
    , JobState(NScheduler::EJobState::Running)
    , JobProgress(NScheduler::EJobProgress::Created)
{
    VERIFY_INVOKER_AFFINITY(Slot->GetInvoker(), JobThread);
    Slot->Acquire();
}

TJob::~TJob()
{
    Slot->Release();
}

void TJob::Start(TEnvironmentManager* environmentManager)
{
    YASSERT(JobProgress == EJobProgress::Created);

    JobProgress = EJobProgress::PreparingProxy;
    Slot->GetInvoker()->Invoke(FromMethod(
        &TJob::DoStart,
        MakeWeak(this),
        environmentManager));
}

void TJob::DoStart(TEnvironmentManagerPtr environmentManager)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress == EJobProgress::PreparingProxy);

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
        DoAbort(TError(msg), NScheduler::EJobState::Failed);
        return;
    }

    JobProgress = NScheduler::EJobProgress::PreparingSandbox;
    Slot->InitSandbox();
    // ToDo(psushin): create job proxy config.

    auto awaiter = New<TParallelAwaiter>(~Slot->GetInvoker());

    if (JobSpec.HasExtension(NScheduler::NProto::TUserJobSpec::user_job_spec)) {
        auto userSpec = JobSpec.GetExtension(NScheduler::NProto::TUserJobSpec::user_job_spec);
        for (int fileIndex = 0; fileIndex < userSpec.files_size(); ++fileIndex) {
            auto& fetchedChunk = userSpec.files(fileIndex);

            awaiter->Await(ChunkCache->DownloadChunk(
                NChunkServer::TChunkId::FromProto(fetchedChunk.chunk_id())), 
                FromMethod(
                    &TJob::OnChunkDownloaded,
                    MakeWeak(this),
                    fetchedChunk.file_name(),
                    fetchedChunk.executable()));
        }
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

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress == EJobProgress::PreparingSandbox);

    if (!result.IsOK()) {
        Stroka msg = Sprintf(
            "Failed to download file (JobId: %s, FileName: %s, Error: %s)", 
            ~JobId.ToString(),
            ~fileName,
            ~result.GetMessage());

        LOG_WARNING("%s", msg);
        SetResult(TError(msg));
        JobProgress = NScheduler::EJobProgress::Failed;
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
        SetResult(TError(msg));
        JobProgress = NScheduler::EJobProgress::Failed;
        return;
    }

    LOG_DEBUG("Successfully downloaded file (JobId: %s, FileName: %s)", 
        ~JobId.ToString(),
        ~fileName);
}

void TJob::RunJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress == EJobProgress::PreparingSandbox);

    try {
        JobProgress == EJobProgress::StartedProxy;
        ProxyController->Run();
    } catch (const std::exception& ex) {
        DoAbort(TError(ex.what()), NScheduler::EJobState::Failed);
        return;
    }

    ProxyController->SubscribeExited(FromMethod(
        &TJob::OnJobExit,
        MakeWeak(this))->Via(Slot->GetInvoker())->ToCallback());
}

void TJob::OnJobExit(TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress < EJobProgress::Cleanup);

    if (!error.IsOK()) {
        DoAbort(error, EJobState::Failed);
        return;
    }

    auto jobResult = GetResult();
    if (!jobResult.has_error()) {
        DoAbort(TError(
            "Job proxy successfully exited but job result has not been set."),
            EJobState::Failed);
    } else {
        SetResult(TError());
    }
}

const TJobId& TJob::GetId() const
{
    return JobId;
}

const TJobSpec& TJob::GetSpec()
{
    return JobSpec;
}

NScheduler::NProto::TJobResult TJob::GetResult()
{
    TGuard<TSpinLock> guard(SpinLock);
    return JobResult;
}

void TJob::SetResult(const NScheduler::NProto::TJobResult& jobResult)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!JobResult.has_error() || JobResult.error().code() == 0) {
        JobResult = jobResult;
    }
}

void TJob::SetResult(const TError& error)
{
    NScheduler::NProto::TJobResult jobResult;
    *jobResult.mutable_error() = error.ToProto();
    SetResult(jobResult);
}

NScheduler::EJobState TJob::GetState() const
{
    return JobState;
}

NScheduler::EJobProgress TJob::GetProgress() const
{
    return JobProgress;
}

void TJob::Abort(const TError& error)
{
    JobState = EJobState::Aborting;
    Slot->GetInvoker()->Invoke(FromMethod(
        &TJob::DoAbort,
        MakeStrong(this),
        error,
        EJobState::Aborted));
}

void TJob::DoAbort(const TError& error, EJobState resultState)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress < EJobProgress::Cleanup);

    const auto jobProgress = JobProgress;
    JobProgress = EJobProgress::Cleanup;

    LOG_DEBUG("Aborting job (JobId: %s)", 
        ~JobId.ToString());

    if (jobProgress >= EJobProgress::StartedProxy)
        try {
            LOG_DEBUG("Killing job (JobId: %s)", 
                ~JobId.ToString());
            ProxyController->Kill(error);
        } catch (const std::exception& e) {
            //NB: retries should be done inside proxy controller (if makes sense).
            LOG_FATAL("Failed to kill job (JobId: %s)", 
                ~JobId.ToString());
        }

    if (jobProgress >= EJobProgress::PreparingSandbox) {
        LOG_DEBUG("Cleaning slot (JobId: %s)", 
            ~JobId.ToString());
        Slot->Clean();
    }

    SetResult(error);
    JobState = resultState;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

