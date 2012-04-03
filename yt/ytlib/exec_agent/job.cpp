#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"

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

using NChunkServer::TChunkId;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

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

    Slot->GetInvoker()->Invoke(BIND(
        &TJob::DoStart,
        MakeWeak(this),
        environmentManager));
}

void TJob::DoStart(TEnvironmentManagerPtr environmentManager)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress == EJobProgress::Created);
    JobProgress = EJobProgress::PreparingProxy;

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
        FOREACH (const auto& fetchRsp, userSpec.files()) {
            auto chunkId = TChunkId::FromProto(fetchRsp.chunk_id());
            awaiter->Await(
                ChunkCache->DownloadChunk(chunkId), 
                BIND(
                    &TJob::OnChunkDownloaded,
                    MakeWeak(this),
                    fetchRsp));
        }
    }

    awaiter->Complete(BIND(
        &TJob::RunJobProxy,
        MakeWeak(this)));
}

void TJob::OnChunkDownloaded(
    const NFileServer::NProto::TRspFetch& fetchRsp,
    NChunkHolder::TChunkCache::TDownloadResult result)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress == EJobProgress::PreparingSandbox);

    auto fileName = fetchRsp.file_name();

    if (!result.IsOK()) {
        Stroka msg = Sprintf(
            "Failed to download file (JobId: %s, FileName: %s, Error: %s)", 
            ~JobId.ToString(),
            ~fileName,
            ~result.GetMessage());

        LOG_WARNING("%s", ~msg);
        SetResult(TError(msg));
        JobProgress = NScheduler::EJobProgress::Failed;
        return;
    }

    CachedChunks.push_back(result.Value());

    try {
        Slot->MakeLink(
            fileName, 
            CachedChunks.back()->GetFileName(), 
            fetchRsp.executable());
    } catch (yexception& ex) {
        Stroka msg = Sprintf(
            "Failed to make symlink (JobId: %s, FileName: %s, Error: %s)", 
            ~JobId.ToString(),
            ~fileName,
            ex.what());

        LOG_WARNING("%s", ~msg);
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

    ProxyController->SubscribeExited(BIND(
        &TJob::OnJobExit,
        MakeWeak(this)).Via(Slot->GetInvoker()));
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
        JobProgress = EJobProgress::Cleanup;
        Slot->Clean();

        JobProgress = EJobProgress::Completed;
        
        if (TError::FromProto(jobResult.error()).IsOK())
            JobState = EJobState::Completed;
        else
            JobState = EJobState::Failed;
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
    if (!JobResult.has_error() || JobResult.error().code() == TError::OK) {
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

void TJob::Abort()
{
    JobState = EJobState::Aborting;
    Slot->GetInvoker()->Invoke(BIND(
        &TJob::DoAbort,
        MakeStrong(this),
        TError("Job aborted by scheduler"),
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

    JobProgress = EJobProgress::Failed;

    SetResult(error);
    JobState = resultState;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

