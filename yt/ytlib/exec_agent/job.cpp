#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/assert.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/chunk_holder/chunk.h>
#include <ytlib/chunk_holder/location.h>
#include <ytlib/job_proxy/config.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;

using NChunkServer::TChunkId;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    const TJobSpec& jobSpec,
    TJobProxyConfigPtr proxyConfig,
    NChunkHolder::TChunkCachePtr chunkCache,
    TSlotPtr slot)
    : JobId(jobId)
    , JobSpec(jobSpec)
    , ChunkCache(chunkCache)
    , Slot(slot)
    , JobState(EJobState::Running)
    , JobProgress(EJobProgress::Created)
    , JobResult(Null)
    , JobFinished(NewPromise<void>())
    , ProxyConfig(proxyConfig)
{
    VERIFY_INVOKER_AFFINITY(Slot->GetInvoker(), JobThread);
    Slot->Acquire();
}

TJob::~TJob()
{
    Slot->Release();
}

void TJob::Start(TEnvironmentManagerPtr environmentManager)
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

    JobProgress = EJobProgress::PreparingConfig;

    {
        INodePtr ioConfigNode;
        try {
            ioConfigNode = DeserializeFromYson(JobSpec.io_config());
        } catch (const std::exception& ex) {
            Stroka message = Sprintf(
                "Error deserializing job IO configuration (JobId: %s)\n%s", 
                ~JobId.ToString(), 
                ex.what());
            LOG_ERROR("%s", ~message);
            DoAbort(TError(message), NScheduler::EJobState::Failed);
            return;
        }

        auto ioConfig = New<TJobIOConfig>();
        ioConfig->Load(ioConfigNode);

        auto proxyConfig = CloneConfigurable(ProxyConfig);
        proxyConfig->JobIO = ioConfig;

        auto proxyConfigPath = NFS::CombinePaths(
            Slot->GetWorkingDirectory(), 
            ProxyConfigFileName);
        
        TFileOutput output(proxyConfigPath);
        TYsonWriter writer(&output, EYsonFormat::Pretty);
        proxyConfig->Save(&writer);
    }

    JobProgress = EJobProgress::PreparingProxy;

    Stroka environmentType = "default";
    try {
        ProxyController = environmentManager->CreateProxyController(
            //XXX(psushin): execution environment type must not be directly
            // selectable by user -- it is more of the global cluster setting
            //jobSpec.operation_spec().environment(),
            environmentType,
            JobId,
            Slot->GetWorkingDirectory());
    } catch (const std::exception& ex) {
        Stroka message = Sprintf(
            "Failed to create proxy controller for environment %s (JobId: %s)\n%s", 
            ~environmentType.Quote(),
            ~JobId.ToString(), 
            ex.what());
        LOG_ERROR("%s", ~message);
        DoAbort(TError(message), NScheduler::EJobState::Failed);
        return;
    }

    JobProgress = NScheduler::EJobProgress::PreparingSandbox;
    Slot->InitSandbox();

    auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());

    if (JobSpec.HasExtension(NScheduler::NProto::TMapJobSpecExt::map_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(NScheduler::NProto::TMapJobSpecExt::map_job_spec_ext);
        PrepareUserJob(jobSpecExt.mapper_spec(), awaiter);
    }

    if (JobSpec.HasExtension(NScheduler::NProto::TReduceJobSpecExt::reduce_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(NScheduler::NProto::TReduceJobSpecExt::reduce_job_spec_ext);
        PrepareUserJob(jobSpecExt.reducer_spec(), awaiter);
    }

    awaiter->Complete(BIND(&TJob::RunJobProxy, MakeWeak(this)));
}

void TJob::PrepareUserJob(
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TParallelAwaiterPtr awaiter)
{
    FOREACH (const auto& fetchRsp, userJobSpec.files()) {
        auto chunkId = TChunkId::FromProto(fetchRsp.chunk_id());
        LOG_INFO("Downloading user file %s (JobId: %s, ChunkId: %s)", 
            ~fetchRsp.file_name().Quote(),
            ~JobId.ToString(),
            ~chunkId.ToString());
        awaiter->Await(
            ChunkCache->DownloadChunk(chunkId), 
            BIND(&TJob::OnChunkDownloaded, MakeWeak(this), fetchRsp));
    }
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
        Stroka message = Sprintf(
            "Failed to download user file %s (JobId: %s)\n%s", 
            ~fileName.Quote(),
            ~JobId.ToString(),
            ~result.GetMessage());
        LOG_WARNING("%s", ~message);
        SetResult(TError(message));
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
        Stroka message = Sprintf(
            "Failed to make symlink (JobId: %s, FileName: %s)\n%s", 
            ~JobId.ToString(),
            ~fileName,
            ex.what());
        LOG_ERROR("%s", ~message);
        SetResult(TError(message));
        JobProgress = NScheduler::EJobProgress::Failed;
        return;
    }

    LOG_INFO("User file %s downloaded successfully (JobId: %s)", 
        ~fileName,
        ~JobId.ToString());
}

void TJob::RunJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress == EJobProgress::PreparingSandbox);

    try {
        JobProgress = EJobProgress::StartedProxy;
        ProxyController->Run();
    } catch (const std::exception& ex) {
        DoAbort(TError(ex.what()), NScheduler::EJobState::Failed);
        return;
    }

    ProxyController->SubscribeExited(BIND(
        &TJob::OnJobExit,
        MakeWeak(this)).Via(Slot->GetInvoker()));
}

bool TJob::IsResultSet() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return JobResult.IsInitialized();
}

void TJob::OnJobExit(TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    // NB: we expect that
    //  1. job proxy process finished
    //  2. proxy controller already cleaned up possible child processes.

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress < EJobProgress::Cleanup);

    if (!error.IsOK()) {
        DoAbort(error, EJobState::Failed);
        return;
    }

    if (!IsResultSet()) {
        DoAbort(TError(
            "Job proxy exited successfully but job result has not been set"),
            EJobState::Failed);
        return;
    }

    JobProgress = EJobProgress::Cleanup;
    Slot->Clean();

    JobProgress = EJobProgress::Completed;
        
    if (JobResult->error().code() == TError::OK) {
        JobState = EJobState::Completed;
    } else {
        JobState = EJobState::Failed;
    }

    JobFinished.Set();
}

const TJobId& TJob::GetId() const
{
    return JobId;
}

const TJobSpec& TJob::GetSpec()
{
    return JobSpec;
}

void TJob::SetResult(const NScheduler::NProto::TJobResult& jobResult)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (!JobResult.IsInitialized() || JobResult->error().code() == TError::OK) {
        JobResult.Assign(jobResult);
    }
}

const NScheduler::NProto::TJobResult& TJob::GetResult() const
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(JobResult.IsInitialized());
    return JobResult.Get();
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
        EJobState::Aborted,
        true));
}

void TJob::DoAbort(const TError& error, EJobState resultState, bool killJobProxy)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobProgress > EJobProgress::Cleanup)
        return;

    YASSERT(JobProgress < EJobProgress::Cleanup);

    const auto jobProgress = JobProgress;
    JobProgress = EJobProgress::Cleanup;

    LOG_DEBUG("Aborting job (JobId: %s)", 
        ~JobId.ToString());

    if (jobProgress >= EJobProgress::StartedProxy && killJobProxy) {
        try {
            LOG_DEBUG("Requesting proxy controller to kill job (JobId: %s)", 
                ~JobId.ToString());
            ProxyController->Kill(error);
        } catch (const std::exception& e) {
            //NB: retries should be done inside proxy controller (if makes sense).
            LOG_FATAL("Failed to kill job (JobId: %s)", 
                ~JobId.ToString());
        }
    }

    if (jobProgress >= EJobProgress::PreparingSandbox) {
        LOG_DEBUG("Cleaning slot (JobId: %s)", 
            ~JobId.ToString());
        Slot->Clean();
    }

    SetResult(error);
    JobProgress = EJobProgress::Failed;
    JobState = resultState;
    JobFinished.Set();
}

void TJob::SubscribeFinished(const TClosure& callback)
{
    JobFinished.Subscribe(callback);
}

void TJob::UnsubscribeFinished(const TClosure& callback)
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

