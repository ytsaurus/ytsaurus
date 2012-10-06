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

#include <ytlib/file_client/file_ypath_proxy.h>

#include <server/chunk_holder/chunk.h>
#include <server/chunk_holder/location.h>
#include <server/chunk_holder/chunk_cache.h>

#include <server/job_proxy/config.h>

#include <server/scheduler/job_resources.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using NChunkClient::TChunkId;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    TJobSpec&& jobSpec,
    TJobProxyConfigPtr proxyConfig,
    NChunkHolder::TChunkCachePtr chunkCache,
    TSlotPtr slot)
    : JobId(jobId)
    , JobSpec(jobSpec)
    // Use initial utilization provided by the scheduler.
    , ResourceUtilization(JobSpec.resource_utilization())
    , Logger(ExecAgentLogger)
    , ChunkCache(chunkCache)
    , Slot(slot)
    , JobState(EJobState::Running)
    , JobPhase(EJobPhase::Created)
    , Progress(0.0)
    , JobResult(Null)
    , JobFinished(NewPromise<void>())
    , ProxyConfig(proxyConfig)
{
    VERIFY_INVOKER_AFFINITY(Slot->GetInvoker(), JobThread);

    Logger.AddTag(Sprintf("JobId: %s", ~jobId.ToString()));
    Slot->Acquire();
}

TJob::~TJob()
{
    Slot->Release();
}

void TJob::Start(TEnvironmentManagerPtr environmentManager)
{
    YCHECK(JobPhase == EJobPhase::Created);

    Slot->GetInvoker()->Invoke(BIND(
        &TJob::DoStart,
        MakeWeak(this),
        environmentManager));
}

void TJob::DoStart(TEnvironmentManagerPtr environmentManager)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup)
        return;

    YCHECK(JobPhase == EJobPhase::Created);

    JobPhase = EJobPhase::PreparingConfig;

    {
        INodePtr ioConfigNode;
        try {
            ioConfigNode = ConvertToNode(TYsonString(JobSpec.io_config()));
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Error deserializing job IO configuration")
                << ex;
            DoAbort(wrappedError, EJobState::Failed);
            return;
        }

        auto ioConfig = New<TJobIOConfig>();
        try {
            ioConfig->Load(ioConfigNode);
        } catch (const std::exception& ex) {
            auto error = TError("Error validating job IO configuration")
                << ex;
            DoAbort(error, EJobState::Failed);
            return;
        }

        auto proxyConfig = CloneYsonSerializable(ProxyConfig);
        proxyConfig->JobIO = ioConfig;

        auto proxyConfigPath = NFS::CombinePaths(
            Slot->GetWorkingDirectory(), 
            ProxyConfigFileName);

        TFileOutput output(proxyConfigPath);
        TYsonWriter writer(&output, EYsonFormat::Pretty);
        proxyConfig->Save(&writer);
    }

    JobPhase = EJobPhase::PreparingProxy;

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
        auto wrappedError = TError(
            "Failed to create proxy controller for environment %s", 
            ~environmentType.Quote())
            << ex;
        DoAbort(wrappedError, EJobState::Failed);
        return;
    }

    JobPhase = EJobPhase::PreparingSandbox;
    Slot->InitSandbox();

    auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());

    if (JobSpec.HasExtension(TMapJobSpecExt::map_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
        PrepareUserJob(jobSpecExt.mapper_spec(), awaiter);
    }

    if (JobSpec.HasExtension(TReduceJobSpecExt::reduce_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        PrepareUserJob(jobSpecExt.reducer_spec(), awaiter);
    }

    if (JobSpec.HasExtension(TPartitionJobSpecExt::partition_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        if (jobSpecExt.has_mapper_spec()) {
            PrepareUserJob(jobSpecExt.mapper_spec(), awaiter);
        }
    }

    awaiter->Complete(BIND(&TJob::RunJobProxy, MakeWeak(this)));
}

void TJob::PrepareUserJob(
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TParallelAwaiterPtr awaiter)
{
    FOREACH (const auto& fetchRsp, userJobSpec.files()) {
        auto chunkId = TChunkId::FromProto(fetchRsp.chunk_id());
        LOG_INFO("Downloading user file (FileName: %s, ChunkId: %s)", 
            ~fetchRsp.file_name(),
            ~chunkId.ToString());
        awaiter->Await(
            ChunkCache->DownloadChunk(chunkId), 
            BIND(&TJob::OnChunkDownloaded, MakeWeak(this), fetchRsp));
    }
}

void TJob::OnChunkDownloaded(
    const NFileClient::NProto::TRspFetchFile& fetchRsp,
    NChunkHolder::TChunkCache::TDownloadResult result)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup)
        return;

    YCHECK(JobPhase == EJobPhase::PreparingSandbox);

    auto fileName = fetchRsp.file_name();

    if (!result.IsOK()) {
        auto wrappedError = TError(
            "Failed to download user file %s", 
            ~fileName.Quote())
            << result;
        DoAbort(wrappedError, EJobState::Failed, false);
        return;
    }

    CachedChunks.push_back(result.Value());

    try {
        Slot->MakeLink(
            fileName, 
            CachedChunks.back()->GetFileName(), 
            fetchRsp.executable());
    } catch (const std::exception& ex) {
        auto wrappedError = TError(
            "Failed to create a symlink for %s", 
            ~fileName.Quote())
            << ex;
        DoAbort(wrappedError, EJobState::Failed, false);
        return;
    }

    LOG_INFO("User file downloaded successfully (FileName: %s)",
        ~fileName);
}

void TJob::RunJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup)
        return;

    YCHECK(JobPhase == EJobPhase::PreparingSandbox);

    try {
        JobPhase = EJobPhase::StartedProxy;
        ProxyController->Run();
    } catch (const std::exception& ex) {
        DoAbort(ex, EJobState::Failed);
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

    if (JobPhase > EJobPhase::Cleanup)
        return;

    YCHECK(JobPhase < EJobPhase::Cleanup);

    if (!error.IsOK()) {
        DoAbort(error, EJobState::Failed);
        return;
    }

    if (!IsResultSet()) {
        DoAbort(
            TError("Job proxy exited successfully but job result has not been set"),
            EJobState::Failed);
        return;
    }

    JobPhase = EJobPhase::Cleanup;
    Slot->Clean();

    JobPhase = EJobPhase::Completed;
        
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

void TJob::SetResult(const TJobResult& jobResult)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (!JobResult.IsInitialized() || JobResult->error().code() == TError::OK) {
        JobResult.Assign(jobResult);
    }
}

const TJobResult& TJob::GetResult() const
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(JobResult.IsInitialized());
    return JobResult.Get();
}

void TJob::SetResult(const TError& error)
{
    TJobResult jobResult;
    ToProto(jobResult.mutable_error(), error);
    SetResult(jobResult);
}

EJobState TJob::GetState() const
{
    return JobState;
}

EJobPhase TJob::GetPhase() const
{
    return JobPhase;
}

TNodeResources TJob::GetResourceUtilization() const
{
    return
        JobState == EJobState::Running || JobState == EJobState::Aborting
        ? ResourceUtilization
        : ZeroResources();
}

void TJob::UpdateResourceUtilization(const TNodeResources& utilization)
{
    if (JobState != EJobState::Running)
        return;

    LOG_FATAL_IF(
        ResourceUtilization.cpu() < utilization.cpu() ||
        ResourceUtilization.memory() < utilization.memory(),
        "Job resource utilization has increased: old value %s, new value %s",
        ~ResourceUtilization.DebugString(),
        ~utilization.DebugString());

    ResourceUtilization = utilization;
    ResourceUtilizationSet_.Fire();
}

void TJob::Abort(const TError& error)
{
    JobState = EJobState::Aborting;
    Slot->GetInvoker()->Invoke(BIND(
        &TJob::DoAbort,
        MakeStrong(this),
        error,
        EJobState::Aborted,
        true));
}

void TJob::DoAbort(const TError& error, EJobState resultState, bool killJobProxy)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup) {
        JobState = resultState;
        return;
    }

    YCHECK(JobPhase < EJobPhase::Cleanup);

    const auto jobPhase = JobPhase;
    JobPhase = EJobPhase::Cleanup;

    if (resultState == EJobState::Failed) {
        LOG_ERROR(error, "Job failed, aborting");
    } else {
        LOG_INFO(error, "Aborting job");
    }

    if (jobPhase >= EJobPhase::StartedProxy && killJobProxy) {
        try {
            LOG_INFO("Killing job");
            ProxyController->Kill(error);
        } catch (const std::exception& ex) {
            // NB: Retries should be done inside proxy controller (if makes sense).
            LOG_FATAL(ex, "Failed to kill job");
        }
    }

    if (jobPhase >= EJobPhase::PreparingSandbox) {
        LOG_INFO("Cleaning slot");
        Slot->Clean();
    }

    SetResult(error);
    JobPhase = EJobPhase::Failed;
    JobState = resultState;
    JobFinished.Set();

    LOG_INFO("Job aborted");
}

void TJob::SubscribeFinished(const TClosure& callback)
{
    JobFinished.Subscribe(callback);
}

void TJob::UnsubscribeFinished(const TClosure& callback)
{
    YUNREACHABLE();
}

void TJob::UpdateProgress(double progress)
{
    Progress = progress;
}

double TJob::GetProgress() const
{
    return Progress;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

