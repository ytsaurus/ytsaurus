#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"
#include "bootstrap.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/assert.h>

#include <ytlib/ytree/serialize.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/multi_chunk_sequential_reader.h>
#include <ytlib/table_client/config.h>


#include <ytlib/chunk_client/client_block_cache.h>

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
using namespace NYson;
using namespace NTableClient;
using NChunkClient::TChunkId;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    const NScheduler::NProto::TNodeResources& resourceLimits,
    TJobSpec&& jobSpec,
    TBootstrap* bootstrap)
    : JobId(jobId)
    , JobSpec(jobSpec)
    , ResourceLimits(resourceLimits)
    , ResourceUsage(resourceLimits)
    , Logger(ExecAgentLogger)
    , Bootstrap(bootstrap)
    , ChunkCache(bootstrap->GetChunkCache())
    , JobState(EJobState::Waiting)
    , JobPhase(EJobPhase::Created)
    , Progress(0.0)
    , JobFinished(NewPromise<void>())
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    Logger.AddTag(Sprintf("JobId: %s", ~jobId.ToString()));
}

void TJob::Start(TEnvironmentManagerPtr environmentManager, TSlotPtr slot)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(!Slot);

    if (JobState != EJobState::Waiting) {
        return;
    }

    JobState = EJobState::Running;

    Slot = slot;
    slot->Acquire();

    VERIFY_INVOKER_AFFINITY(Slot->GetInvoker(), JobThread);

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

        auto proxyConfig = CloneYsonSerializable(Bootstrap->GetJobProxyConfig());
        proxyConfig->JobIO = ioConfig;
        proxyConfig->UserId = Slot->GetUserId();

        auto proxyConfigPath = NFS::CombinePaths(
            Slot->GetWorkingDirectory(),
            ProxyConfigFileName);

        TFile file(proxyConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TFileOutput output(file);
        TYsonWriter writer(&output, EYsonFormat::Pretty);
        proxyConfig->Save(&writer);
    }

    JobPhase = EJobPhase::PreparingProxy;

    i64 proxyMemoryLimit = GetResourceLimits().memory();
    const NScheduler::NProto::TUserJobSpec* userJobSpec = nullptr;
    {
        if (JobSpec.HasExtension(TMapJobSpecExt::map_job_spec_ext)) {
            const auto& jobSpecExt = JobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
            userJobSpec = &jobSpecExt.mapper_spec();
        }

        if (JobSpec.HasExtension(TReduceJobSpecExt::reduce_job_spec_ext)) {
            const auto& jobSpecExt = JobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            userJobSpec = &jobSpecExt.reducer_spec();
        }

        if (JobSpec.HasExtension(TPartitionJobSpecExt::partition_job_spec_ext)) {
            const auto& jobSpecExt = JobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            if (jobSpecExt.has_mapper_spec()) {
                userJobSpec = &jobSpecExt.mapper_spec();
            }
        }

        if (userJobSpec) {
            proxyMemoryLimit -= userJobSpec->memory_limit();
        }
    }

    Stroka environmentType = "default";
    try {
        YCHECK(proxyMemoryLimit > 0);
        ProxyController = environmentManager->CreateProxyController(
            //XXX(psushin): execution environment type must not be directly
            // selectable by user -- it is more of the global cluster setting
            //jobSpec.operation_spec().environment(),
            environmentType,
            JobId,
            Slot->GetWorkingDirectory(),
            static_cast<i64>(proxyMemoryLimit * Bootstrap->GetConfig()->MemoryLimitMultiplier));
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
    if (userJobSpec) {
        PrepareUserJob(*userJobSpec, awaiter);
    }

    awaiter->Complete(BIND(&TJob::RunJobProxy, MakeWeak(this)));
}

TPromise<void> TJob::PrepareDownloadingTableFile(
    const NYT::NScheduler::NProto::TTableFile& rsp)
{
    std::vector<TChunkId> chunkIds;
    FOREACH (const auto chunk, rsp.table().chunks()) {
        chunkIds.push_back(TChunkId::FromProto(chunk.slice().chunk_id()));
    }

    LOG_INFO(
        "Downloading user table file (FileName: %s, ChunkIds: %s)",
        ~rsp.file_name(),
        ~JoinToString(chunkIds));

    TSharedPtr<TDownloadedChunks> chunks(new TDownloadedChunks());

    auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());
    FOREACH (const auto& chunkId, chunkIds) {
        awaiter->Await(
            ChunkCache->DownloadChunk(chunkId),
            BIND([=](NChunkHolder::TChunkCache::TDownloadResult result) {
                if (!result.IsOK()) {
                    auto wrappedError = TError(
                        "Failed to download chunk %s of table %s",
                        ~chunkId.ToString(),
                        ~rsp.file_name().Quote())
                        << result;
                    DoAbort(wrappedError, EJobState::Failed, false);
                    return;
                }

                chunks->push_back(result);
            })
        );
    }

    auto promise = NewPromise<void>();
    awaiter->Complete(BIND(&TJob::OnTableDownloaded, MakeWeak(this), rsp, chunks, promise));
    return promise;
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

    FOREACH (const auto& rsp, userJobSpec.table_files()) {
        awaiter->Await(PrepareDownloadingTableFile(rsp));
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

void TJob::OnTableDownloaded(
    const NYT::NScheduler::NProto::TTableFile& tableFileRsp,
    TSharedPtr< std::vector<NChunkHolder::TChunkCache::TDownloadResult> > downloadedChunks,
    TPromise<void> promise)
{
    UNUSED(downloadedChunks);

    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup) {
        return;
    }
    YCHECK(JobPhase == EJobPhase::PreparingSandbox);

    // Preparing chunks
    std::vector<NTableClient::NProto::TInputChunk> chunks;
    chunks.insert(
        chunks.end(),
        tableFileRsp.table().chunks().begin(),
        tableFileRsp.table().chunks().end());
    FOREACH(auto& chunk, chunks) {
        chunk.clear_node_addresses();
        chunk.add_node_addresses(Bootstrap->GetPeerAddress());
    }

    // Creating table reader
    auto config = New<TTableReaderConfig>();
    auto blockCache = NChunkClient::CreateClientBlockCache(
        New<NChunkClient::TClientBlockCacheConfig>());
    auto reader = New<TTableChunkSequenceReader>(
        config,
        Bootstrap->GetMasterChannel(),
        blockCache,
        MoveRV(chunks),
        New<TTableChunkReaderProvider>(config));

    auto syncReader = CreateSyncReader(reader);
    syncReader->Open();

    auto tableProducer = BIND(&ProduceYson, syncReader);
    auto format = ConvertTo<NFormats::TFormat>(TYsonString(tableFileRsp.format()));

    auto fileName = tableFileRsp.file_name();
    try {
        Slot->MakeFile(
            fileName,
            tableProducer,
            format);
    } catch (const std::exception& ex) {
        auto wrappedError = TError(
            "Failed to write user table file %s",
            ~fileName.Quote())
            << ex;
        DoAbort(wrappedError, EJobState::Failed, false);
        return;
    }

    LOG_INFO("User table file downloaded successfully (FileName: %s)",
        ~fileName);

    promise.Set();
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
    TGuard<TSpinLock> guard(ResultLock);
    return JobResult.HasValue();
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

    FinalizeJob();
}

const TJobId& TJob::GetId() const
{
    return JobId;
}

const TJobSpec& TJob::GetSpec() const
{
    return JobSpec;
}

void TJob::SetResult(const TJobResult& jobResult)
{
    TGuard<TSpinLock> guard(ResultLock);

    if (!JobResult.HasValue() || JobResult->error().code() == TError::OK) {
        JobResult.Assign(jobResult);
    }
}

const TJobResult& TJob::GetResult() const
{
    TGuard<TSpinLock> guard(ResultLock);
    YCHECK(JobResult.HasValue());
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

const TNodeResources& TJob::GetResourceLimits() const
{
    return ResourceLimits;
}

TNodeResources TJob::GetResourceUsage() const
{
    TGuard<TSpinLock> guard(ResourcesLock);
    return ResourceUsage;
}

void TJob::ReleaseResources(const TNodeResources& newUsage)
{
    TGuard<TSpinLock> guard(ResourcesLock);
    auto oldUsage = ResourceUsage;

    LOG_FATAL_IF(
        JobState == EJobState::Running && !Dominates(oldUsage, newUsage),
        "Job resource usage has increased: old value {%s}, new value {%s}",
        ~FormatResources(ResourceUsage),
        ~FormatResources(newUsage));

    if (Dominates(oldUsage, newUsage) && newUsage != oldUsage) {
        ResourceUsage = newUsage;
        guard.Release();
        ResourcesReleased_.Fire(oldUsage, newUsage);
    }
}

void TJob::Abort(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (JobState == EJobState::Waiting) {
        YCHECK(!Slot);
        JobState = EJobState::Aborted;
        ReleaseResources(ZeroNodeResources());
    } else {
        Slot->GetInvoker()->Invoke(BIND(
            &TJob::DoAbort,
            MakeStrong(this),
            error,
            EJobState::Aborted,
            true));
    }
}

void TJob::DoAbort(const TError& error, EJobState resultState, bool killJobProxy)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup) {
        JobState = resultState;
        return;
    }

    JobState = EJobState::Aborting;

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
            ProxyController->Kill(Slot->GetUserId(), error);
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

    LOG_INFO("Job aborted");

    FinalizeJob();
}

void TJob::FinalizeJob()
{
    Slot->Release();
    ReleaseResources(ZeroNodeResources());
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

