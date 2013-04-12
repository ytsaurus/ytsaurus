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

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/security_client/public.h>

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

using NChunkClient::InvalidNodeId;
using NChunkClient::TChunkId;
using NChunkClient::TChunkReplica;
using NNodeTrackerClient::TNodeDirectory;

////////////////////////////////////////////////////////////////////////////////

const i64 MemoryLimitBoost = 2L * 1024 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    const NScheduler::NProto::TNodeResources& resourceLimits,
    TJobSpec&& jobSpec,
    TBootstrap* bootstrap)
    : JobId(jobId)
    , ResourceLimits(resourceLimits)
    , Bootstrap(bootstrap)
    , ResourceUsage(resourceLimits)
    , JobProxyMemoryLimit(resourceLimits.memory())
    , Logger(ExecAgentLogger)
    , ChunkCache(bootstrap->GetChunkCache())
    , JobState(EJobState::Waiting)
    , JobPhase(EJobPhase::Created)
    , FinalJobState(EJobState::Completed)
    , Progress(0.0)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    Logger.AddTag(Sprintf("JobId: %s", ~ToString(jobId)));

    JobSpec.Swap(&jobSpec);

    UserJobSpec = nullptr;
    if (JobSpec.HasExtension(TMapJobSpecExt::map_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
        UserJobSpec = &jobSpecExt.mapper_spec();
    } else if (JobSpec.HasExtension(TReduceJobSpecExt::reduce_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        UserJobSpec = &jobSpecExt.reducer_spec();
    } else if (JobSpec.HasExtension(TPartitionJobSpecExt::partition_job_spec_ext)) {
        const auto& jobSpecExt = JobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        if (jobSpecExt.has_mapper_spec()) {
            UserJobSpec = &jobSpecExt.mapper_spec();
        }
    }

    if (UserJobSpec) {
        JobProxyMemoryLimit -= UserJobSpec->memory_limit();
        ResourceUsage.set_memory(JobProxyMemoryLimit + UserJobSpec->memory_reserve());
    }
}

void TJob::Start(TEnvironmentManagerPtr environmentManager, TSlotPtr slot)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(!Slot);

    if (JobState != EJobState::Waiting)
        return;
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
            auto* schedulerJobSpecExt = JobSpec.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            ioConfigNode = ConvertToNode(TYsonString(schedulerJobSpecExt->io_config()));
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

    Stroka environmentType = "default";
    try {
        YCHECK(JobProxyMemoryLimit > 0);
        ProxyController = environmentManager->CreateProxyController(
            //XXX(psushin): execution environment type must not be directly
            // selectable by user -- it is more of the global cluster setting
            //jobSpec.operation_spec().environment(),
            environmentType,
            JobId,
            Slot->GetWorkingDirectory(),
            static_cast<i64>(JobProxyMemoryLimit * Bootstrap->GetConfig()->MemoryLimitMultiplier + MemoryLimitBoost));
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

    PrepareUserJob().Subscribe(
        BIND(&TJob::RunJobProxy, MakeStrong(this))
            .Via(Slot->GetInvoker()));
}

TFuture<void> TJob::DownloadRegularFile(
    const NYT::NScheduler::NProto::TRegularFileDescriptor& descriptor)
{
    const auto& fetchRsp = descriptor.file();
    auto chunkId = FromProto<TChunkId>(fetchRsp.chunk_id());
    
    LOG_INFO("Downloading user file (FileName: %s, ChunkId: %s)",
        ~fetchRsp.file_name(),
        ~ToString(chunkId));

    auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());
    awaiter->Await(
        ChunkCache->DownloadChunk(chunkId),
        BIND(&TJob::OnRegularFileChunkDownloaded, MakeWeak(this), descriptor));

    return awaiter->Complete();
}

void TJob::OnRegularFileChunkDownloaded(
    const NYT::NScheduler::NProto::TRegularFileDescriptor& descriptor,
    NChunkHolder::TChunkCache::TDownloadResult result)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup)
        return;
    YCHECK(JobPhase == EJobPhase::PreparingSandbox);

    const auto& fetchRsp = descriptor.file();
    auto fileName = fetchRsp.file_name();

    if (!result.IsOK()) {
        auto wrappedError = TError(
            "Failed to download user file %s",
            ~fileName.Quote())
            << result;
        DoAbort(wrappedError, EJobState::Failed);
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
        DoAbort(wrappedError, EJobState::Failed);
        return;
    }

    LOG_INFO("User file downloaded successfully (FileName: %s)",
        ~fileName);
}

TFuture<void> TJob::DownloadTableFile(
    const NYT::NScheduler::NProto::TTableFileDescriptor& descriptor)
{
    std::vector<TChunkId> chunkIds;
    FOREACH (const auto chunk, descriptor.table().chunks()) {
        chunkIds.push_back(FromProto<TChunkId>(chunk.chunk_id()));
    }

    LOG_INFO("Downloading user table file (FileName: %s, ChunkIds: %s)",
        ~descriptor.file_name(),
        ~JoinToString(chunkIds));

    auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());
    FOREACH (const auto& chunkId, chunkIds) {
        awaiter->Await(
            ChunkCache->DownloadChunk(chunkId),
            BIND([=](NChunkHolder::TChunkCache::TDownloadResult result) {
                if (!result.IsOK()) {
                    auto wrappedError = TError(
                        "Failed to download chunk %s of table %s",
                        ~ToString(chunkId),
                        ~descriptor.file_name().Quote())
                        << result;
                    DoAbort(wrappedError, EJobState::Failed);
                    return;
                }
            })
        );
    }

    return awaiter->Complete(BIND(&TJob::OnTableChunksDownloaded, MakeStrong(this), descriptor));
}

void TJob::OnTableChunksDownloaded(
    const NYT::NScheduler::NProto::TTableFileDescriptor& descriptor)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup)
        return;
    YCHECK(JobPhase == EJobPhase::PreparingSandbox);

    // Create a dummy node directory; prepare chunks.
    // TODO(babenko): change this to handle erasure chunks
    auto nodeDirectory = New<TNodeDirectory>();
    nodeDirectory->AddDescriptor(InvalidNodeId, Bootstrap->GetLocalDescriptor());
    std::vector<NTableClient::NProto::TInputChunk> chunks;
    chunks.insert(
        chunks.end(),
        descriptor.table().chunks().begin(),
        descriptor.table().chunks().end());
    FOREACH (auto& chunk, chunks) {
        chunk.clear_replicas();
        chunk.add_replicas(ToProto<ui32>(TChunkReplica(InvalidNodeId, 0)));
    }

    // Create async table reader.
    auto config = New<TTableReaderConfig>();
    auto blockCache = NChunkClient::CreateClientBlockCache(
        New<NChunkClient::TClientBlockCacheConfig>());
    auto asyncReader = New<TTableChunkSequenceReader>(
        config,
        Bootstrap->GetMasterChannel(),
        blockCache,
        nodeDirectory,
        std::move(chunks),
        New<TTableChunkReaderProvider>(config));

    auto syncReader = CreateSyncReader(asyncReader);
    auto format = ConvertTo<NFormats::TFormat>(TYsonString(descriptor.format()));
    auto fileName = descriptor.file_name();
    try {
        syncReader->Open();
        Slot->MakeFile(
            fileName,
            BIND(&ProduceYson, syncReader),
            format);
    } catch (const std::exception& ex) {
        auto wrappedError = TError(
            "Failed to write user table file %s",
            ~fileName.Quote())
            << ex;
        DoAbort(wrappedError, EJobState::Failed);
        return;
    }

    LOG_INFO("User table file downloaded successfully (FileName: %s)",
        ~fileName);
}

TFuture<void> TJob::PrepareUserJob()
{
    if (!UserJobSpec) {
        return MakeFuture();
    }

    auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());

    FOREACH (const auto& descriptor, UserJobSpec->regular_files()) {
        awaiter->Await(DownloadRegularFile(descriptor));
    }

    FOREACH (const auto& descriptor, UserJobSpec->table_files()) {
        awaiter->Await(DownloadTableFile(descriptor));
    }

    return awaiter->Complete();
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

void TJob::OnJobExit(TError exitError)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase > EJobPhase::Cleanup)
        return;

    YCHECK(JobPhase < EJobPhase::Cleanup);

    if (!exitError.IsOK()) {
        DoAbort(exitError, EJobState::Failed);
        return;
    }

    if (!IsResultSet()) {
        DoAbort(
            TError("Job proxy exited successfully but job result has not been set"),
            EJobState::Failed);
        return;
    }

    // NB: we should explicitly call Kill() to clean up possible child processes.
    ProxyController->Kill(Slot->GetUserId(), TError());

    JobPhase = EJobPhase::Cleanup;
    Slot->Clean();

    JobPhase = EJobPhase::Completed;

    {
        TGuard<TSpinLock> guard(ResultLock);
        JobState = FinalJobState;
    }

    FinalizeJob();
}

bool TJob::IsFatalError(const TError& error)
{
    return
        error.FindMatching(NTableClient::EErrorCode::SortOrderViolation) ||
        error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
        error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
        error.FindMatching(NSecurityClient::EErrorCode::AccountIsOverLimit);
}

bool TJob::IsRetriableSystemError(const TError& error)
{
    return
        error.FindMatching(NChunkClient::EErrorCode::AllTargetNodesFailed) ||
        error.FindMatching(NChunkClient::EErrorCode::MasterCommunicationFailed) ||
        error.FindMatching(NTableClient::EErrorCode::MasterCommunicationFailed);
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

    if (JobState == EJobState::Completed ||
        JobState == EJobState::Aborted ||
        JobState == EJobState::Failed)
    {
        return;
    }

    if (JobResult.HasValue() && JobResult->error().code() != TError::OK) {
        return;
    }

    JobResult.Assign(jobResult);

    auto resultError = FromProto(jobResult.error());
    if (resultError.IsOK()) {
        return;
    } else if (IsFatalError(resultError)) {
        resultError.Attributes().Set("fatal", true);
        ToProto(JobResult->mutable_error(), resultError);
        FinalJobState = EJobState::Failed;
    } else if (IsRetriableSystemError(resultError)) {
        FinalJobState = EJobState::Aborted;
    } else {
        FinalJobState = EJobState::Failed;
    }
}

TJobResult TJob::GetResult() const
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
    TGuard<TSpinLock> guard(ResultLock);
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

void TJob::SetResourceUsage(const TNodeResources& newUsage)
{
    TGuard<TSpinLock> guard(ResourcesLock);
    ResourceUsage = newUsage;
}

void TJob::Abort(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (JobState == EJobState::Waiting) {
        YCHECK(!Slot);
        SetResult(TError("Job aborted by scheduler"));
        JobState = EJobState::Aborted;
        SetResourceUsage(ZeroNodeResources());
        ResourcesReleased_.Fire();
    } else {
        Slot->GetInvoker()->Invoke(BIND(
            &TJob::DoAbort,
            MakeStrong(this),
            error,
            EJobState::Aborted));
    }
}

void TJob::DoAbort(const TError& error, EJobState resultState)
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

    if (jobPhase >= EJobPhase::StartedProxy) {
        // NB: Kill() never throws.
        ProxyController->Kill(Slot->GetUserId(), error);
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
    SetResourceUsage(ZeroNodeResources());
    ResourcesReleased_.Fire();
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

