#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"
#include "slot_manager.h"
#include "config.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/assert.h>

#include <ytlib/ytree/serialize.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/config.h>

#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/security_client/public.h>

#include <server/chunk_holder/chunk.h>
#include <server/chunk_holder/location.h>
#include <server/chunk_holder/chunk_cache.h>

#include <server/job_proxy/config.h>

#include <server/job_agent/job.h>

#include <server/scheduler/config.h>
#include <server/scheduler/job_resources.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/chunk_holder/config.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NCellNode;
using namespace NChunkHolder;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;

using NNodeTrackerClient::TNodeDirectory;
using NScheduler::NProto::TUserJobSpec;

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public NJobAgent::IJob
{
    DEFINE_SIGNAL(void(), ResourcesReleased);

public:
    TJob(
        const TJobId& jobId,
        const TNodeResources& resourceLimits,
        TJobSpec&& jobSpec,
        TBootstrap* bootstrap)
        : JobId(jobId)
        , ResourceLimits(resourceLimits)
        , Bootstrap(bootstrap)
        , ResourceUsage(resourceLimits)
        , Logger(ExecAgentLogger)
        , JobState(EJobState::Waiting)
        , JobPhase(EJobPhase::Created)
        , FinalJobState(EJobState::Completed)
        , Progress_(0.0)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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

        Logger.AddTag(Sprintf("JobId: %s", ~ToString(jobId)));
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(!Slot);

        if (JobState != EJobState::Waiting)
            return;
        JobState = EJobState::Running;

        auto slotManager = Bootstrap->GetSlotManager();
        Slot = slotManager->AcquireSlot();

        VERIFY_INVOKER_AFFINITY(Slot->GetInvoker(), JobThread);

        Slot->GetInvoker()->Invoke(BIND(
            &TJob::DoStart,
            MakeWeak(this)));
    }

    virtual void Abort(const TError& error) override
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

    virtual const TJobId& GetId() const override
    {
        return JobId;
    }

    virtual const TJobSpec& GetSpec() const override
    {
        return JobSpec;
    }

    virtual EJobState GetState() const override
    {
        TGuard<TSpinLock> guard(ResultLock);
        return JobState;
    }

    virtual EJobPhase GetPhase() const override
    {
        return JobPhase;
    }

    virtual TNodeResources GetResourceUsage() const override
    {
        TGuard<TSpinLock> guard(ResourcesLock);
        return ResourceUsage;
    }

    virtual void SetResourceUsage(const TNodeResources& newUsage) override
    {
        TGuard<TSpinLock> guard(ResourcesLock);
        ResourceUsage = newUsage;
    }

    virtual TJobResult GetResult() const override
    {
        TGuard<TSpinLock> guard(ResultLock);
        return JobResult.Get();
    }

    virtual void SetResult(const TJobResult& jobResult) override
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

        JobResult = jobResult;

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

    virtual double GetProgress() const override
    {
        return Progress_;
    }

    virtual void SetProgress(double value) override
    {
        TGuard<TSpinLock> guard(ResultLock);
        if (JobState == EJobState::Running) {
            Progress_ = value;
        }
    }

private:
    TJobId JobId;
    TJobSpec JobSpec;
    const TUserJobSpec* UserJobSpec;
    TNodeResources ResourceLimits;
    NCellNode::TBootstrap* Bootstrap;

    TSpinLock ResourcesLock;
    TNodeResources ResourceUsage;

    NLog::TTaggedLogger Logger;

    TSlotPtr Slot;

    EJobState JobState;
    EJobPhase JobPhase;

    EJobState FinalJobState;

    double Progress_;

    std::vector<NChunkHolder::TCachedChunkPtr> CachedChunks;

    IProxyControllerPtr ProxyController;

    // Protects #JobResult and #JobState.
    TSpinLock ResultLock;
    TNullable<TJobResult> JobResult;

    NJobProxy::TJobProxyConfigPtr ProxyConfig;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);


    void DoStart()
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
            auto environmentManager = Bootstrap->GetEnvironmentManager();
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

        PrepareUserJob().Subscribe(
            BIND(&TJob::RunJobProxy, MakeStrong(this))
            .Via(Slot->GetInvoker()));
    }

    void DoAbort(const TError& error, EJobState resultState)
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

        if (jobPhase >= EJobPhase::Running) {
            // NB: Kill() never throws.
            ProxyController->Kill(Slot->GetUserId(), error);
        }

        if (jobPhase >= EJobPhase::PreparingSandbox) {
            LOG_INFO("Cleaning slot");
            Slot->Clean();
        }

        SetResult(error);
        JobPhase = EJobPhase::Finished;
        JobState = resultState;

        LOG_INFO("Job aborted");

        FinalizeJob();
    }


    TFuture<void> PrepareUserJob()
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


    void RunJobProxy()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (JobPhase > EJobPhase::Cleanup)
            return;

        YCHECK(JobPhase == EJobPhase::PreparingSandbox);

        try {
            JobPhase = EJobPhase::Running;
            ProxyController->Run();
        } catch (const std::exception& ex) {
            DoAbort(ex, EJobState::Failed);
            return;
        }

        ProxyController->SubscribeExited(BIND(
            &TJob::OnProxyFinished,
            MakeWeak(this)).Via(Slot->GetInvoker()));
    }

    void OnProxyFinished(TError exitError)
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

        JobPhase = EJobPhase::Finished;

        {
            TGuard<TSpinLock> guard(ResultLock);
            JobState = FinalJobState;
        }

        FinalizeJob();
    }


    void FinalizeJob()
    {
        Slot->Release();
        SetResourceUsage(ZeroNodeResources());
        ResourcesReleased_.Fire();
    }


    void SetResult(const TError& error)
    {
        TJobResult jobResult;
        ToProto(jobResult.mutable_error(), error);
        SetResult(jobResult);
    }

    bool IsResultSet() const
    {
        TGuard<TSpinLock> guard(ResultLock);
        return JobResult.HasValue();
    }


    TFuture<void> DownloadRegularFile(const TRegularFileDescriptor& descriptor)
    {
        if (descriptor.file().chunks_size() > 1) {
            auto error = TError(
                "Only single chunk files can be used inside jobs. Failed to prepare file %s (ChunkCount: %d)",
                ~descriptor.file_name(),
                static_cast<int>(descriptor.file().chunks_size()));
            DoAbort(error, EJobState::Failed);
            return MakeFuture();
        }

        if (descriptor.file().chunks_size() == 0) {
            LOG_INFO("Empty user file (FileName: %s)", ~descriptor.file_name());
            Slot->MakeEmptyFile(descriptor.file_name());
            return MakeFuture();
        }

        const auto& chunk = descriptor.file().chunks(0);
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunk.extensions());

        auto compressionCodecId = NCompression::ECodec(miscExt.compression_codec());
        if (compressionCodecId != NCompression::ECodec::None) {
            auto error = TError(
                "Only uncompressed files can be used inside jobs. Failed to prepare file %s (CompressionCodec: %d)",
                ~descriptor.file_name(),
                ~FormatEnum(compressionCodecId));
            DoAbort(error, EJobState::Failed);
            return MakeFuture();
        }

        auto chunkId = FromProto<TChunkId>(chunk.chunk_id());
        if (IsErasureChunkId(chunkId)) {
            auto error = TError(
                "Only non-erasure files can be used inside jobs. Failed to prepare file %s",
                ~descriptor.file_name());
            DoAbort(error, EJobState::Failed);
            return MakeFuture();
        }

        LOG_INFO("Downloading user file (FileName: %s, ChunkId: %s)",
            ~descriptor.file_name(),
            ~ToString(chunkId));

        auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());
        auto chunkCache = Bootstrap->GetChunkCache();
        awaiter->Await(
            chunkCache->DownloadChunk(chunkId),
            BIND(&TJob::OnRegularFileChunkDownloaded, MakeWeak(this), descriptor));

        return awaiter->Complete();
    }

    void OnRegularFileChunkDownloaded(
        const TRegularFileDescriptor& descriptor,
        TChunkCache::TDownloadResult result)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (JobPhase > EJobPhase::Cleanup)
            return;
        YCHECK(JobPhase == EJobPhase::PreparingSandbox);

        auto fileName = descriptor.file_name();

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
                descriptor.executable());
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


    TFuture<void> DownloadTableFile(const TTableFileDescriptor& descriptor)
    {
        std::vector<TChunkId> chunkIds;
        FOREACH (const auto chunk, descriptor.table().chunks()) {
            chunkIds.push_back(FromProto<TChunkId>(chunk.chunk_id()));
        }

        LOG_INFO("Downloading user table file (FileName: %s, ChunkIds: %s)",
            ~descriptor.file_name(),
            ~JoinToString(chunkIds));

        auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());
        auto chunkCache = Bootstrap->GetChunkCache();
        FOREACH (const auto& chunkId, chunkIds) {
            awaiter->Await(
                chunkCache->DownloadChunk(chunkId),
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

    void OnTableChunksDownloaded(const TTableFileDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (JobPhase > EJobPhase::Cleanup)
            return;
        YCHECK(JobPhase == EJobPhase::PreparingSandbox);

        // Create a dummy node directory; prepare chunks.
        // TODO(babenko): change this to handle erasure chunks
        auto nodeDirectory = New<TNodeDirectory>();
        nodeDirectory->AddDescriptor(InvalidNodeId, Bootstrap->GetLocalDescriptor());
        std::vector<NChunkClient::NProto::TInputChunk> chunks;
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

    auto readerProvider = New<TTableChunkReaderProvider>(
        chunks,
        config);
    auto asyncReader = New<TTableChunkSequenceReader>(
        config,
        Bootstrap->GetMasterChannel(),
        blockCache,
        nodeDirectory,
        std::move(chunks),
        readerProvider);

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


    static bool IsFatalError(const TError& error)
    {
        return
            error.FindMatching(NTableClient::EErrorCode::SortOrderViolation) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AccountIsOverLimit);
    }

    static bool IsRetriableSystemError(const TError& error)
    {
        return
            error.FindMatching(NChunkClient::EErrorCode::AllTargetNodesFailed) ||
            error.FindMatching(NChunkClient::EErrorCode::MasterCommunicationFailed) ||
            error.FindMatching(NTableClient::EErrorCode::MasterCommunicationFailed);
    }

};

NJobAgent::IJobPtr CreateUserJob(
    const TJobId& jobId,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec,
    TBootstrap* bootstrap)
{
    return New<TJob>(
        jobId,
        resourceLimits,
        std::move(jobSpec),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

