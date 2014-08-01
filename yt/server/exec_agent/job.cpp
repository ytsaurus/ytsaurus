#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"
#include "slot_manager.h"
#include "config.h"

#include <core/misc/fs.h>
#include <core/misc/proc.h>
#include <core/misc/assert.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/serialize.h>

#include <core/logging/log.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/file_client/file_ypath_proxy.h>
#include <ytlib/file_client/file_chunk_reader.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/config.h>

#include <ytlib/file_client/config.h>
#include <ytlib/file_client/file_chunk_reader.h>

#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/security_client/public.h>

#include <server/data_node/chunk.h>
#include <server/data_node/location.h>
#include <server/data_node/chunk_cache.h>
#include <server/data_node/block_store.h>

#include <server/job_proxy/config.h>
#include <server/job_proxy/public.h>

#include <server/job_agent/job.h>

#include <server/scheduler/config.h>
#include <server/scheduler/job_resources.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/data_node/config.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NFileClient;
using namespace NCellNode;
using namespace NDataNode;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NConcurrency;
using namespace NApi;

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
        , JobStatistics(ZeroJobStatistics())
        , StartTime(Null)
        , NodeDirectory(New<TNodeDirectory>())
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        JobSpec.Swap(&jobSpec);

        NodeDirectory->AddDescriptor(InvalidNodeId, Bootstrap->GetLocalDescriptor());

        Logger.AddTag("JobId: %v", jobId);
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(!Slot);

        if (JobState != EJobState::Waiting)
            return;
        StartTime = TInstant::Now();
        JobState = EJobState::Running;

        auto slotManager = Bootstrap->GetSlotManager();
        Slot = slotManager->AcquireSlot();

        auto invoker = Slot->GetInvoker();

        VERIFY_INVOKER_AFFINITY(invoker, JobThread);

        invoker->Invoke(BIND(&TJob::DoRun, MakeWeak(this)));
    }

    virtual void Abort(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (JobState == EJobState::Waiting) {
            YCHECK(!Slot);
            SetResult(error);
            JobPhase = EJobPhase::Finished;
            FinalizeJob();
        } else {
            Slot->GetInvoker()->Invoke(BIND(&TJob::DoAbort, MakeStrong(this), error));
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

        if (JobResult && JobResult->error().code() != TError::OK) {
            return;
        }

        JobResult = jobResult;
        auto resultError = FromProto<TError>(jobResult.error());

        if (resultError.IsOK()) {
            return;
        } 

        if (IsFatalError(resultError)) {
            resultError.Attributes().Set("fatal", IsFatalError(resultError));
            ToProto(JobResult->mutable_error(), resultError);
            FinalJobState = EJobState::Failed;
            return;
        }

        auto abortReason = GetAbortReason(jobResult);
        if (abortReason) {
            resultError.Attributes().Set("abort_reason", abortReason);
            ToProto(JobResult->mutable_error(), resultError);
            FinalJobState = EJobState::Aborted;
            return;
        }

        FinalJobState = EJobState::Failed;
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

    virtual TJobStatistics GetJobStatistics() const override
    {
        TGuard<TSpinLock> guard(ResultLock);
        if (JobResult.HasValue()) {
            return JobResult.Get().statistics();
        } else {
            auto result = JobStatistics;
            result.set_time(GetElapsedTime().MilliSeconds());
            return result;
        }
    }

    virtual void SetJobStatistics(const TJobStatistics& statistics) override
    {
        TGuard<TSpinLock> guard(ResultLock);
        if (JobState == EJobState::Running) {
            JobStatistics = statistics;
        }
    }

    TDuration GetElapsedTime() const
    {
        if (StartTime.HasValue()) {
            return TInstant::Now() - StartTime.Get();
        } else {
            return TDuration::Seconds(0);
        }
    }

private:
    TJobId JobId;
    TJobSpec JobSpec;

    TNodeResources ResourceLimits;
    NCellNode::TBootstrap* Bootstrap;

    TSpinLock ResourcesLock;
    TNodeResources ResourceUsage;

    NLog::TLogger Logger;

    TSlotPtr Slot;

    EJobState JobState;
    EJobPhase JobPhase;

    EJobState FinalJobState;

    double Progress_;
    TJobStatistics JobStatistics;

    TNullable<TInstant> StartTime;

    std::vector<NDataNode::IChunkPtr> CachedChunks;

    // Special node directory used to read cached chunks.
    TNodeDirectoryPtr NodeDirectory;

    IProxyControllerPtr ProxyController;

    // Protects #JobResult and #JobState.
    TSpinLock ResultLock;
    TNullable<TJobResult> JobResult;

    NJobProxy::TJobProxyConfigPtr ProxyConfig;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);


    void DoRun()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ThrowIfFinished();

        try {
            YCHECK(JobPhase == EJobPhase::Created);
            JobPhase = EJobPhase::PreparingConfig;
            PrepareConfig();

            YCHECK(JobPhase == EJobPhase::PreparingConfig);
            JobPhase = EJobPhase::PreparingProxy;
            PrepareProxy();

            YCHECK(JobPhase == EJobPhase::PreparingProxy);
            JobPhase = EJobPhase::PreparingSandbox;
            Slot->InitSandbox();

            YCHECK(JobPhase == EJobPhase::PreparingSandbox);
            JobPhase = EJobPhase::PreparingFiles;
            PrepareUserFiles();

            YCHECK(JobPhase == EJobPhase::PreparingFiles);
            JobPhase = EJobPhase::Running;
            RunJobProxy();
        } catch (const std::exception& ex) {
            DoAbort(ex);
        }
    }

    void PrepareConfig()
    {
        INodePtr ioConfigNode;
        try {
            auto* schedulerJobSpecExt = JobSpec.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            ioConfigNode = ConvertToNode(TYsonString(schedulerJobSpecExt->io_config()));
        } catch (const std::exception& ex) {
            auto error = TError("Error deserializing job IO configuration") << ex;
            THROW_ERROR error;
        }

        auto ioConfig = New<TJobIOConfig>();
        try {
            ioConfig->Load(ioConfigNode);
        } catch (const std::exception& ex) {
            auto error = TError("Error validating job IO configuration") << ex;
            THROW_ERROR error;
        }

        auto proxyConfig = CloneYsonSerializable(Bootstrap->GetJobProxyConfig());
        proxyConfig->JobIO = ioConfig;
        proxyConfig->UserId = Slot->GetUserId();

        auto proxyConfigPath = NFS::CombinePaths(
            Slot->GetWorkingDirectory(),
            ProxyConfigFileName);

        try {
            TFile file(proxyConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
            TFileOutput output(file);
            TYsonWriter writer(&output, EYsonFormat::Pretty);
            proxyConfig->Save(&writer);
        } catch (const std::exception& ex) {
            auto error = TError(NExecAgent::EErrorCode::ConfigCreationFailed, "Error saving job proxy config")
                << ex;
            THROW_ERROR error;
        }
    }

    void PrepareProxy()
    {
        Stroka environmentType = "default";
        try {
            auto environmentManager = Bootstrap->GetEnvironmentManager();
            ProxyController = environmentManager->CreateProxyController(
                //XXX(psushin): execution environment type must not be directly
                // selectable by user -- it is more of the global cluster setting
                //jobSpec.operation_spec().environment(),
                environmentType,
                JobId,
                *Slot,
                Slot->GetWorkingDirectory());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Failed to create proxy controller for environment %Qv",
                environmentType)
                << ex;
        }
    }

    void PrepareUserFiles()
    {
        const TUserJobSpec* userJobSpec = nullptr;
        if (JobSpec.HasExtension(TMapJobSpecExt::map_job_spec_ext)) {
            const auto& jobSpecExt = JobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
            userJobSpec = &jobSpecExt.mapper_spec();
        } else if (JobSpec.HasExtension(TReduceJobSpecExt::reduce_job_spec_ext)) {
            const auto& jobSpecExt = JobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            userJobSpec = &jobSpecExt.reducer_spec();
        } else if (JobSpec.HasExtension(TPartitionJobSpecExt::partition_job_spec_ext)) {
            const auto& jobSpecExt = JobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            if (jobSpecExt.has_mapper_spec()) {
                userJobSpec = &jobSpecExt.mapper_spec();
            }
        }

        if (!userJobSpec)
            return;

        for (const auto& descriptor : userJobSpec->regular_files()) {
            PrepareRegularFile(descriptor);
        }

        for (const auto& descriptor : userJobSpec->table_files()) {
            PrepareTableFile(descriptor);
        }
    }

    void RunJobProxy()
    {
        auto asyncError = ProxyController->Run();

        auto exitResult = CheckedWaitFor(asyncError);
        // NB: we should explicitly call Kill() to clean up possible child processes.
        ProxyController->Kill(Slot->GetProcessGroup(), TError());
        
        THROW_ERROR_EXCEPTION_IF_FAILED(exitResult);

        if (!IsResultSet()) {
            THROW_ERROR_EXCEPTION("Job proxy exited successfully but job result has not been set");
        }

        YCHECK(JobPhase == EJobPhase::Running);
        JobPhase = EJobPhase::Cleanup;

        Slot->Clean();

        YCHECK(JobPhase == EJobPhase::Cleanup);
        JobPhase = EJobPhase::Finished;

        FinalizeJob();
    }

    void FinalizeJob()
    {
        if (Slot) {
            Slot->Release();
        }

        {
            TGuard<TSpinLock> guard(ResultLock);
            JobState = FinalJobState;
        }

        SetResourceUsage(ZeroNodeResources());
        ResourcesReleased_.Fire();
    }

    void DoAbort(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (JobPhase == EJobPhase::Finished) {
            return;
        }
        JobState = EJobState::Aborting;

        const auto jobPhase = JobPhase;
        JobPhase = EJobPhase::Cleanup;

        LOG_INFO(error, "Aborting job");

        if (jobPhase >= EJobPhase::Running) {
            // NB: Kill() never throws.
            ProxyController->Kill(Slot->GetProcessGroup(), error);
        }

        if (jobPhase >= EJobPhase::PreparingSandbox) {
            LOG_INFO("Cleaning slot");
            Slot->Clean();
        }

        JobPhase = EJobPhase::Finished;
        SetResult(error);

        LOG_INFO("Job aborted");

        FinalizeJob();
    }

    void SetResult(const TError& error)
    {
        TJobResult jobResult;
        ToProto(jobResult.mutable_error(), error);
        ToProto(jobResult.mutable_statistics(), GetJobStatistics());
        SetResult(jobResult);
    }

    bool IsResultSet() const
    {
        TGuard<TSpinLock> guard(ResultLock);
        return JobResult.HasValue();
    }


    TFuture<void> DownloadChunks(const NChunkClient::NProto::TRspFetch& fetchRsp)
    {
        auto awaiter = New<TParallelAwaiter>(Slot->GetInvoker());
        auto chunkCache = Bootstrap->GetChunkCache();
        auto this_ = MakeStrong(this);

        for (const auto chunk : fetchRsp.chunks()) {
            auto chunkId = FromProto<TChunkId>(chunk.chunk_id());

            if (IsErasureChunkId(chunkId)) {
                DoAbort(TError("Cannot download erasure chunk %v", chunkId));
                break;
            }

            awaiter->Await(
                chunkCache->DownloadChunk(chunkId),
                BIND([=] (NDataNode::TChunkCache::TDownloadResult result) {
                    if (!result.IsOK()) {
                        auto wrappedError = TError(
                            "Failed to download chunk %v",
                            chunkId)
                            << result;
                        this_->DoAbort(wrappedError);
                        return;
                    }
                    this_->CachedChunks.push_back(result.Value());
                }));
        }

        return awaiter->Complete();
    }

    std::vector<NChunkClient::NProto::TChunkSpec>
    PatchCachedChunkReplicas(const NChunkClient::NProto::TRspFetch& fetchRsp)
    {
        std::vector<NChunkClient::NProto::TChunkSpec> chunks;
        chunks.insert(chunks.end(), fetchRsp.chunks().begin(), fetchRsp.chunks().end());
        for (auto& chunk : chunks) {
            chunk.clear_replicas();
            chunk.add_replicas(ToProto<ui32>(TChunkReplica(InvalidNodeId, 0)));
        }
        return chunks;
    }

    void PrepareRegularFile(const TRegularFileDescriptor& descriptor)
    {
        if (CanPrepareRegularFileViaSymlink(descriptor)) {
            PrepareRegularFileViaSymlink(descriptor);
        } else {
            PrepareRegularFileViaDownload(descriptor);
        }
    }

    bool CanPrepareRegularFileViaSymlink(const TRegularFileDescriptor& descriptor)
    {
        if (descriptor.file().chunks_size() != 1) {
            return false;
        }

        const auto& chunk = descriptor.file().chunks(0);
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunk.chunk_meta().extensions());
        auto compressionCodecId = NCompression::ECodec(miscExt.compression_codec());
        auto chunkId = FromProto<TChunkId>(chunk.chunk_id());
        return !IsErasureChunkId(chunkId) && (compressionCodecId == NCompression::ECodec::None);
    }

    void PrepareRegularFileViaSymlink(const TRegularFileDescriptor& descriptor)
    {
        const auto& chunkSpec = descriptor.file().chunks(0);
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        const auto& fileName = descriptor.file_name();

        LOG_INFO("Preparing regular user file via symlink (FileName: %v, ChunkId: %v)",
            fileName,
            chunkId);

        auto chunkCache = Bootstrap->GetChunkCache();
        auto chunkOrError = CheckedWaitFor(chunkCache->DownloadChunk(chunkId));
        YCHECK(JobPhase == EJobPhase::PreparingFiles);
        THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError, "Failed to download user file %Qv",
            fileName);

        auto chunk = chunkOrError.Value();
        CachedChunks.push_back(chunk);

        try {
            Slot->MakeLink(
                fileName,
                chunk->GetFileName(),
                descriptor.executable());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Failed to create a symlink for %Qv",
                fileName)
                << ex;
        }

        LOG_INFO("Regular user file prepared successfully (FileName: %v)",
            fileName);
    }

    void PrepareRegularFileViaDownload(const TRegularFileDescriptor& descriptor)
    {
        const auto& fileName = descriptor.file_name();

        LOG_INFO("Preparing regular user file via download (FileName: %v, ChunkCount: %v)",
            fileName,
            descriptor.file().chunks_size());

        CheckedWaitFor(DownloadChunks(descriptor.file()));
        YCHECK(JobPhase == EJobPhase::PreparingFiles);

        auto chunks = PatchCachedChunkReplicas(descriptor.file());
        auto config = New<TFileReaderConfig>();

        auto provider = New<TFileChunkReaderProvider>(config);

        typedef TOldMultiChunkSequentialReader<TFileChunkReader> TReader;

        auto reader = New<TReader>(
            config,
            Bootstrap->GetMasterClient()->GetMasterChannel(),
            Bootstrap->GetBlockStore()->GetBlockCache(),
            NodeDirectory,
            std::move(chunks),
            provider);

        try {
            {
                auto result = CheckedWaitFor(reader->AsyncOpen());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            auto producer = [&] (TOutputStream* output) {
                auto* facade = reader->GetFacade();
                while (facade) {
                    auto block = facade->GetBlock();
                    output->Write(block.Begin(),block.Size());

                    if (!reader->FetchNext()) {
                        auto result = CheckedWaitFor(reader->GetReadyEvent());
                        THROW_ERROR_EXCEPTION_IF_FAILED(result);
                    }
                    facade = reader->GetFacade();
                }
            };

            Slot->MakeFile(fileName, producer, descriptor.executable());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Failed to write regular user file %Qv",
                fileName)
                << ex;
        }

        LOG_INFO("Regular user file prepared successfully (FileName: %v)",
            fileName);
    }


    void PrepareTableFile(const TTableFileDescriptor& descriptor)
    {
        LOG_INFO("Preparing user table file (FileName: %v, ChunkCount: %v)",
            descriptor.file_name(),
            descriptor.table().chunks_size());

        CheckedWaitFor(DownloadChunks(descriptor.table()));

        YCHECK(JobPhase == EJobPhase::PreparingFiles);

        auto chunks = PatchCachedChunkReplicas(descriptor.table());

        auto config = New<TTableReaderConfig>();

        auto readerProvider = New<TTableChunkReaderProvider>(
            chunks,
            config);

        auto asyncReader = New<TTableChunkSequenceReader>(
            config,
            Bootstrap->GetMasterClient()->GetMasterChannel(),
            Bootstrap->GetBlockStore()->GetBlockCache(),
            NodeDirectory,
            std::move(chunks),
            readerProvider);

        auto syncReader = CreateSyncReader(asyncReader);
        auto format = ConvertTo<NFormats::TFormat>(TYsonString(descriptor.format()));
        auto fileName = descriptor.file_name();
        try {
            syncReader->Open();

            auto producer = [&] (TOutputStream* output) {
                auto consumer = CreateConsumerForFormat(
                    format,
                    NFormats::EDataType::Tabular,
                    output);
                ProduceYson(syncReader, consumer.get());
            };

            Slot->MakeFile(fileName, producer);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Failed to write user table file %Qv",
                fileName)
                << ex;
        }

        LOG_INFO("User table file prepared successfully (FileName: %v)",
            fileName);
    }

    static TNullable<EAbortReason> GetAbortReason(const TJobResult& jobResult)
    {
        auto resultError = FromProto<TError>(jobResult.error());

        if (resultError.FindMatching(NChunkClient::EErrorCode::AllTargetNodesFailed) || 
            resultError.FindMatching(NChunkClient::EErrorCode::MasterCommunicationFailed) ||
            resultError.FindMatching(NExecAgent::EErrorCode::ConfigCreationFailed))
        {
            return MakeNullable(EAbortReason::Other);
        } else if (resultError.FindMatching(NExecAgent::EErrorCode::ResourceOverdraft)) {
            return MakeNullable(EAbortReason::ResourceOverdraft);
        } else if (resultError.FindMatching(NExecAgent::EErrorCode::AbortByScheduler)) {
            return MakeNullable(EAbortReason::Scheduler);
        }

        if (jobResult.HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext)) {
            const auto& schedulerResultExt = jobResult.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            if (schedulerResultExt.failed_chunk_ids_size() > 0) {
                return MakeNullable(EAbortReason::FailedChunks);
            }
        }

        return Null;
    }

    static bool IsFatalError(const TError& error)
    {
        return
            error.FindMatching(NTableClient::EErrorCode::SortOrderViolation) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded) ||
            error.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNetwork);
    }

    void ThrowIfFinished()
    {
        if (JobPhase == EJobPhase::Finished) {
            throw TFiberCanceledException();
        }
    }

    template <class T>
    T CheckedWaitFor(TFuture<T> future)
    {
        auto result = WaitFor(future);
        ThrowIfFinished();
        return result;
    }

    void CheckedWaitFor(TFuture<void> future)
    {
        WaitFor(future);
        ThrowIfFinished();
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

