#include "stdafx.h"
#include "job.h"
#include "environment_manager.h"
#include "slot.h"
#include "environment.h"
#include "private.h"
#include "slot_manager.h"
#include "config.h"

#include <core/misc/proc.h>

#include <core/actions/cancelable_context.h>

#include <core/logging/log_manager.h>

#include <core/bus/tcp_client.h>

#include <core/rpc/bus_channel.h>

#include <core/concurrency/async_stream.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/file_client/config.h>
#include <ytlib/file_client/file_ypath_proxy.h>
#include <ytlib/file_client/file_chunk_reader.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_writer.h>
#include <ytlib/new_table_client/helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/job_prober_client/job_prober_service_proxy.h>

#include <ytlib/security_client/public.h>

#include <server/data_node/chunk.h>
#include <server/data_node/chunk_cache.h>
#include <server/data_node/block_store.h>
#include <server/data_node/master_connector.h>

#include <server/job_agent/job.h>

#include <server/scheduler/config.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
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
public:
    DEFINE_SIGNAL(void(const TNodeResources&), ResourcesUpdated);

public:
    TJob(
        const TJobId& jobId,
        const TNodeResources& resourceUsage,
        TJobSpec&& jobSpec,
        TBootstrap* bootstrap)
        : JobId(jobId)
        , Bootstrap(bootstrap)
        , ResourceUsage(resourceUsage)
    {
        JobSpec.Swap(&jobSpec);

        NodeDirectory->AddDescriptor(
            InvalidNodeId,
            Bootstrap->GetMasterConnector()->GetLocalDescriptor());

        Logger.AddTag("JobId: %v, JobType: %v",
            GetId(),
            GetType());
    }

    virtual void Start() override
    {
        // No SpinLock here, because concurrent access is impossible before
        // calling Start.
        YCHECK(JobState == EJobState::Waiting);
        JobState = EJobState::Running;

        PrepareTime = TInstant::Now();
        auto slotManager = Bootstrap->GetExecSlotManager();
        Slot = slotManager->AcquireSlot();

        auto invoker = CancelableContext->CreateInvoker(Slot->GetInvoker());
        BIND(&TJob::DoRun, MakeWeak(this))
            .Via(invoker)
            .Run();
    }

    virtual void Abort(const TError& error) override
    {
        if (GetState() == EJobState::Waiting) {
            // Abort before the start.
            YCHECK(!JobResult.HasValue());
            DoSetResult(error);
            SetFinalState();
            return;
        }

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (JobState != EJobState::Running) {
                return;
            }
            DoSetResult(error);
            JobState = EJobState::Aborting;
        }

        CancelableContext->Cancel();
        YCHECK(Slot);
        BIND(&TJob::DoAbort, MakeStrong(this))
            .Via(Slot->GetInvoker())
            .Run();
    }

    virtual const TJobId& GetId() const override
    {
        return JobId;
    }

    virtual EJobType GetType() const override
    {
        return EJobType(JobSpec.type());
    }

    virtual const TJobSpec& GetSpec() const override
    {
        return JobSpec;
    }

    virtual EJobState GetState() const override
    {
        TGuard<TSpinLock> guard(SpinLock);
        return JobState;
    }

    virtual EJobPhase GetPhase() const override
    {
        return JobPhase;
    }

    virtual TNodeResources GetResourceUsage() const override
    {
        TGuard<TSpinLock> guard(SpinLock);
        return ResourceUsage;
    }

    virtual void SetResourceUsage(const TNodeResources& newUsage) override
    {
        TNodeResources delta = newUsage;
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (JobState != EJobState::Running) {
                return;
            }
            delta -= ResourceUsage;
            ResourceUsage = newUsage;
        }
        ResourcesUpdated_.Fire(delta);
    }

    virtual TJobResult GetResult() const override
    {
        TGuard<TSpinLock> guard(SpinLock);
        return JobResult.Get();
    }

    virtual void SetResult(const TJobResult& jobResult) override
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (JobState != EJobState::Running) {
            return;
        }
        DoSetResult(jobResult);
    }

    virtual double GetProgress() const override
    {
        TGuard<TSpinLock> guard(SpinLock);
        return Progress_;
    }

    virtual void SetProgress(double value) override
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (JobState == EJobState::Running) {
            Progress_ = value;
        }
    }

    NJobProberClient::TJobProberServiceProxy CreateJobProber() const
    {
        auto jobProberClient = CreateTcpBusClient(Slot->GetRpcClientConfig());
        auto jobProberChannel = CreateBusChannel(jobProberClient);

        NJobProberClient::TJobProberServiceProxy jobProberProxy(jobProberChannel);
        jobProberProxy.SetDefaultTimeout(Bootstrap->GetConfig()->ExecAgent->JobProberRpcTimeout);
        return jobProberProxy;
    }

    virtual void SetStatistics(const TYsonString& statistics) override
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (JobState == EJobState::Running) {
            Statistics = statistics;
        }
    }

    std::vector<TChunkId> DumpInputContexts() const override
    {
        auto jobProberProxy = CreateJobProber();
        auto req = jobProberProxy.DumpInputContext();

        ToProto(req->mutable_job_id(), JobId);
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return FromProto<TGuid>(rsp->chunk_id());
    }

    virtual TYsonString Strace() const override
    {
        auto jobProberProxy = CreateJobProber();
        auto req = jobProberProxy.Strace();

        ToProto(req->mutable_job_id(), JobId);
        auto res = WaitFor(req->Invoke())
            .ValueOrThrow();

        return TYsonString(FromProto<Stroka>(res->trace()));
    }

private:
    const TJobId JobId;
    NCellNode::TBootstrap* const Bootstrap;

    TJobSpec JobSpec;

    TSpinLock SpinLock;
    TNodeResources ResourceUsage;

    EJobState JobState = EJobState::Waiting;
    EJobPhase JobPhase = EJobPhase::Created;

    TCancelableContextPtr CancelableContext = New<TCancelableContext>();

    double Progress_ = 0.0;
    TYsonString Statistics = SerializedEmptyStatistics;

    TNullable<TJobResult> JobResult;

    TNullable<TInstant> PrepareTime;
    TNullable<TInstant> ExecTime;
    TSlotPtr Slot;

    IProxyControllerPtr ProxyController;

    EJobState FinalJobState = EJobState::Completed;

    std::vector<NDataNode::IChunkPtr> CachedChunks;

    TNodeDirectoryPtr NodeDirectory = New<TNodeDirectory>();

    NLogging::TLogger Logger = ExecAgentLogger;

    void DoRun()
    {
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

            {
                TGuard<TSpinLock> guard(SpinLock);
                ExecTime = TInstant::Now();
            }

            RunJobProxy();
        } catch (const std::exception& ex) {
            {
                TGuard<TSpinLock> guard(SpinLock);
                if (JobState != EJobState::Running) {
                    YCHECK(JobState == EJobState::Aborting);
                    return;
                }
                DoSetResult(ex);
                JobState = EJobState::Aborting;
            }
            BIND(&TJob::DoAbort, MakeStrong(this))
                .Via(Slot->GetInvoker())
                .Run();
        }
    }

    // Must be called with set SpinLock.
    void DoSetResult(const TError& error)
    {
        TJobResult jobResult;
        ToProto(jobResult.mutable_error(), error);
        ToProto(jobResult.mutable_statistics(), Statistics.Data());
        DoSetResult(jobResult);
    }

    // Must be called with set SpinLock.
    void DoSetResult(const TJobResult& jobResult)
    {
        if (JobResult) {
            auto error = FromProto<TError>(JobResult->error());
            if (!error.IsOK()) {
                return;
            }
        }

        JobResult = jobResult;
        if (ExecTime) {
            JobResult->set_exec_time((TInstant::Now() - *ExecTime).MilliSeconds());
            JobResult->set_prepare_time((*ExecTime - *PrepareTime).MilliSeconds());
        } else if (PrepareTime) {
            JobResult->set_prepare_time((TInstant::Now() - *PrepareTime).MilliSeconds());
        }

        auto error = FromProto<TError>(jobResult.error());

        if (error.IsOK()) {
            return;
        }

        if (IsFatalError(error)) {
            error.Attributes().Set("fatal", IsFatalError(error));
            ToProto(JobResult->mutable_error(), error);
            FinalJobState = EJobState::Failed;
            return;
        }

        auto abortReason = GetAbortReason(jobResult);
        if (abortReason) {
            error.Attributes().Set("abort_reason", abortReason);
            ToProto(JobResult->mutable_error(), error);
            FinalJobState = EJobState::Aborted;
            return;
        }

        FinalJobState = EJobState::Failed;
    }

    void PrepareConfig()
    {
        INodePtr ioConfigNode;
        try {
            auto schedulerJobSpecExt = JobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            ioConfigNode = ConvertToNode(TYsonString(schedulerJobSpecExt.io_config()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error deserializing job IO configuration")
                << ex;
        }

        auto ioConfig = New<TJobIOConfig>();
        try {
            ioConfig->Load(ioConfigNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error validating job IO configuration")
                << ex;
        }

        auto proxyConfig = CloneYsonSerializable(Bootstrap->GetJobProxyConfig());
        proxyConfig->JobIO = ioConfig;
        proxyConfig->UserId = Slot->GetUserId();

        proxyConfig->RpcServer = Slot->GetRpcServerConfig();

        auto proxyConfigPath = NFS::CombinePaths(
            Slot->GetWorkingDirectory(),
            ProxyConfigFileName);

        try {
            TFile file(proxyConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
            TFileOutput output(file);
            TYsonWriter writer(&output, EYsonFormat::Pretty);
            proxyConfig->Save(&writer);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error saving job proxy config (Path: %v)",
                proxyConfigPath);
            NLogging::TLogManager::Get()->Shutdown();
            _exit(1);
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
            THROW_ERROR_EXCEPTION("Failed to create proxy controller for environment %Qv",
                environmentType)
                << ex;
        }
    }

    void PrepareUserFiles()
    {
        const auto& schedulerJobSpecExt = JobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (!schedulerJobSpecExt.has_user_job_spec())
            return;

        const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();

        NodeDirectory->MergeFrom(userJobSpec.node_directory());

        for (const auto& descriptor : userJobSpec.regular_files()) {
            PrepareRegularFile(descriptor);
        }

        for (const auto& descriptor : userJobSpec.table_files()) {
            PrepareTableFile(descriptor);
        }
    }

    void RunJobProxy()
    {
        auto runError = WaitFor(ProxyController->Run());

        // NB: We should explicitly call Kill() to clean up possible child processes.
        ProxyController->Kill(Slot->GetProcessGroup());

        runError.ThrowOnError();

        YCHECK(JobResult.HasValue());
        YCHECK(JobPhase == EJobPhase::Running);

        JobPhase = EJobPhase::Cleanup;
        Slot->Clean();
        YCHECK(JobPhase == EJobPhase::Cleanup);

        LOG_INFO("Job completed");

        FinalizeJob();
    }

    void FinalizeJob()
    {
        Slot->Release();
        auto resourceDelta = ZeroNodeResources() - ResourceUsage;
        {
            TGuard<TSpinLock> guard(SpinLock);
            SetFinalState();
        }

        ResourcesUpdated_.Fire(resourceDelta);
    }

    // Must be called with set SpinLock.
    void SetFinalState()
    {
        ResourceUsage = ZeroNodeResources();

        JobPhase = EJobPhase::Finished;
        JobState = FinalJobState;
    }

    void DoAbort()
    {
        if (GetState() != EJobState::Aborting) {
            return;
        }

        LOG_INFO("Aborting job");

        auto prevJobPhase = JobPhase;
        JobPhase = EJobPhase::Cleanup;

        if (prevJobPhase >= EJobPhase::Running) {
            ProxyController->Kill(Slot->GetProcessGroup());
        }

        if (prevJobPhase >= EJobPhase::PreparingSandbox) {
            Slot->Clean();
        }

        LOG_INFO("Job aborted");

        FinalizeJob();
    }

    void DownloadChunks(const google::protobuf::RepeatedPtrField<TChunkSpec>& chunks)
    {
        auto chunkCache = Bootstrap->GetChunkCache();

        std::vector<TFuture<IChunkPtr>> asyncResults;
        for (const auto chunk : chunks) {
            auto chunkId = FromProto<TChunkId>(chunk.chunk_id());
            auto seedReplicas = FromProto<TChunkReplica, TChunkReplicaList>(chunk.replicas());

            if (IsErasureChunkId(chunkId)) {
                THROW_ERROR_EXCEPTION("Some files and/or tables required by job contain erasure chunks");
            }

            asyncResults.push_back(chunkCache->DownloadChunk(
                chunkId,
                NodeDirectory,
                seedReplicas));
        }

        auto resultsOrError = WaitFor(Combine(asyncResults));
        THROW_ERROR_EXCEPTION_IF_FAILED(resultsOrError, "Error downloading chunks required by job");

        const auto& results = resultsOrError.Value();
        CachedChunks.insert(CachedChunks.end(), results.begin(), results.end());
    }

    std::vector<TChunkSpec> PatchCachedChunkReplicas(const google::protobuf::RepeatedPtrField<TChunkSpec>& chunks)
    {
        std::vector<TChunkSpec> result;
        result.insert(result.end(), chunks.begin(), chunks.end());
        for (auto& chunk : result) {
            chunk.clear_replicas();
            chunk.add_replicas(ToProto<ui32>(TChunkReplica(InvalidNodeId, 0)));
        }
        return result;
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
        if (descriptor.chunks_size() != 1) {
            return false;
        }

        const auto& chunk = descriptor.chunks(0);
        auto miscExt = GetProtoExtension<TMiscExt>(chunk.chunk_meta().extensions());
        auto compressionCodecId = NCompression::ECodec(miscExt.compression_codec());
        auto chunkId = FromProto<TChunkId>(chunk.chunk_id());
        return !IsErasureChunkId(chunkId) && (compressionCodecId == NCompression::ECodec::None);
    }

    void PrepareRegularFileViaSymlink(const TRegularFileDescriptor& descriptor)
    {
        const auto& chunkSpec = descriptor.chunks(0);
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto seedReplicas = FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());
        const auto& fileName = descriptor.file_name();

        LOG_INFO("Preparing regular user file via symlink (FileName: %v, ChunkId: %v)",
            fileName,
            chunkId);

        auto chunkCache = Bootstrap->GetChunkCache();
        auto chunkOrError = WaitFor(chunkCache->DownloadChunk(
            chunkId,
            NodeDirectory,
            seedReplicas));
        YCHECK(JobPhase == EJobPhase::PreparingFiles);
        THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError, "Failed to download user file %Qv",
            fileName);

        const auto& chunk = chunkOrError.Value();
        CachedChunks.push_back(chunk);

        try {
            Slot->MakeLink(
                chunk->GetFileName(),
                fileName,
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
            descriptor.chunks_size());

        DownloadChunks(descriptor.chunks());
        YCHECK(JobPhase == EJobPhase::PreparingFiles);

        auto chunks = PatchCachedChunkReplicas(descriptor.chunks());

        auto reader = CreateFileMultiChunkReader(
            New<TFileReaderConfig>(),
            New<TMultiChunkReaderOptions>(),
            Bootstrap->GetMasterClient(),
            Bootstrap->GetBlockCache(),
            NodeDirectory,
            std::move(chunks));

        try {
            WaitFor(reader->Open())
                .ThrowOnError();

            auto producer = [&] (TOutputStream* output) {
                TSharedRef block;
                while (reader->ReadBlock(&block)) {
                    if (block.Empty()) {
                        WaitFor(reader->GetReadyEvent())
                            .ThrowOnError();
                    } else {
                        output->Write(block.Begin(), block.Size());
                    }
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
        const auto& fileName = descriptor.file_name();

        LOG_INFO("Preparing user table file (FileName: %v, ChunkCount: %v)",
            descriptor.file_name(),
            descriptor.chunks_size());

        DownloadChunks(descriptor.chunks());
        YCHECK(JobPhase == EJobPhase::PreparingFiles);

        auto chunks = PatchCachedChunkReplicas(descriptor.chunks());

        auto config = New<TTableReaderConfig>();
        auto options = New<TMultiChunkReaderOptions>();
        auto nameTable = New<TNameTable>();
        auto reader = CreateSchemalessSequentialMultiChunkReader(
            config,
            options,
            Bootstrap->GetMasterClient(),
            Bootstrap->GetBlockCache(),
            NodeDirectory,
            chunks,
            nameTable);

        auto format = ConvertTo<NFormats::TFormat>(TYsonString(descriptor.format()));

        try {
            WaitFor(reader->Open()).ThrowOnError();

            auto producer = [&] (TOutputStream* output) {
                auto controlAttributesConfig = New<TControlAttributesConfig>();
                auto writer = CreateSchemalessWriterForFormat(
                    format,
                    nameTable,
                    CreateAsyncAdapter(output),
                    false,
                    false,
                    0);
                PipeReaderToWriter(reader, writer, controlAttributesConfig, 10000);
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
            resultError.FindMatching(NChunkClient::EErrorCode::MasterNotConnected) ||
            resultError.FindMatching(EErrorCode::ConfigCreationFailed) ||
            resultError.FindMatching(static_cast<int>(EExitStatus::ExitCodeBase) + static_cast<int>(EJobProxyExitCode::HeartbeatFailed)))
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
            error.FindMatching(NVersionedTableClient::EErrorCode::SortOrderViolation) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded) ||
            error.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNetwork) ||
            error.FindMatching(NVersionedTableClient::EErrorCode::InvalidDoubleValue) ||
            error.FindMatching(NVersionedTableClient::EErrorCode::IncomparableType) ||
            error.FindMatching(NVersionedTableClient::EErrorCode::UnhashableType);
    }

};

NJobAgent::IJobPtr CreateUserJob(
    const TJobId& jobId,
    const TNodeResources& resourceUsage,
    TJobSpec&& jobSpec,
    TBootstrap* bootstrap)
{
    return New<TJob>(
        jobId,
        resourceUsage,
        std::move(jobSpec),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


