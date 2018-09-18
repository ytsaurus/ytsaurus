#include "user_job.h"

#include "asan_warning_filter.h"
#include "private.h"
#include "config.h"
#include "job_detail.h"
#include "stderr_writer.h"
#include "user_job_write_controller.h"
#include "job_satellite_connection.h"
#include "user_job_synchronizer.h"
#include "environment.h"

#include <yt/server/core_dump/public.h>
#include <yt/server/core_dump/core_processor_service.h>

#include <yt/server/exec_agent/public.h>
#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/server/misc/public.h>

#include <yt/server/shell/shell_manager.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/core_dump/proto/core_info.pb.h>
#include <yt/ytlib/core_dump/helpers.h>

#include <yt/ytlib/file_client/file_chunk_output.h>

#include <yt/client/formats/parser.h>

#include <yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/ytlib/job_prober_client/job_probe.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/public.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaless_writer.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/client/table_client/schemaful_reader_adapter.h>
#include <yt/client/table_client/schemaful_writer_adapter.h>
#include <yt/client/table_client/table_consumer.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/pattern_formatter.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/process.h>
#include <yt/core/misc/public.h>
#include <yt/core/misc/subprocess.h>
#include <yt/core/misc/signaler.h>

#include <yt/core/net/connection.h>

#include <yt/core/rpc/server.h>

#include <yt/core/tools/tools.h>

#include <yt/core/ypath/tokenizer.h>

#include <util/generic/guid.h>

#include <util/stream/null.h>
#include <util/stream/tee.h>

#include <util/system/compiler.h>
#include <util/system/execpath.h>
#include <util/system/fs.h>

namespace NYT {
namespace NJobProxy {

using namespace NTools;
using namespace NYTree;
using namespace NYson;
using namespace NNet;
using namespace NTableClient;
using namespace NFormats;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NCGroup;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NChunkClient::NProto;
using namespace NPipes;
using namespace NQueryClient;
using namespace NRpc;
using namespace NCoreDump;
using namespace NExecAgent;
using namespace NYPath;
using namespace NJobTrackerClient;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobSpec;
using NScheduler::NProto::TUserJobSpec;
using NCoreDump::NProto::TCoreInfo;
using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

static const int JobStatisticsFD = 5;
static const size_t BufferSize = 1_MB;

static const size_t MaxCustomStatisticsPathLength = 512;

static TNullOutput NullOutput;

////////////////////////////////////////////////////////////////////////////////

static TString CreateNamedPipePath()
{
    const TString& name = CreateGuidAsString();
    return NFS::GetRealPath(NFS::CombinePaths("./pipes", name));
}

////////////////////////////////////////////////////////////////////////////////

class TUserJob
    : public TJob
{
public:
    TUserJob(
        IJobHostPtr host,
        const TUserJobSpec& userJobSpec,
        const TJobId& jobId,
        const std::vector<int>& ports,
        std::unique_ptr<TUserJobWriteController> userJobWriteController)
        : TJob(host)
        , Logger(Host_->GetLogger())
        , UserJobWriteController_(std::move(userJobWriteController))
        , UserJobSpec_(userJobSpec)
        , Config_(Host_->GetConfig())
        , JobIOConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig())
        , UserJobEnvironment_(Host_->CreateUserJobEnvironment())
        , Ports_(ports)
        , JobErrorPromise_(NewPromise<void>())
        , JobEnvironmentType_(ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment)->Type)
        , PipeIOPool_(New<TThreadPool>(JobIOConfig_->PipeIOPoolSize, "PipeIO"))
        , AuxQueue_(New<TActionQueue>("JobAux"))
        , ReadStderrInvoker_(CreateSerializedInvoker(PipeIOPool_->GetInvoker()))
        , JobSatelliteConnection_(
            jobId,
            host->GetConfig()->BusServer,
            JobEnvironmentType_)
    {
        Synchronizer_ = New<TUserJobSynchronizer>();
        Host_->GetRpcServer()->RegisterService(CreateUserJobSynchronizerService(Logger, Synchronizer_, AuxQueue_->GetInvoker()));
        JobProberClient_ = NJobProberClient::CreateJobProbe(JobSatelliteConnection_.GetRpcClientConfig(), jobId);
        auto jobEnvironmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
        MemoryWatchdogPeriod_ = jobEnvironmentConfig->MemoryWatchdogPeriod;

        UserJobReadController_ = CreateUserJobReadController(
            Host_->GetJobSpecHelper(),
            Host_->GetClient(),
            PipeIOPool_->GetInvoker(),
            Host_->LocalDescriptor(),
            BIND(&IJobHost::ReleaseNetwork, Host_),
            SandboxDirectoryNames[ESandboxKind::Udf],
            BlockReadOptions_,
            Host_->GetTrafficMeter(),
            Host_->GetInBandwidthThrottler(),
            Host_->GetOutRpsThrottler());

        InputPipeBlinker_ = New<TPeriodicExecutor>(
            AuxQueue_->GetInvoker(),
            BIND(&TUserJob::BlinkInputPipe, MakeWeak(this)),
            Config_->InputPipeBlinkerPeriod);

        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            AuxQueue_->GetInvoker(),
            BIND(&TUserJob::CheckMemoryUsage, MakeWeak(this)),
            MemoryWatchdogPeriod_);

        if (HasRootPermissions()) {
            UserId_ = jobEnvironmentConfig->StartUid + Config_->SlotIndex;
        }

        if (UserJobEnvironment_) {
            YCHECK(host->GetConfig()->BusServer->UnixDomainName);
            YCHECK(UserId_);
            Process_ = UserJobEnvironment_->CreateUserJobProcess(
                ExecProgramName,
                *UserId_,
                MakeNullable(UserJobSpec_.has_core_table_spec(), *host->GetConfig()->BusServer->UnixDomainName));

            BlockIOWatchdogExecutor_ = New<TPeriodicExecutor>(
                AuxQueue_->GetInvoker(),
                BIND(&TUserJob::CheckBlockIOUsage, MakeWeak(this)),
                UserJobEnvironment_->GetBlockIOWatchdogPeriod());
        } else {
            Process_ = New<TSimpleProcess>(ExecProgramName, false);
            if (UserId_) {
                Process_->AddArguments({"--uid", ::ToString(*UserId_)});
            }
        }

        if (UserJobSpec_.has_core_table_spec()) {
            const auto& coreTableSpec = UserJobSpec_.core_table_spec();

            auto tableWriterOptions = ConvertTo<TTableWriterOptionsPtr>(
                TYsonString(coreTableSpec.output_table_spec().table_writer_options()));
            tableWriterOptions->EnableValidationOptions();
            auto chunkList = FromProto<TChunkListId>(coreTableSpec.output_table_spec().chunk_list_id());
            auto blobTableWriterConfig = ConvertTo<TBlobTableWriterConfigPtr>(TYsonString(coreTableSpec.blob_table_writer_config()));
            auto debugTransactionId = FromProto<TTransactionId>(UserJobSpec_.debug_output_transaction_id());

            CoreProcessorService_ = New<TCoreProcessorService>(
                Host_,
                blobTableWriterConfig,
                tableWriterOptions,
                debugTransactionId,
                chunkList,
                AuxQueue_->GetInvoker(),
                Config_->CoreForwarderTimeout);

            Host_->GetRpcServer()->RegisterService(CoreProcessorService_);
        }
    }

    virtual void Initialize() override
    { }

    virtual TJobResult Run() override
    {
        LOG_DEBUG("Starting job process");

        UserJobWriteController_->Init();

        Prepare();

        bool expected = false;
        if (Prepared_.compare_exchange_strong(expected, true)) {
            ProcessFinished_ = Process_->Spawn();
            LOG_INFO("Job process started");

            if (BlockIOWatchdogExecutor_) {
                BlockIOWatchdogExecutor_->Start();
            }

            TDelayedExecutorCookie timeLimitCookie;
            if (UserJobSpec_.has_job_time_limit()) {
                const TDuration timeLimit = TDuration::MilliSeconds(UserJobSpec_.job_time_limit());
                LOG_INFO("Setting job time limit to %v", timeLimit);
                timeLimitCookie = TDelayedExecutor::Submit(
                    BIND(&TUserJob::OnJobTimeLimitExceeded, MakeWeak(this)).Via(AuxQueue_->GetInvoker()),
                    timeLimit);
            }

            DoJobIO();

            TDelayedExecutor::CancelAndClear(timeLimitCookie);
            WaitFor(InputPipeBlinker_->Stop())
                .ThrowOnError();

            if (!JobErrorPromise_.IsSet()) {
                FinalizeJobIO();
            }
            UploadStderrFile();

            CleanupUserProcesses();

            if (BlockIOWatchdogExecutor_) {
                WaitFor(BlockIOWatchdogExecutor_->Stop())
                    .ThrowOnError();
            }
            WaitFor(MemoryWatchdogExecutor_->Stop())
                .ThrowOnError();
        } else {
            JobErrorPromise_.TrySet(TError("Job aborted"));
        }

        auto jobResultError = JobErrorPromise_.TryGet();

        std::vector<TError> innerErrors;

        if (jobResultError)  {
            innerErrors.push_back(*jobResultError);
        }

        TJobResult result;
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        SaveErrorChunkId(schedulerResultExt);
        UserJobWriteController_->PopulateStderrResult(schedulerResultExt);

        if (jobResultError) {
            try {
                DumpFailContexts(schedulerResultExt);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Failed to dump input context");
            }
        } else {
            UserJobWriteController_->PopulateResult(schedulerResultExt);
        }

        if (UserJobSpec_.has_core_table_spec()) {
            bool coreDumped = jobResultError.HasValue() && jobResultError->Attributes().Get("core_dumped", false /* defaultValue */);
            auto coreResult = CoreProcessorService_->Finalize(coreDumped ? Config_->CoreForwarderTimeout : TDuration::Zero());

            LOG_INFO("User job produced %v core files", coreResult.CoreInfos.size());
            if (!coreResult.CoreInfos.empty()) {
                for (const auto& coreInfo : coreResult.CoreInfos) {
                    LOG_DEBUG("Core file (Pid: %v, ExecutableName: %v, Size: %v)",
                        coreInfo.process_id(),
                        coreInfo.executable_name(),
                        coreInfo.size());
                }
                innerErrors.push_back(TError("User job produced core files")
                        << TErrorAttribute("core_infos", coreResult.CoreInfos));
            }

            ToProto(schedulerResultExt->mutable_core_infos(), coreResult.CoreInfos);
            YCHECK(coreResult.BoundaryKeys.empty() || coreResult.BoundaryKeys.sorted());
            ToProto(schedulerResultExt->mutable_core_table_boundary_keys(), coreResult.BoundaryKeys);
        }

        auto jobError = innerErrors.empty()
            ? TError()
            : TError(EErrorCode::UserJobFailed, "User job failed") << innerErrors;

        ToProto(result.mutable_error(), jobError);

        return result;
    }

    virtual void Cleanup() override
    {
        bool expected = true;
        if (Prepared_.compare_exchange_strong(expected, false)) {
            // Job has been prepared.
            CleanupUserProcesses();
        }
    }

    virtual double GetProgress() const override
    {
        return UserJobReadController_->GetProgress();
    }

    virtual ui64 GetStderrSize() const override
    {
        if (!Prepared_) {
            return 0;
        }
        auto result = WaitFor(BIND([=] () { return ErrorOutput_->GetCurrentSize(); })
            .AsyncVia(ReadStderrInvoker_)
            .Run());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error collecting job stderr size");
        return result.Value();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UserJobReadController_->GetFailedChunkIds();
    }

    virtual TInterruptDescriptor GetInterruptDescriptor() const override
    {
        return UserJobReadController_->GetInterruptDescriptor();
    }

private:
    const NLogging::TLogger Logger;

    const std::unique_ptr<TUserJobWriteController> UserJobWriteController_;
    IUserJobReadControllerPtr UserJobReadController_;

    const TUserJobSpec& UserJobSpec_;

    const TJobProxyConfigPtr Config_;
    const NScheduler::TJobIOConfigPtr JobIOConfig_;
    const IUserJobEnvironmentPtr UserJobEnvironment_;

    std::vector<int> Ports_;

    mutable TPromise<void> JobErrorPromise_;

    EJobEnvironmentType JobEnvironmentType_;

    const TThreadPoolPtr PipeIOPool_;
    const TActionQueuePtr AuxQueue_;
    const IInvokerPtr ReadStderrInvoker_;

    TProcessBasePtr Process_;
    TJobSatelliteConnection JobSatelliteConnection_;

    TString InputPipePath_;

    TNullable<int> UserId_;

    std::atomic<bool> Prepared_ = { false };
    std::atomic<bool> Woodpecker_ = { false };
    std::atomic<bool> JobStarted_ = { false };

    i64 CumulativeMemoryUsageMbSec_ = 0;

    std::atomic<i64> MaximumTmpfsSize_ = {0};

    TDuration MemoryWatchdogPeriod_;

    std::vector<std::unique_ptr<IOutputStream>> TableOutputs_;
    std::vector<std::unique_ptr<TWritingValueConsumer>> WritingValueConsumers_;

    // Writes stderr data to Cypress file.
    std::unique_ptr<TStderrWriter> ErrorOutput_;

    // StderrCombined_ is set only if stderr table is specified.
    // It redirects data to both ErrorOutput_ and stderr table writer.
    std::unique_ptr<TTeeOutput> StderrCombined_;

#ifdef _asan_enabled_
    std::unique_ptr<TAsanWarningFilter> AsanWarningFilter_;
#endif

    std::unique_ptr<TTableOutput> StatisticsOutput_;
    std::unique_ptr<IYsonConsumer> StatisticsConsumer_;

    std::vector<IConnectionReaderPtr> TablePipeReaders_;
    std::vector<IConnectionWriterPtr> TablePipeWriters_;
    IConnectionReaderPtr StatisticsPipeReader_;
    IConnectionReaderPtr StderrPipeReader_;

    std::vector<ISchemalessFormatWriterPtr> FormatWriters_;

    // Actually InputActions_ has only one element,
    // but use vector to reuse runAction code
    std::vector<TCallback<void()>> InputActions_;
    std::vector<TCallback<void()>> OutputActions_;
    std::vector<TCallback<void()>> StderrActions_;
    std::vector<TCallback<void()>> FinalizeActions_;

    TFuture<void> ProcessFinished_;
    std::vector<TString> Environment_;

    NJobProberClient::IJobProbePtr JobProberClient_;

    TPeriodicExecutorPtr MemoryWatchdogExecutor_;
    TPeriodicExecutorPtr BlockIOWatchdogExecutor_;
    TPeriodicExecutorPtr InputPipeBlinker_;

    TIntrusivePtr<TUserJobSynchronizer> Synchronizer_;

    TSpinLock StatisticsLock_;
    TStatistics CustomStatistics_;

    TCoreProcessorServicePtr CoreProcessorService_;

    TNullable<TString> FailContext_;

    void Prepare()
    {
        PreparePipes();

        JobSatelliteConnection_.MakeConfig();

        Process_->AddArguments({"--command", UserJobSpec_.shell_command()});
        Process_->AddArguments({"--config", Host_->AdjustPath(JobSatelliteConnection_.GetConfigPath())});
        Process_->AddArguments({"--job-id", ToString(JobSatelliteConnection_.GetJobId())});
        Process_->SetWorkingDirectory(NFS::CombinePaths(Host_->GetSlotPath(), SandboxDirectoryNames[ESandboxKind::User]));

        if (UserJobSpec_.has_core_table_spec()) {
            Process_->AddArgument("--enable-core-dump");
        }

        // Init environment variables.
        TPatternFormatter formatter;
        formatter.AddProperty(
            "SandboxPath",
            NFS::CombinePaths(Host_->GetSlotPath(), SandboxDirectoryNames[ESandboxKind::User]));

        for (int i = 0; i < UserJobSpec_.environment_size(); ++i) {
            Environment_.emplace_back(formatter.Format(UserJobSpec_.environment(i)));
        }

        if (Host_->GetConfig()->TestRootFS && Host_->GetConfig()->RootPath) {
            Environment_.push_back(Format("YT_ROOT_FS=%v", *Host_->GetConfig()->RootPath));
        }

        // Copy environment to process arguments
        for (const auto& var : Environment_) {
            Process_->AddArguments({"--env", var});
        }

        for (int index = 0; index < Ports_.size(); ++index) {
            Process_->AddArguments({"--env", Format("YT_PORT_%v=%v", index, Ports_[index])});
        }
    }

    void CleanupUserProcesses() const
    {
        BIND(&TUserJob::DoCleanupUserProcesses, MakeWeak(this))
            .Via(PipeIOPool_->GetInvoker())
            .Run();
    }

    void DoCleanupUserProcesses() const
    {
        if (UserJobEnvironment_) {
            UserJobEnvironment_->CleanProcesses();
        }
    }

    void KillUserProcesses()
    {
        if (JobEnvironmentType_ == EJobEnvironmentType::Simple) {
            return;
        }

        BIND(&TUserJob::DoKillUserProcesses, MakeWeak(this))
            .Via(PipeIOPool_->GetInvoker())
            .Run();
    }

    void DoKillUserProcesses()
    {
        try {
            SignalJob("SIGKILL");
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Failed to kill user processes");
        }
    }

    IOutputStream* CreateStatisticsOutput()
    {
        StatisticsConsumer_.reset(new TStatisticsConsumer(
            BIND(&TUserJob::AddCustomStatistics, Unretained(this))));
        auto parser = CreateParserForFormat(
            TFormat(EFormatType::Yson),
            EDataType::Tabular,
            StatisticsConsumer_.get());
        StatisticsOutput_.reset(new TTableOutput(std::move(parser)));
        return StatisticsOutput_.get();
    }

    TMultiChunkWriterOptionsPtr CreateFileOptions()
    {
        auto options = New<TMultiChunkWriterOptions>();
        options->Account = UserJobSpec_.has_file_account()
            ? UserJobSpec_.file_account()
            : NSecurityClient::TmpAccountName;
        options->ReplicationFactor = 1;
        options->ChunksVital = false;
        return options;
    }

    IOutputStream* CreateErrorOutput()
    {
        IOutputStream* result;

        ErrorOutput_.reset(new TStderrWriter(
            UserJobSpec_.max_stderr_size()));

        auto* stderrTableWriter = UserJobWriteController_->GetStderrTableWriter();
        if (stderrTableWriter) {
            StderrCombined_.reset(new TTeeOutput(ErrorOutput_.get(), stderrTableWriter));
            result = StderrCombined_.get();
        } else {
            result = ErrorOutput_.get();
        }

#ifdef _asan_enabled_
        AsanWarningFilter_.reset(new TAsanWarningFilter(result));
        result = AsanWarningFilter_.get();
#endif

        return result;
    }

    void SaveErrorChunkId(TSchedulerJobResultExt* schedulerResultExt)
    {
        if (!ErrorOutput_) {
            return;
        }

        auto errorChunkId = ErrorOutput_->GetChunkId();
        if (errorChunkId) {
            ToProto(schedulerResultExt->mutable_stderr_chunk_id(), errorChunkId);
            LOG_INFO("Stderr chunk generated (ChunkId: %v)", errorChunkId);
        }
    }

    void DumpFailContexts(TSchedulerJobResultExt* schedulerResultExt)
    {
        auto contexts = WaitFor(UserJobReadController_->GetInputContext())
            .ValueOrThrow();

        size_t size = 0;
        for (const auto& context : contexts) {
            size += context.Size();
        }

        FailContext_ = TString();
        FailContext_->reserve(size);
        for (const auto& context : contexts) {
            FailContext_->append(context.Begin(), context.Size());
        }

        auto contextChunkIds = DoDumpInputContext(contexts);

        YCHECK(contextChunkIds.size() <= 1);
        if (!contextChunkIds.empty()) {
            ToProto(schedulerResultExt->mutable_fail_context_chunk_id(), contextChunkIds.front());
        }
    }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        ValidatePrepared();

        auto result = WaitFor(UserJobReadController_->GetInputContext());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error collecting job input context");
        const auto& contexts = result.Value();

        auto chunks = DoDumpInputContext(contexts);
        YCHECK(chunks.size() == 1);

        if (chunks.front() == NullChunkId) {
            THROW_ERROR_EXCEPTION("Cannot dump job context: reading has not started yet");
        }

        return chunks;
    }

    std::vector<TChunkId> DoDumpInputContext(const std::vector<TBlob>& contexts)
    {
        std::vector<TChunkId> result;

        auto transactionId = FromProto<TTransactionId>(UserJobSpec_.debug_output_transaction_id());
        for (int index = 0; index < contexts.size(); ++index) {
            TFileChunkOutput contextOutput(
                JobIOConfig_->ErrorFileWriter,
                CreateFileOptions(),
                Host_->GetClient(),
                transactionId,
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());

            const auto& context = contexts[index];
            contextOutput.Write(context.Begin(), context.Size());
            contextOutput.Finish();

            auto contextChunkId = contextOutput.GetChunkId();
            LOG_INFO("Input context chunk generated (ChunkId: %v, InputIndex: %v)",
                contextChunkId,
                index);

            result.push_back(contextChunkId);
        }

        return result;
    }

    virtual TNullable<TString> GetFailContext() override
    {
        ValidatePrepared();

        return FailContext_;
    }

    virtual TString GetStderr() override
    {
        ValidatePrepared();

        auto result = WaitFor(BIND([=] () { return ErrorOutput_->GetCurrentData(); })
            .AsyncVia(ReadStderrInvoker_)
            .Run());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error collecting job stderr");
        return result.Value();
    }

    virtual TYsonString StraceJob() override
    {
        return JobProberClient_->StraceJob();
    }

    virtual void SignalJob(const TString& signalName) override
    {
        JobProberClient_->SignalJob(signalName);
    }

    virtual TYsonString PollJobShell(const TYsonString& parameters) override
    {
        return JobProberClient_->PollJobShell(parameters);
    }

    virtual void Interrupt() override
    {
        ValidatePrepared();

        UserJobReadController_->InterruptReader();
    }

    virtual void Fail() override
    {
        auto error = TError("Job failed by external request");
        CleanupUserProcesses();
        JobErrorPromise_.TrySet(error);
    }

    void ValidatePrepared()
    {
        if (!Prepared_) {
            THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot operate on job: job has not been prepared yet");
        }
    }

    std::vector<IValueConsumer*> CreateValueConsumers(TTypeConversionConfigPtr typeConversionConfig)
    {
        std::vector<IValueConsumer*> valueConsumers;
        for (const auto& writer : UserJobWriteController_->GetWriters()) {
            WritingValueConsumers_.emplace_back(new TWritingValueConsumer(writer, typeConversionConfig));
            valueConsumers.push_back(WritingValueConsumers_.back().get());
        }
        return valueConsumers;
    }

    void UploadStderrFile()
    {
        if (JobErrorPromise_.IsSet() || UserJobSpec_.upload_stderr_if_completed()) {
            ErrorOutput_->Upload(
                JobIOConfig_->ErrorFileWriter,
                CreateFileOptions(),
                Host_->GetClient(),
                FromProto<TTransactionId>(UserJobSpec_.debug_output_transaction_id()),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());
        }
    }

    void PrepareOutputTablePipes()
    {
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.output_format()));

        const auto& writers = UserJobWriteController_->GetWriters();

        TableOutputs_.resize(writers.size());
        for (int i = 0; i < writers.size(); ++i) {
            auto valueConsumers = CreateValueConsumers(ConvertTo<TTypeConversionConfigPtr>(format.Attributes()));
            auto parser = CreateParserForFormat(format, valueConsumers, i);
            TableOutputs_[i].reset(new TTableOutput(std::move(parser)));

            int jobDescriptor = UserJobSpec_.use_yamr_descriptors()
                ? 3 + i
                : 3 * i + 1;

            // In case of YAMR jobs dup 1 and 3 fd for YAMR compatibility
            auto wrappingError = TError("Error writing to output table %v", i);
            auto reader = (UserJobSpec_.use_yamr_descriptors() && jobDescriptor == 3)
                ? PrepareOutputPipe({1, jobDescriptor}, TableOutputs_[i].get(), &OutputActions_, wrappingError)
                : PrepareOutputPipe({jobDescriptor}, TableOutputs_[i].get(), &OutputActions_, wrappingError);
            TablePipeReaders_.push_back(reader);
        }

        FinalizeActions_.push_back(BIND([=] () {
            auto checkErrors = [&] (const std::vector<TFuture<void>>& asyncErrors) {
                auto error = WaitFor(Combine(asyncErrors));
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing table output");
            };

            std::vector<TFuture<void>> flushResults;
            for (const auto& valueConsumer : WritingValueConsumers_) {
                flushResults.push_back(valueConsumer->Flush());
            }
            checkErrors(flushResults);

            std::vector<TFuture<void>> closeResults;
            for (auto writer : UserJobWriteController_->GetWriters()) {
                closeResults.push_back(writer->Close());
            }
            checkErrors(closeResults);
        }));
    }

    IConnectionReaderPtr PrepareOutputPipe(
        const std::vector<int>& jobDescriptors,
        IOutputStream* output,
        std::vector<TCallback<void()>>* actions,
        const TError& wrappingError)
    {
        auto pipe = TNamedPipe::Create(CreateNamedPipePath());

        for (auto jobDescriptor : jobDescriptors) {
            // Since inside job container we see another rootfs, we must adjust pipe path.
            TNamedPipeConfig pipeId(Host_->AdjustPath(pipe->GetPath()), jobDescriptor, true);
            Process_->AddArguments({"--pipe", ConvertToYsonString(pipeId, EYsonFormat::Text).GetData()});
        }

        auto asyncInput = pipe->CreateAsyncReader();

        actions->push_back(BIND([=] () {
            try {
                auto input = CreateSyncAdapter(asyncInput);
                PipeInputToOutput(input.get(), output, BufferSize);
            } catch (const std::exception& ex) {
                auto error = wrappingError
                    << ex;
                LOG_ERROR(error);

                // We abort asyncInput for stderr.
                // Almost all readers are aborted in `OnIOErrorOrFinished', but stderr doesn't,
                // because we want to read and save as much stderr as possible even if job is failing.
                // But if stderr transferring fiber itself fails, child process may hang
                // if it wants to write more stderr. So we abort input (and therefore close the pipe) here.
                if (asyncInput == StderrPipeReader_) {
                    asyncInput->Abort();
                }

                THROW_ERROR error;
            }
        }));

        return asyncInput;
    }

    void PrepareInputTablePipe()
    {
        int jobDescriptor = 0;
        InputPipePath_= CreateNamedPipePath();
        auto pipe = TNamedPipe::Create(InputPipePath_);
        TNamedPipeConfig pipeId(Host_->AdjustPath(pipe->GetPath()), jobDescriptor, false);
        Process_->AddArguments({"--pipe", ConvertToYsonString(pipeId, EYsonFormat::Text).GetData()});
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.input_format()));

        auto reader = pipe->CreateAsyncReader();
        auto asyncOutput = pipe->CreateAsyncWriter();

        TablePipeWriters_.push_back(asyncOutput);

        auto transferInput = UserJobReadController_->PrepareJobInputTransfer(asyncOutput);
        InputActions_.push_back(BIND([=] () {
            try {
                auto transferComplete = transferInput();
                WaitFor(transferComplete)
                      .ThrowOnError();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Table input pipe failed")
                        << TErrorAttribute("fd", jobDescriptor)
                        << ex;
            }
        }));

        FinalizeActions_.push_back(BIND([=] () {
            if (!UserJobSpec_.check_input_fully_consumed()) {
                return;
            }
            auto buffer = TSharedMutableRef::Allocate(1, false);
            auto future = reader->Read(buffer);
            TErrorOr<size_t> result = WaitFor(future);
            if (!result.IsOK()) {
                reader->Abort();
                THROW_ERROR_EXCEPTION("Failed to check input stream after user process")
                    << TErrorAttribute("fd", jobDescriptor)
                    << result;
            }
            // Try to read some data from the pipe.
            if (result.Value() > 0) {
                THROW_ERROR_EXCEPTION("Input stream was not fully consumed by user process")
                    << TErrorAttribute("fd", jobDescriptor);
            }
            reader->Abort();
        }));
    }

    void PreparePipes()
    {
        LOG_DEBUG("Initializing pipes");

        // We use the following convention for designating input and output file descriptors
        // in job processes:
        // fd == 3 * (N - 1) for the N-th input table (if exists)
        // fd == 3 * (N - 1) + 1 for the N-th output table (if exists)
        // fd == 2 for the error stream
        // e. g.
        // 0 - first input table
        // 1 - first output table
        // 2 - error stream
        // 3 - second input
        // 4 - second output
        // etc.
        //
        // A special option (ToDo(psushin): which one?) enables concatenating
        // all input streams into fd == 0.

        // Configure stderr pipe.
        StderrPipeReader_ = PrepareOutputPipe(
            {STDERR_FILENO},
            CreateErrorOutput(),
            &StderrActions_,
            TError("Error writing to stderr"));

        PrepareOutputTablePipes();

        if (!UserJobSpec_.use_yamr_descriptors()) {
            StatisticsPipeReader_ = PrepareOutputPipe(
                {JobStatisticsFD},
                CreateStatisticsOutput(),
                &OutputActions_,
                TError("Error writing custom job statistics"));
        }

        PrepareInputTablePipe();

        LOG_DEBUG("Pipes initialized");
    }

    void AddCustomStatistics(const INodePtr& sample)
    {
        TGuard<TSpinLock> guard(StatisticsLock_);
        CustomStatistics_.AddSample("/custom", sample);

        size_t customStatisticsCount = 0;
        for (const auto& pair : CustomStatistics_.Data()) {
            if (HasPrefix(pair.first, "/custom")) {
                if (pair.first.size() > MaxCustomStatisticsPathLength) {
                    THROW_ERROR_EXCEPTION(
                        "Custom statistics path is too long: %v > %v",
                        pair.first.size(),
                        MaxCustomStatisticsPathLength);
                }
                ++customStatisticsCount;
            }

            // ToDo(psushin): validate custom statistics path does not contain $.
        }

        if (customStatisticsCount > UserJobSpec_.custom_statistics_count_limit()) {
            THROW_ERROR_EXCEPTION(
                "Custom statistics count exceeded: %v > %v",
                customStatisticsCount,
                UserJobSpec_.custom_statistics_count_limit());
        }
    }

    virtual TStatistics GetStatistics() const override
    {
        TStatistics statistics;
        {
            TGuard<TSpinLock> guard(StatisticsLock_);
            statistics = CustomStatistics_;
        }

        if (const auto& dataStatistics = UserJobReadController_->GetDataStatistics()) {
            statistics.AddSample("/data/input", *dataStatistics);
        }

        if (const auto& codecStatistics = UserJobReadController_->GetDecompressionStatistics()) {
            DumpCodecStatistics(*codecStatistics, "/codec/cpu/decode", &statistics);
        }

        DumpChunkReaderStatistics(&statistics, "/chunk_reader_statistics", BlockReadOptions_.ChunkReaderStatistics);

        auto writers = UserJobWriteController_->GetWriters();
        for (size_t index = 0; index < writers.size(); ++index) {
            const auto& writer = writers[index];
            statistics.AddSample("/data/output/" + ToYPathLiteral(index), writer->GetDataStatistics());
            DumpCodecStatistics(writer->GetCompressionStatistics(), "/codec/cpu/encode/" + ToYPathLiteral(index), &statistics);
        }

        // Cgroups statistics.
        if (UserJobEnvironment_ && Prepared_) {
            try {
                auto cpuStatistics = UserJobEnvironment_->GetCpuStatistics();
                statistics.AddSample("/user_job/cpu", cpuStatistics);
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Unable to get cpu statistics for user job");
            }

            try {
                auto blockIOStatistics = UserJobEnvironment_->GetBlockIOStatistics();
                statistics.AddSample("/user_job/block_io", blockIOStatistics);
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Unable to get block io statistics for user job");
            }

            try {
                auto memoryStatistics = UserJobEnvironment_->GetMemoryStatistics();
                statistics.AddSample("/user_job/current_memory", memoryStatistics);
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Unable to get memory statistics for user job");
            }

            try {
                auto maxMemoryUsage = UserJobEnvironment_->GetMaxMemoryUsage();
                statistics.AddSample("/user_job/max_memory", maxMemoryUsage);
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Unable to get max memory usage for user job");
            }

            statistics.AddSample("/user_job/cumulative_memory_mb_sec", CumulativeMemoryUsageMbSec_);
            statistics.AddSample("/user_job/woodpecker", Woodpecker_ ? 1 : 0);
        }

        statistics.AddSample("/user_job/tmpfs_size", GetTmpfsSize());
        statistics.AddSample("/user_job/max_tmpfs_size", MaximumTmpfsSize_);

        statistics.AddSample("/user_job/memory_limit", UserJobSpec_.memory_limit());
        statistics.AddSample("/user_job/memory_reserve", UserJobSpec_.memory_reserve());

        YCHECK(UserJobSpec_.memory_limit() > 0);
        statistics.AddSample(
            "/user_job/memory_reserve_factor_x10000",
            static_cast<int>((1e4 * UserJobSpec_.memory_reserve()) / UserJobSpec_.memory_limit()));

        // Pipe statistics.
        if (Prepared_) {
            auto inputStatistics = TablePipeWriters_[0]->GetWriteStatistics();
            statistics.AddSample(
                "/user_job/pipes/input/idle_time",
                inputStatistics.IdleDuration);
            statistics.AddSample(
                "/user_job/pipes/input/busy_time",
                inputStatistics.BusyDuration);
            statistics.AddSample(
                "/user_job/pipes/input/bytes",
                TablePipeWriters_[0]->GetWriteByteCount());

            for (int i = 0; i < TablePipeReaders_.size(); ++i) {
                const auto& tablePipeReader = TablePipeReaders_[i];
                auto outputStatistics = tablePipeReader->GetReadStatistics();

                statistics.AddSample(
                    Format("/user_job/pipes/output/%v/idle_time", NYPath::ToYPathLiteral(i)),
                    outputStatistics.IdleDuration);
                statistics.AddSample(
                    Format("/user_job/pipes/output/%v/busy_time", NYPath::ToYPathLiteral(i)),
                    outputStatistics.BusyDuration);
                statistics.AddSample(
                    Format("/user_job/pipes/output/%v/bytes", NYPath::ToYPathLiteral(i)),
                    tablePipeReader->GetReadByteCount());
            }
        }

        return statistics;
    }

    void OnIOErrorOrFinished(const TError& error, const TString& message)
    {
        if (error.IsOK() || error.FindMatching(NNet::EErrorCode::Aborted)) {
            return;
        }

        if (!JobErrorPromise_.TrySet(error)) {
            return;
        }

        LOG_ERROR(error, "%v", message);

        KillUserProcesses();

        for (const auto& reader : TablePipeReaders_) {
            reader->Abort();
        }

        for (const auto& writer : TablePipeWriters_) {
            writer->Abort();
        }

        if (StatisticsPipeReader_) {
            StatisticsPipeReader_->Abort();
        }

        if (!JobStarted_) {
            // If start action didn't finish successfully, stderr could have stayed closed,
            // and output action may hang.
            // But if job is started we want to save as much stderr as possible
            // so we don't close stderr in that case.
            StderrPipeReader_->Abort();
        }
    }

    void DoJobIO()
    {
        auto onIOError = BIND([=] (const TError& error) {
            OnIOErrorOrFinished(error, "Job input/output error, aborting");
        });

        auto onStartIOError = BIND([=] (const TError& error) {
            OnIOErrorOrFinished(error, "Executor input/output error, aborting");
        });

        auto onProcessFinished = BIND([=, this_ = MakeStrong(this)] (const TError& satelliteError) {
            try {
                auto userJobError = Synchronizer_->GetUserProcessStatus();

                LOG_DEBUG("Process finished (UserJobError: %v, SatelliteError: %v)",
                    userJobError,
                    satelliteError);

                // If Syncroniser_->GetUserProcessStatus() returns some status but
                // satellite returns nonzero exit code - it is a bug, or satellite
                // was killed
                if (!satelliteError.IsOK()) {
                    OnIOErrorOrFinished(satelliteError, "Unexpected crash of job satellite");
                } else {
                    OnIOErrorOrFinished(userJobError, "Job control process has finished, aborting");
                }
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Unable to get user process status");

                // Likely it is a real bug in satellite or rpc code.
                LOG_FATAL_IF(satelliteError.IsOK(),
                     "Unable to get process status but satellite returns no errors");
                OnIOErrorOrFinished(satelliteError, "Satellite failed");
            }

            // If process has crashed before sending notification we stuck
            // on Syncroniser_->Wait() call, so cancel wait here.
            // Do this after JobProxyError is set (if necessary).
            Synchronizer_->CancelWait();
        });

        auto runActions = [&] (const std::vector<TCallback<void()>>& actions,
                const NYT::TCallback<void(const TError&)>& onError,
                IInvokerPtr invoker)
        {
            std::vector<TFuture<void>> result;
            for (const auto& action : actions) {
                auto asyncError = action
                    .AsyncVia(invoker)
                    .Run();
                asyncError.Subscribe(onError);
                result.emplace_back(std::move(asyncError));
            }
            return result;
        };

        ProcessFinished_.Subscribe(onProcessFinished);

        // Wait until executor opens and dup named pipes,
        // satellite calls waitpid()
        LOG_DEBUG("Wait for signal from executor/satellite");
        Synchronizer_->Wait();

        auto jobSatelliteRss = Synchronizer_->GetJobSatelliteRssUsage();

        MemoryWatchdogExecutor_->Start();

        if (!JobErrorPromise_.IsSet()) {
            Host_->OnPrepared();
            // Now writing pipe is definitely ready, so we can start blinking.
            InputPipeBlinker_->Start();
            JobStarted_ = true;
        } else {
            LOG_ERROR(JobErrorPromise_.Get(), "Failed to prepare satellite/executor");
            return;
        }
        LOG_INFO("Start actions finished (SatelliteRss: %v)", jobSatelliteRss);
        auto inputFutures = runActions(InputActions_, onIOError, PipeIOPool_->GetInvoker());
        auto outputFutures = runActions(OutputActions_, onIOError, PipeIOPool_->GetInvoker());
        auto stderrFutures = runActions(StderrActions_, onIOError, ReadStderrInvoker_);

        // First, wait for all job output pipes.
        // If job successfully completes or dies prematurely, they close automatically.
        WaitFor(CombineAll(outputFutures))
            .ThrowOnError();
        LOG_INFO("Output actions finished");

        WaitFor(CombineAll(stderrFutures))
            .ThrowOnError();
        LOG_INFO("Error actions finished");

        // Then, wait for job process to finish.
        // Theoretically, process could have explicitely closed its output pipes
        // but still be doing some computations.
        auto jobExitError = WaitFor(ProcessFinished_);
        LOG_INFO(jobExitError, "Job process finished");
        onIOError.Run(jobExitError);

        // Abort input pipes unconditionally.
        // If the job didn't read input to the end, pipe writer could be blocked,
        // because we didn't close the reader end (see check_input_fully_consumed).
        for (const auto& writer : TablePipeWriters_) {
            writer->Abort();
        }

        // Now make sure that input pipes are also completed.
        WaitFor(CombineAll(inputFutures))
            .ThrowOnError();
        LOG_INFO("Input actions finished");
    }

    void FinalizeJobIO()
    {
        for (const auto& action : FinalizeActions_) {
            try {
                action.Run();
            } catch (const std::exception& ex) {
                JobErrorPromise_.TrySet(ex);
            }
        }
    }

    i64 GetMemoryUsageByUid(int uid, pid_t excludePid) const
    {
        auto pids = GetPidsByUid(uid);

        i64 rss = 0;
        // Warning: we can account here a ytserver process in executor mode memory consumption.
        // But this is not a problem because it does not consume much.
        for (int pid : pids) {
            if (pid == excludePid) {
                continue;
            }
            try {
                auto memoryUsage = GetProcessMemoryUsage(pid);
                LOG_DEBUG("Pid: %v, ProcessName: %v, Rss: %v, Shared: %v",
                    pid,
                    GetProcessName(pid),
                    memoryUsage.Rss,
                    memoryUsage.Shared);
                rss += memoryUsage.Rss;
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to get memory usage for pid %v", pid);
            }
        }
        return rss;
    }

    i64 GetTmpfsSize() const
    {
        i64 tmpfsSize = 0;
        if (Config_->TmpfsPath) {
            try {
                auto diskSpaceStatistics = NFS::GetDiskSpaceStatistics(*Config_->TmpfsPath);
                tmpfsSize = diskSpaceStatistics.TotalSpace - diskSpaceStatistics.AvailableSpace;
            } catch (const std::exception& ex) {
                auto error = TError(
                    NJobProxy::EErrorCode::MemoryCheckFailed,
                    "Failed to get tmpfs size") << ex;
                JobErrorPromise_.TrySet(error);
                CleanupUserProcesses();
            }
        }
        return tmpfsSize;
    }

    void CheckMemoryUsage()
    {
        if (!UserId_) {
            LOG_DEBUG("Memory usage control is disabled");
            return;
        }

        auto getMemoryUsage = [&] () {
            try {

                if (UserJobEnvironment_) {
                    auto memoryStatistics = UserJobEnvironment_->GetMemoryStatistics();

                    i64 rss = UserJobSpec_.include_memory_mapped_files() ? memoryStatistics.MappedFile : 0;
                    rss += memoryStatistics.Rss;
                    return rss;
                } else {
                    return GetMemoryUsageByUid(*UserId_, Process_->GetProcessId());
                }
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Unable to get memory statistics to check memory limits");
            }

            return 0l;
        };

        auto rss = getMemoryUsage();
        i64 tmpfsSize = GetTmpfsSize();
        i64 memoryLimit = UserJobSpec_.memory_limit();
        i64 currentMemoryUsage = rss + tmpfsSize;

        CumulativeMemoryUsageMbSec_ += (currentMemoryUsage / 1_MB) * MemoryWatchdogPeriod_.Seconds();

        LOG_DEBUG("Checking memory usage (Tmpfs: %v, Rss: %v, MemoryLimit: %v)",
            tmpfsSize,
            rss,
            memoryLimit);
        if (currentMemoryUsage > memoryLimit) {
            LOG_DEBUG("Memory limit exceeded");
            auto error = TError(
                NJobProxy::EErrorCode::MemoryLimitExceeded,
                "Memory limit exceeded")
                << TErrorAttribute("rss", rss)
                << TErrorAttribute("tmpfs", tmpfsSize)
                << TErrorAttribute("limit", memoryLimit);
            JobErrorPromise_.TrySet(error);
            CleanupUserProcesses();
        }

        MaximumTmpfsSize_ = std::max(MaximumTmpfsSize_.load(), tmpfsSize);

        Host_->SetUserJobMemoryUsage(currentMemoryUsage);
    }

    void CheckBlockIOUsage()
    {
        if (!UserJobEnvironment_) {
            return;
        }

        TBlockIOStatistics blockIOStats;
        try {
            blockIOStats = UserJobEnvironment_->GetBlockIOStatistics();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Unable to get block io statistics to find a woodpecker");
            return;
        }

        if (UserJobSpec_.has_iops_threshold() &&
            blockIOStats.IOTotal > UserJobSpec_.iops_threshold() &&
            !Woodpecker_)
        {
            LOG_DEBUG("Woodpecker detected (IORead: %v, IOTotal: %v, Threshold: %v)",
                blockIOStats.IORead,
                blockIOStats.IOTotal,
                UserJobSpec_.iops_threshold());
            Woodpecker_ = true;

            if (UserJobSpec_.has_iops_throttler_limit()) {
                LOG_DEBUG("Set IO throttle (Iops: %v)", UserJobSpec_.iops_throttler_limit());
                UserJobEnvironment_->SetIOThrottle(UserJobSpec_.iops_throttler_limit());
            }
        }
    }

    void OnJobTimeLimitExceeded()
    {
        auto error = TError(
            NJobProxy::EErrorCode::JobTimeLimitExceeded,
            "Job time limit exceeded")
            << TErrorAttribute("limit", UserJobSpec_.job_time_limit());
        JobErrorPromise_.TrySet(error);
        CleanupUserProcesses();
    }

    // NB(psushin): Read st before asking questions: st/YT-5629.
    void BlinkInputPipe() const
    {
        // This method is called after preparation and before finalization.
        // Reader must be opened and ready, so open must succeed.
        // Still an error can occur in case of external forced sandbox clearance (e.g. in integration tests).
        auto fd = HandleEintr(::open, InputPipePath_.c_str(), O_WRONLY |  O_CLOEXEC | O_NONBLOCK);
        if (fd >= 0) {
            ::close(fd);
        } else {
            LOG_WARNING(TError::FromSystem(), "Failed to blink input pipe (Path: %v)", InputPipePath_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(
    IJobHostPtr host,
    const TUserJobSpec& userJobSpec,
    const TJobId& jobId,
    const std::vector<int>& ports,
    std::unique_ptr<TUserJobWriteController> userJobWriteController)
{
    return New<TUserJob>(
        host,
        userJobSpec,
        jobId,
        std::move(ports),
        std::move(userJobWriteController));
}

#else

IJobPtr CreateUserJob(
    IJobHostPtr host,
    const TUserJobSpec& UserJobSpec_,
    const TJobId& jobId,
    const std::vector<int>& ports,
    std::unique_ptr<TUserJobWriteController> userJobWriteController)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
