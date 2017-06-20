#include "user_job.h"
#include "private.h"
#include "config.h"
#include "job_detail.h"
#include "stderr_writer.h"
#include "stracer.h"
#include "user_job_io.h"

#include <yt/server/core_dump/public.h>
#include <yt/server/core_dump/core_processor_service.h>

#include <yt/server/exec_agent/public.h>
#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/server/program/names.h>

#include <yt/server/job_proxy/job_signaler.h>

#include <yt/server/shell/shell_manager.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/core_dump/core_info.pb.h>
#include <yt/ytlib/core_dump/helpers.h>

#include <yt/ytlib/file_client/file_chunk_output.h>

#include <yt/ytlib/formats/parser.h>

#include <yt/ytlib/job_proxy/user_job_read_controller.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/public.h>
#include <yt/ytlib/query_client/query_statistics.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>
#include <yt/ytlib/table_client/schemaful_writer_adapter.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/schemaless_writer.h>
#include <yt/ytlib/table_client/table_consumer.h>

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

#include <yt/core/pipes/async_reader.h>
#include <yt/core/pipes/async_writer.h>

#include <yt/core/rpc/server.h>

#include <yt/core/tools/tools.h>

#include <yt/core/ypath/tokenizer.h>

#include <util/system/fs.h>

#include <util/stream/null.h>

#include <util/system/execpath.h>

#include <util/generic/guid.h>
#include <util/stream/tee.h>

namespace NYT {
namespace NJobProxy {

using namespace NTools;
using namespace NYTree;
using namespace NYson;
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
using namespace NShell;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobSpec;
using NScheduler::NProto::TUserJobSpec;
using NCoreDump::NProto::TCoreInfo;
using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

static const int JobStatisticsFD = 5;
static TString CGroupBase = "user_jobs";
static TString CGroupPrefix = CGroupBase + "/yt-job-";

static const size_t BufferSize = (size_t) 1024 * 1024;

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
        std::unique_ptr<IUserJobIO> userJobIO)
        : TJob(host)
        , JobIO_(std::move(userJobIO))
        , UserJobSpec_(userJobSpec)
        , Config_(Host_->GetConfig())
        , JobIOConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig())
        , CGroupsConfig_(Host_->GetCGroupsConfig())
        , JobErrorPromise_(NewPromise<void>())
        , PipeIOPool_(New<TThreadPool>(JobIOConfig_->PipeIOPoolSize, "PipeIO"))
        , AuxQueue_(New<TActionQueue>("JobAux"))
        , ReadStderrInvoker_(CreateSerializedInvoker(PipeIOPool_->GetInvoker()))
        , Process_(New<TProcess>(ExecProgramName, false))
        , CpuAccounting_(CGroupPrefix + ToString(jobId))
        , BlockIO_(CGroupPrefix + ToString(jobId))
        , Memory_(CGroupPrefix + ToString(jobId))
        , Freezer_(CGroupPrefix + ToString(jobId))
        , Logger(Host_->GetLogger())
    {
        auto jobEnvironmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
        MemoryWatchdogPeriod_ = jobEnvironmentConfig->MemoryWatchdogPeriod;

        UserJobReadController_ = CreateUserJobReadController(
            Host_->GetJobSpecHelper(),
            Host_->GetClient(),
            PipeIOPool_->GetInvoker(),
            Host_->LocalDescriptor(),
            BIND(&IJobHost::ReleaseNetwork, Host_),
            SandboxDirectoryNames[ESandboxKind::Udf]);

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

        if (CGroupsConfig_) {
            BlockIOWatchdogExecutor_ = New<TPeriodicExecutor>(
                AuxQueue_->GetInvoker(),
                BIND(&TUserJob::CheckBlockIOUsage, MakeWeak(this)),
                CGroupsConfig_->BlockIOWatchdogPeriod);
        }

        if (UserJobSpec_.has_core_table_spec()) {
            const auto& coreTableSpec = UserJobSpec_.core_table_spec();

            auto tableWriterOptions = ConvertTo<TTableWriterOptionsPtr>(
                TYsonString(coreTableSpec.output_table_spec().table_writer_options()));
            tableWriterOptions->EnableValidationOptions();
            auto chunkList = FromProto<TChunkListId>(coreTableSpec.output_table_spec().chunk_list_id());
            auto blobTableWriterConfig = ConvertTo<TBlobTableWriterConfigPtr>(TYsonString(coreTableSpec.blob_table_writer_config()));
            auto transactionId = FromProto<TTransactionId>(
                Host_->GetJobSpecHelper()->GetSchedulerJobSpecExt().output_transaction_id());

            CoreProcessorService_ = New<TCoreProcessorService>(
                Host_,
                blobTableWriterConfig,
                tableWriterOptions,
                transactionId,
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

        JobIO_->Init();

        Prepare();

        bool expected = false;
        if (Prepared_.compare_exchange_strong(expected, true)) {
            ProcessFinished_ = Process_->Spawn();
            LOG_INFO("Job process started");

            MemoryWatchdogExecutor_->Start();
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
            WaitFor(InputPipeBlinker_->Stop());

            if (!JobErrorPromise_.IsSet()) {
                FinalizeJobIO();
            }

            auto error = TError("Job finished");
            WaitForActiveShellProcesses(error);
            CleanupUserProcesses(error);

            if (BlockIOWatchdogExecutor_) {
                WaitFor(BlockIOWatchdogExecutor_->Stop());
            }
            WaitFor(MemoryWatchdogExecutor_->Stop());
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
        JobIO_->PopulateStderrResult(schedulerResultExt);

        if (jobResultError) {
            try {
                DumpFailContexts(schedulerResultExt);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Failed to dump input context");
            }
        } else {
            JobIO_->PopulateResult(schedulerResultExt);
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
            : TError("User job failed") << innerErrors;

        ToProto(result.mutable_error(), jobError);

        return result;
    }

    virtual void Abort() override
    {
        bool expected = true;
        if (Prepared_.compare_exchange_strong(expected, false)) {
            // Job has been prepared.
            CleanupUserProcesses(TError("Job aborted"));
        }
    }

    virtual double GetProgress() const override
    {
        return UserJobReadController_->GetProgress();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UserJobReadController_->GetFailedChunkIds();
    }

    virtual std::vector<TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override
    {
        return UserJobReadController_->GetUnreadDataSliceDescriptors();
    }

private:
    const std::unique_ptr<IUserJobIO> JobIO_;
    TUserJobReadControllerPtr UserJobReadController_;

    const TUserJobSpec& UserJobSpec_;

    const TJobProxyConfigPtr Config_;
    const NScheduler::TJobIOConfigPtr JobIOConfig_;

    TString InputPipePath_;

    TCGroupJobEnvironmentConfigPtr CGroupsConfig_;
    TNullable<int> UserId_;

    mutable TPromise<void> JobErrorPromise_;

    std::atomic<bool> Prepared_ = { false };
    std::atomic<bool> IsWoodpecker_ = { false };
    std::atomic<bool> JobStarted_ = { false };

    std::atomic_flag Stracing_ = ATOMIC_FLAG_INIT;

    i64 CumulativeMemoryUsageMbSec_ = 0;

    TDuration MemoryWatchdogPeriod_;

    const TThreadPoolPtr PipeIOPool_;
    const TActionQueuePtr AuxQueue_;
    IInvokerPtr ReadStderrInvoker_;

    std::vector<std::unique_ptr<TOutputStream>> TableOutputs_;
    std::vector<std::unique_ptr<TWritingValueConsumer>> WritingValueConsumers_;

    // Writes stderr data to cypress file.
    std::unique_ptr<TStderrWriter> ErrorOutput_;

    // StderrCombined_ is set only if stderr table is specified.
    // It redirects data to both ErrorOutput_ and stderr table writer.
    std::unique_ptr<TTeeOutput> StderrCombined_;

    std::unique_ptr<TTableOutput> StatisticsOutput_;
    std::unique_ptr<IYsonConsumer> StatisticsConsumer_;

    std::vector<TAsyncReaderPtr> TablePipeReaders_;
    std::vector<TAsyncWriterPtr> TablePipeWriters_;
    TAsyncReaderPtr ControlPipeReader_;
    TAsyncReaderPtr StatisticsPipeReader_;
    TAsyncReaderPtr StderrPipeReader_;

    // Actually StartActions_ and InputActions_ has only one element,
    // but use vector to reuse runAction code
    std::vector<TCallback<void()>> StartActions_;
    std::vector<TCallback<void()>> InputActions_;
    std::vector<TCallback<void()>> OutputActions_;
    std::vector<TCallback<void()>> StderrActions_;
    std::vector<TCallback<void()>> FinalizeActions_;

    TProcessPtr Process_;
    TFuture<void> ProcessFinished_;
    std::vector<TString> Environment_;

    // Destroy shell manager before user job cgrops, since its cgroups are typically
    // nested, and we need to mantain destroy order.
    IShellManagerPtr ShellManager_;

    TCpuAccounting CpuAccounting_;
    TBlockIO BlockIO_;
    TMemory Memory_;
    TFreezer Freezer_;
    TSpinLock FreezerLock_;

    TPeriodicExecutorPtr MemoryWatchdogExecutor_;
    TPeriodicExecutorPtr BlockIOWatchdogExecutor_;
    TPeriodicExecutorPtr InputPipeBlinker_;

    const NLogging::TLogger Logger;

    TSpinLock StatisticsLock_;
    TStatistics CustomStatistics_;

    TCoreProcessorServicePtr CoreProcessorService_;

    void Prepare()
    {
        PrepareCGroups();

        PreparePipes();

        Process_->AddArguments({"--command", UserJobSpec_.shell_command()});
        Process_->SetWorkingDirectory(SandboxDirectoryNames[ESandboxKind::User]);

        if (UserJobSpec_.has_core_table_spec()) {
            Process_->AddArgument("--enable-core-dump");
        }

        if (UserId_) {
            Process_->AddArguments({"--uid", ::ToString(*UserId_)});
        }

        // Init environment variables.
        TPatternFormatter formatter;
        formatter.AddProperty(
            "SandboxPath",
            NFS::CombinePaths(~NFs::CurrentWorkingDirectory(), SandboxDirectoryNames[ESandboxKind::User]));

        for (int i = 0; i < UserJobSpec_.environment_size(); ++i) {
            Environment_.emplace_back(formatter.Format(UserJobSpec_.environment(i)));
        }

        // Copy environment to process arguments
        std::vector<TString> shellEnvironment;
        shellEnvironment.reserve(Environment_.size());
        for (const auto& var : Environment_) {
            Process_->AddArguments({"--env", var});
            if (var.StartsWith("YT_")) {
                shellEnvironment.emplace_back(var);
            }
        }

        ShellManager_ = CreateShellManager(
            NFS::CombinePaths(NFs::CurrentWorkingDirectory(), SandboxDirectoryNames[ESandboxKind::Home]),
            UserId_,
            TNullable<TString>(static_cast<bool>(CGroupsConfig_), CGroupBase),
            Format("Job environment:\n%v\n", JoinToString(shellEnvironment, STRINGBUF("\n"))),
            std::move(shellEnvironment));
    }

    void WaitForActiveShellProcesses(const TError& error)
    {
        // Ignore errors.
        WaitFor(BIND(&IShellManager::GracefulShutdown, ShellManager_, error)
            .AsyncVia(AuxQueue_->GetInvoker())
            .Run());
    }

    void CleanupUserProcesses(const TError& error) const
    {
        BIND(&IShellManager::Terminate, ShellManager_, error)
            .Via(AuxQueue_->GetInvoker())
            .Run();
        BIND(&TUserJob::DoCleanupUserProcesses, MakeWeak(this))
            .Via(PipeIOPool_->GetInvoker())
            .Run();
    }

    void DoCleanupUserProcesses() const
    {
        if (!CGroupsConfig_) {
            return;
        }

        try {
            // Kill everything for sanity reasons: main user process completed,
            // but its children may still be alive.
            RunKiller(Freezer_.GetFullPath());
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to clean up user processes");
        }
    }

    TOutputStream* CreateStatisticsOutput()
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

    TOutputStream* CreateErrorOutput()
    {
        ErrorOutput_.reset(new TStderrWriter(
            JobIOConfig_->ErrorFileWriter,
            CreateFileOptions(),
            Host_->GetClient(),
            FromProto<TTransactionId>(UserJobSpec_.async_scheduler_transaction_id()),
            UserJobSpec_.max_stderr_size()));

        auto* stderrTableWriter = JobIO_->GetStderrTableWriter();
        if (stderrTableWriter) {
            StderrCombined_.reset(new TTeeOutput(ErrorOutput_.get(), stderrTableWriter));
            return StderrCombined_.get();
        } else {
            return ErrorOutput_.get();
        }
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

        auto transactionId = FromProto<TTransactionId>(UserJobSpec_.async_scheduler_transaction_id());
        for (int index = 0; index < contexts.size(); ++index) {
            TFileChunkOutput contextOutput(
                JobIOConfig_->ErrorFileWriter,
                CreateFileOptions(),
                Host_->GetClient(),
                transactionId);

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
        ValidatePrepared();

        if (Stracing_.test_and_set()) {
            THROW_ERROR_EXCEPTION("Another strace session is in progress");
        }

        auto guard = Finally([&] () {
            Stracing_.clear();
        });

        auto pids = GetPidsFromFreezer();
        auto result = WaitFor(BIND([=] () { return RunTool<TStraceTool>(pids); })
            .AsyncVia(AuxQueue_->GetInvoker())
            .Run());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error running job strace tool");

        return ConvertToYsonString(result.Value());
    }

    virtual void SignalJob(const TString& signalName) override
    {
        ValidatePrepared();

        auto arg = New<TJobSignalerArg>();
        arg->Pids = GetPidsFromFreezer();
        auto it = std::find(arg->Pids.begin(), arg->Pids.end(), Process_->GetProcessId());
        if (it != arg->Pids.end()) {
            arg->Pids.erase(it);
        }
        if (arg->Pids.empty()) {
            THROW_ERROR_EXCEPTION("No processes in the job to send signal");
        }

        arg->SignalName = signalName;
        LOG_INFO("Sending signal %v to pids %v",
            arg->SignalName,
            arg->Pids);

        auto result = WaitFor(BIND([=] () { return RunTool<TJobSignalerTool>(arg); })
            .AsyncVia(AuxQueue_->GetInvoker())
            .Run());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error running job signaler tool");
    }

    virtual TYsonString PollJobShell(const TYsonString& parameters) override
    {
        ValidatePrepared();

        auto result = WaitFor(BIND([=] () { return ShellManager_->PollJobShell(parameters); })
            .AsyncVia(AuxQueue_->GetInvoker())
            .Run());

        return result
            .ValueOrThrow();
    }

    virtual void Interrupt() override
    {
        ValidatePrepared();

        UserJobReadController_->InterruptReader();
    }

    void ValidatePrepared()
    {
        if (!Prepared_) {
            THROW_ERROR_EXCEPTION("Cannot operate on job: job has not been prepared yet");
        }
    }

    std::vector<int> GetPidsFromFreezer()
    {
        TGuard<TSpinLock> guard(FreezerLock_);
        if (!Freezer_.IsCreated()) {
            THROW_ERROR_EXCEPTION("Cannot determine pids of user job processes: freezer cgroup is not created yet");
        }
        return Freezer_.GetTasks();
    }

    std::vector<IValueConsumer*> CreateValueConsumers(TTypeConversionConfigPtr typeConversionConfig)
    {
        std::vector<IValueConsumer*> valueConsumers;
        for (const auto& writer : JobIO_->GetWriters()) {
            WritingValueConsumers_.emplace_back(new TWritingValueConsumer(writer, typeConversionConfig));
            valueConsumers.push_back(WritingValueConsumers_.back().get());
        }
        return valueConsumers;
    }

    void PrepareOutputTablePipes()
    {
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.output_format()));

        const auto& writers = JobIO_->GetWriters();

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
            for (const auto& valueConsumer : WritingValueConsumers_) {
                valueConsumer->Flush();
            }

            std::vector<TFuture<void>> asyncResults;
            for (auto writer : JobIO_->GetWriters()) {
                asyncResults.push_back(writer->Close());
            }

            auto error = WaitFor(Combine(asyncResults));
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing table output");
        }));
    }

    TAsyncReaderPtr PrepareOutputPipe(
        const std::vector<int>& jobDescriptors,
        TOutputStream* output,
        std::vector<TCallback<void()>>* actions,
        const TError& wrappingError)
    {
        auto pipe = TNamedPipe::Create(CreateNamedPipePath());

        for (auto jobDescriptor : jobDescriptors) {
            TNamedPipeConfig pipeId(pipe->GetPath(), jobDescriptor, true);
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

    void CreateControlPipe()
    {
        auto pipe = TNamedPipe::Create(CreateNamedPipePath());

        Process_->AddArguments({"--control-pipe-path", pipe->GetPath()});

        ControlPipeReader_ = pipe->CreateAsyncReader();

        StartActions_.push_back(BIND([=] () {
            try {
                auto input = CreateSyncAdapter(ControlPipeReader_);
                auto data = input->ReadLine();

                auto executorResult = ConvertTo<TError>(TYsonString(data));
                executorResult.ThrowOnError();
                WaitFor(ControlPipeReader_->Abort())
                  .ThrowOnError();

                // Notify node process that user job is fully prepared and running.
                Host_->OnPrepared();

                // Now writing pipe is definitely ready, so we can start blinking.
                InputPipeBlinker_->Start();

                JobStarted_ = true;
            } catch (const std::exception& ex) {
                auto error = TError("Start action failed") << ex;
                LOG_ERROR(error);
                THROW_ERROR error;
            }
        }));
    }

    void PrepareInputTablePipe()
    {
        int jobDescriptor = 0;
        InputPipePath_= CreateNamedPipePath();
        auto pipe = TNamedPipe::Create(InputPipePath_);
        TNamedPipeConfig pipeId(pipe->GetPath(), jobDescriptor, false);
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

        CreateControlPipe();

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

    void PrepareCGroups()
    {
        if (!CGroupsConfig_) {
            return;
        }

        try {
            {
                TGuard<TSpinLock> guard(FreezerLock_);
                Freezer_.Create();
                Process_->AddArguments({"--cgroup", Freezer_.GetFullPath()});
            }

            if (CGroupsConfig_->IsCGroupSupported(TCpuAccounting::Name)) {
                CpuAccounting_.Create();
                Process_->AddArguments({"--cgroup", CpuAccounting_.GetFullPath()});
                Environment_.emplace_back(Format("YT_CGROUP_CPUACCT=%v", CpuAccounting_.GetFullPath()));
            }

            if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
                BlockIO_.Create();
                Process_->AddArguments({"--cgroup", BlockIO_.GetFullPath()});
                Environment_.emplace_back(Format("YT_CGROUP_BLKIO=%v", BlockIO_.GetFullPath()));
            }

            if (CGroupsConfig_->IsCGroupSupported(TMemory::Name)) {
                Memory_.Create();
                Process_->AddArguments({"--cgroup", Memory_.GetFullPath()});
                Environment_.emplace_back(Format("YT_CGROUP_MEMORY=%v", Memory_.GetFullPath()));
            }
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to create required cgroups");
        }
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

        int i = 0;
        for (const auto& writer : JobIO_->GetWriters()) {
            statistics.AddSample(
                "/data/output/" + NYPath::ToYPathLiteral(i),
                writer->GetDataStatistics());
            ++i;
        }

        // Cgroups statistics.
        if (CGroupsConfig_ && Prepared_) {
            if (CGroupsConfig_->IsCGroupSupported(TCpuAccounting::Name)) {
                statistics.AddSample("/user_job/cpu", CpuAccounting_.GetStatistics());
            }

            if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
                statistics.AddSample("/user_job/block_io", BlockIO_.GetStatistics());
            }

            if (CGroupsConfig_->IsCGroupSupported(TMemory::Name)) {
                statistics.AddSample("/user_job/max_memory", Memory_.GetMaxMemoryUsage());
                statistics.AddSample("/user_job/current_memory", Memory_.GetStatistics());
            }

            statistics.AddSample("/user_job/cumulative_memory_mb_sec", CumulativeMemoryUsageMbSec_);
            statistics.AddSample("/user_job/woodpecker", IsWoodpecker_ ? 1 : 0);
        }

        statistics.AddSample("/user_job/tmpfs_size", GetTmpfsSize());

        statistics.AddSample("/user_job/memory_limit", UserJobSpec_.memory_limit());
        statistics.AddSample("/user_job/memory_reserve", UserJobSpec_.memory_reserve());

        YCHECK(UserJobSpec_.memory_limit() > 0);
        statistics.AddSample(
            "/user_job/memory_reserve_factor_x10000",
            static_cast<int>((1e4 * UserJobSpec_.memory_reserve()) / UserJobSpec_.memory_limit()));

        // Pipe statistics.
        if (Prepared_) {
            statistics.AddSample(
                "/user_job/pipes/input/idle_time",
                WaitFor(TablePipeWriters_[0]->GetIdleDuration()).Value());
            statistics.AddSample(
                "/user_job/pipes/input/busy_time",
                WaitFor(TablePipeWriters_[0]->GetBusyDuration()).Value());
            statistics.AddSample(
                "/user_job/pipes/input/bytes",
                TablePipeWriters_[0]->GetByteCount());

            for (int i = 0; i < TablePipeReaders_.size(); ++i) {
                const auto& tablePipeReader = TablePipeReaders_[i];

                statistics.AddSample(
                    Format("/user_job/pipes/output/%v/idle_time", NYPath::ToYPathLiteral(i)),
                    WaitFor(tablePipeReader->GetIdleDuration()).Value());
                statistics.AddSample(
                    Format("/user_job/pipes/output/%v/busy_time", NYPath::ToYPathLiteral(i)),
                    WaitFor(tablePipeReader->GetBusyDuration()).Value());
                statistics.AddSample(
                    Format("/user_job/pipes/output/%v/bytes", NYPath::ToYPathLiteral(i)),
                    tablePipeReader->GetByteCount());
            }
        }

        return statistics;
    }

    void OnIOErrorOrFinished(const TError& error, const TString& message)
    {
        if (error.IsOK() || error.FindMatching(NPipes::EErrorCode::Aborted)) {
            return;
        }

        if (!JobErrorPromise_.TrySet(error)) {
            return;
        }

        LOG_ERROR(error, "%v", message);

        WaitForActiveShellProcesses(error);
        CleanupUserProcesses(error);

        ControlPipeReader_->Abort();

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

        auto onProcessFinished = BIND([=] (const TError& error) {
            OnIOErrorOrFinished(error, "Job control process has finished, aborting");
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

        auto startFutures = runActions(StartActions_, onStartIOError, PipeIOPool_->GetInvoker());
        // Wait until executor opens and dup named pipes.

        ProcessFinished_.Subscribe(onProcessFinished);

        WaitFor(CombineAll(startFutures));
        LOG_INFO("Start actions finished");

        auto inputFutures = runActions(InputActions_, onIOError, PipeIOPool_->GetInvoker());
        auto outputFutures = runActions(OutputActions_, onIOError, PipeIOPool_->GetInvoker());
        auto stderrFutures = runActions(StderrActions_, onIOError, ReadStderrInvoker_);

        // First, wait for all job output pipes.
        // If job successfully completes or dies prematurely, they close automatically.
        WaitFor(CombineAll(outputFutures));
        WaitFor(CombineAll(stderrFutures));
        LOG_INFO("Output actions finished");

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
        WaitFor(CombineAll(inputFutures));
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

    i64 GetMemoryUsageByUid(int uid) const
    {
        auto pids = GetPidsByUid(uid);

        i64 rss = 0;
        // Warning: we can account here a ytserver process in executor mode memory consumption.
        // But this is not a problem because it does not consume much.
        for (int pid : pids) {
            try {
                i64 processRss = GetProcessRss(pid);
                LOG_DEBUG("PID: %v, ProcessName: %Qv, RSS: %v",
                    pid,
                    GetProcessName(pid),
                    processRss);
                rss += processRss;
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to get RSS for PID %v", pid);
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
                CleanupUserProcesses(error);
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

        i64 rss = GetMemoryUsageByUid(*UserId_);

        if (Memory_.IsCreated()) {
            auto statistics = Memory_.GetStatistics();

            i64 uidRss = rss;
            rss = UserJobSpec_.include_memory_mapped_files() ? statistics.MappedFile : 0;
            rss += statistics.Rss;

            if (rss > 1.05 * uidRss && uidRss > 0) {
                LOG_ERROR("Memory usage measured by cgroup is much greater than via procfs: %v > %v",
                    rss,
                    uidRss);
            }
        }

        i64 tmpfsSize = GetTmpfsSize();
        i64 memoryLimit = UserJobSpec_.memory_limit();
        i64 currentMemoryUsage = rss + tmpfsSize;

        CumulativeMemoryUsageMbSec_ += (currentMemoryUsage / (1024 * 1024)) * MemoryWatchdogPeriod_.Seconds();

        LOG_DEBUG("Checking memory usage (Tmpfs: %v, Rss: %v, MemoryLimit: %v)",
            tmpfsSize,
            rss,
            memoryLimit);
        if (currentMemoryUsage > memoryLimit) {
            auto error = TError(
                NJobProxy::EErrorCode::MemoryLimitExceeded,
                "Memory limit exceeded")
                << TErrorAttribute("rss", rss)
                << TErrorAttribute("tmpfs", tmpfsSize)
                << TErrorAttribute("limit", memoryLimit);
            JobErrorPromise_.TrySet(error);
            CleanupUserProcesses(error);
        }

        Host_->SetUserJobMemoryUsage(currentMemoryUsage);
    }

    void CheckBlockIOUsage()
    {
        if (!BlockIO_.IsCreated()) {
            return;
        }

        auto statistics = BlockIO_.GetStatistics();

        if (UserJobSpec_.has_iops_threshold() &&
            statistics.IORead > UserJobSpec_.iops_threshold() &&
            !IsWoodpecker_)
        {
            LOG_DEBUG("Woodpecker detected (IORead: %v, Threshold: %v)",
                statistics.IORead,
                UserJobSpec_.iops_threshold());
            IsWoodpecker_ = true;

            if (UserJobSpec_.has_iops_throttler_limit()) {
                BlockIO_.ThrottleOperations(UserJobSpec_.iops_throttler_limit());
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
        CleanupUserProcesses(error);
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
            LOG_WARNING(TError::FromSystem(), "Failed to blink input pipe");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(
    IJobHostPtr host,
    const TUserJobSpec& userJobSpec,
    const TJobId& jobId,
    std::unique_ptr<IUserJobIO> userJobIO)
{
    return New<TUserJob>(
        host,
        userJobSpec,
        jobId,
        std::move(userJobIO));
}

#else

IJobPtr CreateUserJob(
    IJobHostPtr host,
    const TUserJobSpec& UserJobSpec_,
    const TJobId& jobId,
    std::unique_ptr<IUserJobIO> userJobIO)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
