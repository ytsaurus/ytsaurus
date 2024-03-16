#include "user_job.h"

#include "asan_warning_filter.h"
#include "private.h"
#include "job_detail.h"
#include "stderr_writer.h"
#include "user_job_synchronizer_service.h"
#include "user_job_write_controller.h"
#include "memory_tracker.h"
#include "tmpfs_manager.h"
#include "environment.h"
#include "core_watcher.h"

#ifdef __linux__
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>
#endif

#include <yt/yt/server/job_proxy/public.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/supervisor_service_proxy.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/job_proxy/job_probe.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/server/lib/shell/shell_manager.h>

#include <yt/yt/server/lib/user_job/config.h>

#include <yt/yt/server/exec/user_job_synchronizer.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/signaler.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/file_client/file_chunk_output.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>
#include <yt/yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaful_reader_adapter.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/public.h>

#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/table_consumer.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/pattern_formatter.h>
#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/generic/guid.h>

#include <util/stream/null.h>
#include <util/stream/tee.h>

#include <util/system/compiler.h>
#include <util/system/execpath.h>
#include <util/system/fs.h>
#include <util/system/shellcommand.h>

namespace NYT::NJobProxy {

using namespace NApi;
using namespace NTools;
using namespace NYTree;
using namespace NYson;
using namespace NNet;
using namespace NTableClient;
using namespace NFormats;
using namespace NFS;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NShell;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NChunkClient::NProto;
using namespace NPipes;
using namespace NQueryClient;
using namespace NRpc;
using namespace NCoreDump;
using namespace NExecNode;
using namespace NYPath;
using namespace NJobProberClient;
using namespace NJobTrackerClient;
using namespace NUserJob;

using NControllerAgent::NProto::TJobResult;
using NControllerAgent::NProto::TJobSpec;
using NControllerAgent::NProto::TUserJobSpec;
using NControllerAgent::NProto::TCoreInfo;
using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

constexpr int JobStatisticsFD = 5;
constexpr int JobProfileFD = 8;
constexpr size_t BufferSize = 1_MB;

constexpr size_t MaxCustomStatisticsPathLength = 512;

constexpr int DefaultArtifactPermissions = 0666;
constexpr int ExecutableArtifactPermissions = 0777;

static TNullOutput NullOutput;

////////////////////////////////////////////////////////////////////////////////

static TString CreateNamedPipePath()
{
    const TString& name = CreateGuidAsString();
    return GetRealPath(CombinePaths("./pipes", name));
}

static TNamedPipePtr CreateNamedPipe()
{
    auto path = CreateNamedPipePath();
    return TNamedPipe::Create(path, DefaultArtifactPermissions);
}

////////////////////////////////////////////////////////////////////////////////

class TUserJob
    : public TJob
{
public:
    TUserJob(
        IJobHostPtr host,
        const TUserJobSpec& userJobSpec,
        TJobId jobId,
        const std::vector<int>& ports,
        std::unique_ptr<TUserJobWriteController> userJobWriteController)
        : TJob(host)
        , Logger(Host_->GetLogger())
        , JobId_(jobId)
        , UserJobWriteController_(std::move(userJobWriteController))
        , UserJobSpec_(userJobSpec)
        , Config_(Host_->GetConfig())
        , JobIOConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig())
        , UserJobEnvironment_(Host_->CreateUserJobEnvironment(
            TJobSpecEnvironmentOptions{
                .EnablePortoMemoryTracking = UserJobSpec_.use_porto_memory_tracking(),
                .EnableCoreDumps = UserJobSpec_.has_core_table_spec(),
                .EnableGpuCoreDumps = UserJobSpec_.enable_cuda_gpu_core_dump(),
                .EnablePorto = TranslateEnablePorto(CheckedEnumCast<NScheduler::EEnablePorto>(UserJobSpec_.enable_porto())),
                .ThreadLimit = UserJobSpec_.thread_limit()
            }))
        , Ports_(ports)
        , JobErrorPromise_(NewPromise<void>())
        , JobEnvironmentType_(ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment)->Type)
        , PipeIOPool_(CreateThreadPool(JobIOConfig_->PipeIOPoolSize, "PipeIO"))
        , AuxQueue_(New<TActionQueue>("JobAux"))
        , ReadStderrInvoker_(CreateSerializedInvoker(PipeIOPool_->GetInvoker(), "user_job"))
        , TmpfsManager_(New<TTmpfsManager>(Config_->TmpfsManager))
        , MemoryTracker_(New<TMemoryTracker>(Config_->MemoryTracker, UserJobEnvironment_, TmpfsManager_))
    {
        Host_->GetRpcServer()->RegisterService(CreateUserJobSynchronizerService(Logger, ExecutorPreparedPromise_, AuxQueue_->GetInvoker()));

        auto jobEnvironmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);

        UserJobReadController_ = CreateUserJobReadController(
            Host_->GetJobSpecHelper(),
            Host_->GetChunkReaderHost(),
            PipeIOPool_->GetInvoker(),
            BIND(&IJobHost::ReleaseNetwork, MakeWeak(Host_)),
            GetSandboxRelPath(ESandboxKind::Udf),
            ChunkReadOptions_,
            Host_->GetLocalHostName());

        InputPipeBlinker_ = New<TPeriodicExecutor>(
            AuxQueue_->GetInvoker(),
            BIND(&TUserJob::BlinkInputPipe, MakeWeak(this)),
            Config_->InputPipeBlinkerPeriod);

        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            AuxQueue_->GetInvoker(),
            BIND(&TUserJob::CheckMemoryUsage, MakeWeak(this)),
            jobEnvironmentConfig->MemoryWatchdogPeriod);

        ThrashingDetector_ = New<TPeriodicExecutor>(
            AuxQueue_->GetInvoker(),
            BIND(&TUserJob::CheckThrashing, MakeWeak(this)),
            jobEnvironmentConfig->JobThrashingDetector->CheckPeriod);

        // User job usually runs by per-slot users: yt_slot_{N}.
        // Which is not available for single-user, non-privileged or testing setup.
        if (!Config_->DoNotSetUserId && JobEnvironmentType_ == EJobEnvironmentType::Porto) {
            UserId_ = jobEnvironmentConfig->StartUid + Config_->SlotIndex;
        }

        if (!Config_->BusServer->UnixDomainSocketPath) {
            THROW_ERROR_EXCEPTION("Unix domain socket path is not configured");
        }

        BlockIOWatchdogExecutor_ = New<TPeriodicExecutor>(
            AuxQueue_->GetInvoker(),
            BIND(&TUserJob::CheckBlockIOUsage, MakeWeak(this)),
            UserJobEnvironment_->GetBlockIOWatchdogPeriod());

        if (UserJobSpec_.has_core_table_spec()) {
            const auto& coreTableSpec = UserJobSpec_.core_table_spec();

            auto tableWriterOptions = ConvertTo<TTableWriterOptionsPtr>(
                TYsonString(coreTableSpec.output_table_spec().table_writer_options()));
            tableWriterOptions->EnableValidationOptions();
            auto schemaId = FromProto<TMasterTableSchemaId>(coreTableSpec.output_table_spec().schema_id());
            auto chunkList = FromProto<TChunkListId>(coreTableSpec.output_table_spec().chunk_list_id());
            auto blobTableWriterConfig = ConvertTo<TBlobTableWriterConfigPtr>(TYsonString(coreTableSpec.blob_table_writer_config()));
            auto debugTransactionId = FromProto<TTransactionId>(UserJobSpec_.debug_transaction_id());

            CoreWatcher_ = New<TCoreWatcher>(
                Config_->CoreWatcher,
                GetRealPath("./cores"),
                Host_,
                AuxQueue_->GetInvoker(),
                blobTableWriterConfig,
                tableWriterOptions,
                debugTransactionId,
                chunkList,
                schemaId);
        }
    }

    TJobResult Run() override
    {
        YT_LOG_INFO("Starting job process");

        UserJobWriteController_->Init();

        Prepare();

        bool expected = false;
        if (Prepared_.compare_exchange_strong(expected, true)) {
            ProcessFinished_ = SpawnUserProcess();
            YT_LOG_INFO("Job process started");

            InitShellManager();

            if (BlockIOWatchdogExecutor_) {
                BlockIOWatchdogExecutor_->Start();
            }

            TDelayedExecutorCookie timeLimitCookie;
            if (UserJobSpec_.has_job_time_limit()) {
                auto timeLimit = FromProto<TDuration>(UserJobSpec_.job_time_limit());
                YT_LOG_INFO("Setting job time limit (Limit: %v)",
                    timeLimit);
                timeLimitCookie = TDelayedExecutor::Submit(
                    BIND(&TUserJob::OnJobTimeLimitExceeded, MakeWeak(this))
                        .Via(AuxQueue_->GetInvoker()),
                    timeLimit);
            }

            DoJobIO();

            TDelayedExecutor::Cancel(timeLimitCookie);
            WaitFor(InputPipeBlinker_->Stop())
                .ThrowOnError();

            if (!JobErrorPromise_.IsSet()) {
                FinalizeJobIO();
            }

            CleanupUserProcesses();

            if (BlockIOWatchdogExecutor_) {
                WaitFor(BlockIOWatchdogExecutor_->Stop())
                    .ThrowOnError();
            }
            WaitFor(MemoryWatchdogExecutor_->Stop())
                .ThrowOnError();
            WaitFor(ThrashingDetector_->Stop())
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
        auto* jobResultExt = result.MutableExtension(TJobResultExt::job_result_ext);
        jobResultExt->set_has_stderr(ErrorOutput_->GetCurrentSize() > 0);
        UserJobWriteController_->PopulateStderrResult(jobResultExt);

        if (jobResultError) {
            try {
                DumpFailContexts(jobResultExt);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to dump input context");
            }
        } else {
            UserJobWriteController_->PopulateResult(jobResultExt);
        }

        if (UserJobSpec_.has_core_table_spec()) {
            auto coreDumped = jobResultError && jobResultError->Attributes().Get("core_dumped", false /*defaultValue*/);
            std::optional<TDuration> finalizationTimeout;
            if (coreDumped) {
                finalizationTimeout = Config_->CoreWatcher->FinalizationTimeout;
                YT_LOG_INFO("Job seems to produce core dump, core watcher will wait for it (FinalizationTimeout: %v)",
                    finalizationTimeout);
            }
            auto coreResult = CoreWatcher_->Finalize(finalizationTimeout);

            YT_LOG_INFO("Core watcher finalized (CoreDumpCount: %v)",
                coreResult.CoreInfos.size());

            if (!coreResult.CoreInfos.empty()) {
                for (const auto& coreInfo : coreResult.CoreInfos) {
                    YT_LOG_INFO("Core file found (Pid: %v, ExecutableName: %v, Size: %v)",
                        coreInfo.process_id(),
                        coreInfo.executable_name(),
                        coreInfo.size());
                }
                if (UserJobSpec_.fail_job_on_core_dump()) {
                    innerErrors.push_back(TError(EErrorCode::UserJobProducedCoreFiles, "User job produced core files")
                        << TErrorAttribute("core_infos", coreResult.CoreInfos));
                }
            }

            CoreInfos_ = coreResult.CoreInfos;

            ToProto(jobResultExt->mutable_core_infos(), coreResult.CoreInfos);
            YT_VERIFY(coreResult.BoundaryKeys.empty() || coreResult.BoundaryKeys.sorted());
            ToProto(jobResultExt->mutable_core_result(), coreResult.BoundaryKeys);
        }

        if (ShellManager_) {
            WaitFor(BIND(&IShellManager::GracefulShutdown, ShellManager_, TError("Job completed"))
                .AsyncVia(Host_->GetControlInvoker())
                .Run())
                .ThrowOnError();
        }

        auto jobError = innerErrors.empty()
            ? TError()
            : TError(EErrorCode::UserJobFailed, "User job failed") << std::move(innerErrors);

        ToProto(result.mutable_error(), jobError);

        return result;
    }

    void Cleanup() override
    {
        if (Prepared_.exchange(false)) {
            // Job has been prepared.
            CleanupUserProcesses();
        }
    }

    void PrepareArtifacts() override
    {
        YT_LOG_INFO("Started preparing artifacts");

        // Prepare user artifacts.
        for (const auto& file : UserJobSpec_.files()) {
            if (!file.bypass_artifact_cache() && !file.copy_file()) {
                continue;
            }

            PrepareArtifact(
                file.file_name(),
                file.executable() ? ExecutableArtifactPermissions : DefaultArtifactPermissions);
        }

        // We need to give read access to sandbox directory to yt_node/yt_job_proxy effective user (usually yt:yt)
        // and to job user (e.g. yt_slot_N). Since they can have different groups, we fallback to giving read
        // access to everyone.
        // job proxy requires read access e.g. for getting tmpfs size.
        // Write access is for job user only, who becomes an owner.
        if (UserId_) {
            auto sandboxPath = CombinePaths(
                Host_->GetPreparationPath(),
                GetSandboxRelPath(ESandboxKind::User));

            auto config = New<TChownChmodConfig>();
            config->Permissions = 0755;
            config->Path = sandboxPath;
            config->UserId = static_cast<uid_t>(*UserId_);
            RunTool<TChownChmodTool>(config);
        }

        YT_LOG_INFO("Artifacts prepared");
    }

    void PrepareArtifact(
        const TString& artifactName,
        int permissions)
    {
        auto Logger = this->Logger
            .WithTag("ArtifactName: %v", artifactName);

        YT_LOG_INFO("Preparing artifact");

        auto sandboxPath = CombinePaths(
            Host_->GetPreparationPath(),
            GetSandboxRelPath(ESandboxKind::User));
        auto artifactPath = CombinePaths(sandboxPath, artifactName);

        auto onError = [&] (const TError& error) {
            Host_->OnArtifactPreparationFailed(artifactName, artifactPath, error);
        };

        try {
            auto pipePath = CreateNamedPipePath();
            auto pipe = TNamedPipe::Create(pipePath, /*permissions*/ 0755);

            auto pipeFd = HandleEintr(::open, pipePath.c_str(), O_RDONLY | O_NONBLOCK);
            TFile pipeFile(pipeFd);

            MakeDirRecursive(GetDirectoryName(artifactPath));

            TFile artifactFile(artifactPath, CreateAlways | WrOnly | Seq | CloseOnExec);
            artifactFile.Flock(LOCK_EX);

            Host_->PrepareArtifact(artifactName, pipePath);

            // Now pipe is opened and O_NONBLOCK is not required anymore.
            auto fcntlResult = HandleEintr(::fcntl, pipeFd, F_SETFL, O_RDONLY);
            if (fcntlResult < 0) {
                THROW_ERROR_EXCEPTION("Failed to disable O_RDONLY for artifact pipe")
                    << TError::FromSystem();
            }

            YT_LOG_INFO("Materializing artifact");

            constexpr ssize_t SpliceCopyBlockSize = 16_MB;
            Splice(pipeFile, artifactFile, SpliceCopyBlockSize);

            SetPermissions(artifactPath, permissions);

            YT_LOG_INFO("Artifact materialized with permissions (Permissions: %x)", permissions);
        } catch (const TSystemError& ex) {
            // For util functions.
            onError(TError::FromSystem(ex));
        } catch (const std::exception& ex) {
            onError(TError(ex));
        }
    }

    double GetProgress() const override
    {
        return UserJobReadController_->GetProgress();
    }

    i64 GetStderrSize() const override
    {
        if (!Prepared_) {
            return 0;
        }
        auto result = WaitFor(BIND([this, this_ = MakeStrong(this)] { return ErrorOutput_->GetCurrentSize(); })
            .AsyncVia(ReadStderrInvoker_)
            .Run());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error collecting job stderr size");
        return result.Value();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UserJobReadController_->GetFailedChunkIds();
    }

    TInterruptDescriptor GetInterruptDescriptor() const override
    {
        return UserJobReadController_->GetInterruptDescriptor();
    }

private:
    const NLogging::TLogger Logger;

    const TJobId JobId_;

    const std::unique_ptr<TUserJobWriteController> UserJobWriteController_;
    IUserJobReadControllerPtr UserJobReadController_;

    const TUserJobSpec& UserJobSpec_;

    const TJobProxyInternalConfigPtr Config_;
    const NScheduler::TJobIOConfigPtr JobIOConfig_;
    const IUserJobEnvironmentPtr UserJobEnvironment_;

    std::vector<int> Ports_;

    TPromise<void> JobErrorPromise_;

    const EJobEnvironmentType JobEnvironmentType_;

    const IThreadPoolPtr PipeIOPool_;
    const TActionQueuePtr AuxQueue_;
    const IInvokerPtr ReadStderrInvoker_;

    TString InputPipePath_;

    std::optional<int> UserId_;

    std::atomic<bool> Prepared_ = false;
    std::atomic<bool> Woodpecker_ = false;
    std::atomic<bool> JobStarted_ = false;
    std::atomic<bool> InterruptionSignalSent_ = false;

    const TTmpfsManagerPtr TmpfsManager_;

    const TMemoryTrackerPtr MemoryTracker_;

    std::vector<std::unique_ptr<IOutputStream>> TableOutputs_;

    // Writes stderr data to Cypress file.
    std::unique_ptr<TStderrWriter> ErrorOutput_;

    // Core infos.
    TCoreInfos CoreInfos_;

    // StderrCombined_ is set only if stderr table is specified.
    // It redirects data to both ErrorOutput_ and stderr table writer.
    std::unique_ptr<TTeeOutput> StderrCombined_;

    IShellManagerPtr ShellManager_;

    std::vector<TNamedPipeConfigPtr> PipeConfigs_;

#ifdef _asan_enabled_
    std::unique_ptr<TAsanWarningFilter> AsanWarningFilter_;
#endif

    std::unique_ptr<TTableOutput> StatisticsOutput_;
    std::unique_ptr<IYsonConsumer> StatisticsConsumer_;

    std::vector<IConnectionReaderPtr> TablePipeReaders_;
    std::vector<IConnectionWriterPtr> TablePipeWriters_;
    IConnectionReaderPtr StatisticsPipeReader_;
    IConnectionReaderPtr StderrPipeReader_;
    IConnectionReaderPtr ProfilePipeReader_;

    std::vector<ISchemalessFormatWriterPtr> FormatWriters_;

    // Actually InputActions_ has only one element,
    // but use vector to reuse runAction code
    std::vector<TCallback<void()>> InputActions_;
    std::vector<TCallback<void()>> OutputActions_;
    std::vector<TCallback<void()>> StderrActions_;
    std::vector<TCallback<void()>> FinalizeActions_;

    TFuture<void> ProcessFinished_;
    std::vector<TString> Environment_;

    std::optional<TExecutorInfo> ExecutorInfo_;

    TPeriodicExecutorPtr MemoryWatchdogExecutor_;
    TPeriodicExecutorPtr BlockIOWatchdogExecutor_;
    TPeriodicExecutorPtr InputPipeBlinker_;
    TPeriodicExecutorPtr ThrashingDetector_;

    TPromise<TExecutorInfo> ExecutorPreparedPromise_ = NewPromise<TExecutorInfo>();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StatisticsLock_);
    NYT::TStatistics CustomStatistics_;

    TCoreWatcherPtr CoreWatcher_;

    std::optional<TString> FailContext_;

    std::atomic<bool> NotFullyConsumed_ = false;

    i64 LastMajorPageFaultCount_ = 0;
    i64 PageFaultLimitOverflowCount_ = 0;

    TFuture<void> SpawnUserProcess()
    {
        WaitFor(Host_->GetUserJobContainerCreationThrottler()->Throttle(1))
            .ThrowOnError();

        return UserJobEnvironment_->SpawnUserProcess(
            ExecProgramName,
            {"--config", Host_->AdjustPath(GetExecutorConfigPath())},
            CombinePaths(Host_->GetSlotPath(), GetSandboxRelPath(ESandboxKind::User)));
    }

    void InitShellManager()
    {
        if (JobEnvironmentType_ == EJobEnvironmentType::Porto) {
#ifdef _linux_
            auto portoJobEnvironmentConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(Config_->JobEnvironment);
            auto portoExecutor = NContainers::CreatePortoExecutor(portoJobEnvironmentConfig->PortoExecutor, "job-shell");

            std::vector<TString> shellEnvironment;
            shellEnvironment.reserve(Environment_.size());
            std::vector<TString> visibleEnvironment;
            visibleEnvironment.reserve(Environment_.size());

            for (const auto& variable : Environment_) {
                if (variable.StartsWith(NControllerAgent::SecureVaultEnvPrefix) &&
                    !UserJobSpec_.enable_secure_vault_variables_in_job_shell())
                {
                    continue;
                }
                if (variable.StartsWith("YT_")) {
                    shellEnvironment.push_back(variable);
                }
                visibleEnvironment.push_back(variable);
            }

            auto shellManagerUid = UserId_;
            if (Config_->TestPollJobShell) {
                shellManagerUid = std::nullopt;
                shellEnvironment.push_back("PS1=\"test_job@shell:\\W$ \"");
            }

            std::optional<int> shellManagerGid;
            // YT-13790.
            if (Config_->RootPath) {
                shellManagerGid = 1001;
            }

            // ToDo(psushin): move ShellManager into user job environment.
            TShellManagerConfig config{
                .PreparationDir = Host_->GetPreparationPath(),
                .WorkingDir = Host_->GetSlotPath(),
                .UserId = shellManagerUid,
                .GroupId = shellManagerGid,
                .MessageOfTheDay = Format("Job environment:\n%v\n", JoinToString(visibleEnvironment, TStringBuf("\n"))),
                .Environment = std::move(shellEnvironment),
                .EnableJobShellSeccopm = Config_->EnableJobShellSeccopm,
            };

            ShellManager_ = CreateShellManager(
                config,
                portoExecutor,
                UserJobEnvironment_->GetUserJobInstance());
#endif
        }
    }

    void Prepare()
    {
        PreparePipes();
        PrepareEnvironment();
        PrepareExecutorConfig();
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
        options->Account = UserJobSpec_.has_debug_artifacts_account()
            ? UserJobSpec_.debug_artifacts_account()
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

    // COMPAT(ignat)
    void SaveErrorChunkId(TJobResultExt* jobResultExt)
    {
        if (!ErrorOutput_) {
            return;
        }

        auto errorChunkId = ErrorOutput_->GetChunkId();
        if (errorChunkId) {
            ToProto(jobResultExt->mutable_stderr_chunk_id(), errorChunkId);
            YT_LOG_INFO("Stderr chunk generated (ChunkId: %v)", errorChunkId);
        }
    }

    void DumpFailContexts(TJobResultExt* jobResultExt)
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

        jobResultExt->set_has_fail_context(size > 0);
    }

    std::vector<TChunkId> DumpInputContext(TTransactionId transactionId) override
    {
        ValidatePrepared();

        auto result = WaitFor(UserJobReadController_->GetInputContext());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error collecting job input context");
        const auto& contexts = result.Value();

        auto chunks = DoDumpInputContext(
            contexts,
            // COMPAT(coteeq)
            transactionId ? transactionId : FromProto<TTransactionId>(UserJobSpec_.input_transaction_id()));
        YT_VERIFY(chunks.size() == 1);

        if (chunks.front() == NullChunkId) {
            THROW_ERROR_EXCEPTION("Cannot dump job context: reading has not started yet");
        }

        return chunks;
    }

    std::vector<TChunkId> DoDumpInputContext(const std::vector<TBlob>& contexts, TTransactionId transactionId)
    {
        std::vector<TChunkId> result;

        for (int index = 0; index < std::ssize(contexts); ++index) {
            // NB. We use empty data sink here, so the details like object path and account are not present in IO tags.
            // That's because this code is legacy anyway and not worth covering with IO tags.
            TFileChunkOutput contextOutput(
                JobIOConfig_->ErrorFileWriter,
                CreateFileOptions(),
                Host_->GetClient(),
                transactionId,
                NChunkClient::TDataSink(),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());

            const auto& context = contexts[index];
            contextOutput.Write(context.Begin(), context.Size());
            contextOutput.Finish();

            auto contextChunkId = contextOutput.GetChunkId();
            YT_LOG_INFO("Input context chunk generated (ChunkId: %v, InputIndex: %v)",
                contextChunkId,
                index);

            result.push_back(contextChunkId);
        }

        return result;
    }

    std::optional<TString> GetFailContext() override
    {
        ValidatePrepared();

        return FailContext_;
    }

    TString GetStderr() override
    {
        ValidatePrepared();

        auto result = WaitFor(BIND([this, this_ = MakeStrong(this)] { return ErrorOutput_->GetCurrentData(); })
            .AsyncVia(ReadStderrInvoker_)
            .Run());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error collecting job stderr");
        return result.Value();
    }

    const TCoreInfos& GetCoreInfos() const override
    {
        return CoreInfos_;
    }

    TPollJobShellResponse PollJobShell(
        const TJobShellDescriptor& jobShellDescriptor,
        const TYsonString& parameters) override
    {
        ValidatePrepared();

        if (!ShellManager_) {
            THROW_ERROR_EXCEPTION("Job shell polling is not supported in non-Porto environment");
        }
        auto response = ShellManager_->PollJobShell(jobShellDescriptor, parameters);
        if (response.LoggingContext) {
            response.LoggingContext = BuildYsonStringFluently<EYsonType::MapFragment>(EYsonFormat::Text)
                .Item("job_id").Value(Host_->GetJobId())
                .Item("operation_id").Value(Host_->GetOperationId())
                .Do([&] (TFluentMap fluent) {
                    fluent.GetConsumer()->OnRaw(response.LoggingContext);
                })
                .Finish();
        }
        return response;
    }

    std::vector<pid_t> GetPidsForInterrupt() const
    {
        std::vector<pid_t> pids;
        if (UserJobSpec_.signal_root_process_only()) {
            if (ExecutorInfo_) {
                auto processPid = ExecutorInfo_->ProcessPid;
                if (UserJobEnvironment_->IsPidNamespaceIsolationEnabled()) {
                    if (auto pid = GetPidByChildNamespacePid(processPid)) {
                        pids.push_back(*pid);
                    }
                } else {
                    pids.push_back(processPid);
                }
            }
            if (pids.empty()) {
                if (auto pid = UserJobEnvironment_->GetJobRootPid()) {
                    pids.push_back(*pid);
                }
            }
        } else {
            pids = UserJobEnvironment_->GetJobPids();
        }
        return pids;
    }

    void Interrupt() override
    {
        ValidatePrepared();

        if (!InterruptionSignalSent_.exchange(true) && UserJobSpec_.has_interruption_signal()) {
            auto signal = UserJobSpec_.interruption_signal();
            try {
                if (UserJobEnvironment_->IsPidNamespaceIsolationEnabled() && UserJobSpec_.signal_root_process_only() && Config_->UsePortoKillForSignalling) {
#ifdef _linux_
                    if (auto signalNumber = FindSignalIdBySignalName(signal)) {
                        UserJobEnvironment_->GetUserJobInstance()->Kill(*signalNumber);
                    } else {
                        THROW_ERROR_EXCEPTION("Unknown signal name")
                            << TErrorAttribute("signal_name", signal);
                    }
#else
                    THROW_ERROR_EXCEPTION("Signalling by Porto is not supported at non-linux environment");
#endif
                } else {
                    auto pids = GetPidsForInterrupt();

                    YT_LOG_INFO("Sending interrupt signal to user job (SignalName: %v, UserJobPids: %v)",
                        signal,
                        pids);

                    auto signalerConfig = New<TSignalerConfig>();
                    signalerConfig->Pids = pids;
                    signalerConfig->SignalName = signal;
                    RunTool<TSignalerTool>(signalerConfig);

                    YT_LOG_INFO("Interrupt signal successfully sent");
                }
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to send interrupt signal to user job");
            }
        }

        UserJobReadController_->InterruptReader();
    }

    void Fail() override
    {
        YT_LOG_DEBUG("User job failed");
        auto error = TError("Job failed by external request");
        JobErrorPromise_.TrySet(error);
        CleanupUserProcesses();
    }

    void GracefulAbort(TError error) override
    {
        YT_LOG_DEBUG("User job gracefully aborted (Error: %v)", error);
        YT_VERIFY(error.GetCode() == NExecNode::EErrorCode::AbortByControllerAgent);
        JobErrorPromise_.TrySet(std::move(error));
        CleanupUserProcesses();
    }

    void ValidatePrepared()
    {
        if (!Prepared_) {
            THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot operate on job: job has not been prepared yet");
        }
    }

    void PrepareOutputTablePipes()
    {
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.output_format()));
        auto typeConversionConfig = ConvertTo<TTypeConversionConfigPtr>(format.Attributes());
        auto valueConsumers = UserJobWriteController_->CreateValueConsumers(typeConversionConfig);
        auto parsers = CreateParsersForFormat(format, valueConsumers);

        auto outputStreamCount = UserJobWriteController_->GetOutputStreamCount();
        TableOutputs_.reserve(outputStreamCount);

        for (int i = 0; i < outputStreamCount; ++i) {
            TableOutputs_.emplace_back(std::make_unique<TTableOutput>(std::move(parsers[i])));

            int jobDescriptor = UserJobSpec_.use_yamr_descriptors()
                ? 3 + i
                : 3 * i + GetJobFirstOutputTableFDFromSpec(UserJobSpec_);

            // In case of YAMR jobs dup 1 and 3 fd for YAMR compatibility
            auto wrappingError = TError("Error writing to output table %v", i);

            auto reader = (UserJobSpec_.use_yamr_descriptors() && jobDescriptor == 3)
                ? PrepareOutputPipeReader(CreateNamedPipe(), {1, jobDescriptor}, TableOutputs_[i].get(), &OutputActions_, wrappingError)
                : PrepareOutputPipeReader(CreateNamedPipe(), {jobDescriptor}, TableOutputs_[i].get(), &OutputActions_, wrappingError);
            TablePipeReaders_.push_back(reader);
        }

        FinalizeActions_.push_back(BIND([this, this_ = MakeStrong(this)] {
            auto checkErrors = [&] (const std::vector<TFuture<void>>& asyncErrors) {
                auto error = WaitFor(AllSucceeded(asyncErrors));
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing table output");
            };

            std::vector<TFuture<void>> flushResults;
            for (const auto& valueConsumer : UserJobWriteController_->GetAllValueConsumers()) {
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

    IConnectionReaderPtr PrepareOutputPipeReader(
        TNamedPipePtr pipe,
        const std::vector<int>& jobDescriptors,
        IOutputStream* output,
        std::vector<TCallback<void()>>* actions,
        const TError& wrappingError)
    {
        for (auto jobDescriptor : jobDescriptors) {
            // Since inside job container we see another rootfs, we must adjust pipe path.
            auto pipeConfig = TNamedPipeConfig::Create(Host_->AdjustPath(pipe->GetPath()), jobDescriptor, true);
            PipeConfigs_.emplace_back(std::move(pipeConfig));
        }

        auto asyncInput = pipe->CreateAsyncReader();

        actions->push_back(BIND([=, this, this_ = MakeStrong(this)] {
            try {
                PipeInputToOutput(asyncInput, output, BufferSize);
                YT_LOG_INFO("Data successfully read from pipe (PipePath: %v)", pipe->GetPath());
            } catch (const std::exception& ex) {
                auto error = wrappingError
                    << ex;
                YT_LOG_ERROR(error);

                // We abort asyncInput for stderr.
                // Almost all readers are aborted in `OnIOErrorOrFinished', but stderr doesn't,
                // because we want to read and save as much stderr as possible even if job is failing.
                // But if stderr transferring fiber itself fails, child process may hang
                // if it wants to write more stderr. So we abort input (and therefore close the pipe) here.
                if (asyncInput == StderrPipeReader_) {
                    YT_UNUSED_FUTURE(asyncInput->Abort());
                }

                THROW_ERROR error;
            }
        }));

        return asyncInput;
    }

    void PrepareInputTablePipe()
    {
        YT_LOG_DEBUG(
            "Creating input table pipe (Path: %v, Permission: %v, CustomCapacity: %v)",
            InputPipePath_,
            DefaultArtifactPermissions,
            JobIOConfig_->PipeCapacity);

        int jobDescriptor = 0;
        InputPipePath_= CreateNamedPipePath();
        auto pipe = TNamedPipe::Create(InputPipePath_, DefaultArtifactPermissions, JobIOConfig_->PipeCapacity);
        auto pipeConfig = TNamedPipeConfig::Create(Host_->AdjustPath(pipe->GetPath()), jobDescriptor, false);
        PipeConfigs_.emplace_back(std::move(pipeConfig));
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.input_format()));

        auto reader = pipe->CreateAsyncReader();
        auto asyncOutput = pipe->CreateAsyncWriter();

        TablePipeWriters_.push_back(asyncOutput);

        //! NB: Context saving effectively forces writer to ignore pipe capacity limit
        //! as it only ever flushes once the socket is closed.
        auto transferInput = UserJobReadController_->PrepareJobInputTransfer(
            asyncOutput,
            /*enableContextSaving*/ !JobIOConfig_->PipeCapacity.has_value());
        InputActions_.push_back(BIND([=] {
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

        FinalizeActions_.push_back(BIND([=, this, this_ = MakeStrong(this)] {
            bool throwOnFailure = UserJobSpec_.check_input_fully_consumed();

            try {
                auto buffer = TSharedMutableRef::Allocate(1, {.InitializeStorage = false});
                auto future = reader->Read(buffer);
                auto result = WaitFor(future);
                if (!result.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to check input stream after user process")
                        << TErrorAttribute("fd", jobDescriptor)
                        << result;
                }
                // Try to read some data from the pipe.
                if (result.Value() > 0) {
                    THROW_ERROR_EXCEPTION("Input stream was not fully consumed by user process")
                        << TErrorAttribute("fd", jobDescriptor);
                }
            } catch (const std::exception& ex) {
                YT_UNUSED_FUTURE(reader->Abort());
                NotFullyConsumed_.store(true);
                if (throwOnFailure) {
                    throw;
                }
            }
        }));
    }

    void PreparePipes()
    {
        YT_LOG_INFO("Initializing pipes");

        // We use the following convention for designating input and output file descriptors
        // in job processes:
        //
        // The first output table file descriptor is
        // defined by the environment variable YT_FIRST_TABLE_OUTPUT_FD.
        // The default value of YT_FIRST_TABLE_OUTPUT_FD is 1 to
        // conform to the convention prior to the addition of this variable.
        //
        // fd = 0 for all table inputs
        // fd = 3 * (N - 1) + $YT_FIRST_TABLE_OUTPUT_FD for the N-th output table (if exists)
        // fd == 2 for the error stream
        // fd == 5 for statistics
        // fd == 8 for job profiles
        // e. g. for YT_FIRST_TABLE_OUTPUT_TABLE_FD = 1
        // 0 - all input tables
        // 1 - first output table
        // 2 - error stream
        // 3 - not used
        // 4 - second output table

        // Configure stderr pipe.

        std::vector<int> errorOutputDescriptors = {STDERR_FILENO};
        // Redirect stdout to stderr to allow writing to stdout.
        if (UserJobSpec_.redirect_stdout_to_stderr()) {
            errorOutputDescriptors.push_back(STDOUT_FILENO);
        }

        StderrPipeReader_ = PrepareOutputPipeReader(
            CreateNamedPipe(),
            errorOutputDescriptors,
            CreateErrorOutput(),
            &StderrActions_,
            TError("Error writing to stderr"));

        PrepareOutputTablePipes();

        if (!UserJobSpec_.use_yamr_descriptors()) {
            StatisticsPipeReader_ = PrepareOutputPipeReader(
                CreateNamedPipe(),
                {JobStatisticsFD},
                CreateStatisticsOutput(),
                &OutputActions_,
                TError("Error writing custom job statistics"));

            if (auto* profileOutput = JobProfiler_->GetUserJobProfileOutput()) {
                auto pipe = CreateNamedPipe();

                auto typeStr = FormatEnum(JobProfiler_->GetUserJobProfilerSpec()->Type);
                Environment_.push_back(Format("YT_%v_PROFILER_PATH=%v", to_upper(typeStr), Host_->AdjustPath(pipe->GetPath())));

                ProfilePipeReader_ = PrepareOutputPipeReader(
                    std::move(pipe),
                    {JobProfileFD},
                    profileOutput,
                    &StderrActions_,
                    TError("Error writing job profile"));
            }
        }

        PrepareInputTablePipe();

        YT_LOG_INFO("Pipes initialized");
    }

    void PrepareEnvironment()
    {
        TPatternFormatter formatter;
        formatter.AddProperty(
            "SandboxPath",
            CombinePaths(Host_->GetSlotPath(), GetSandboxRelPath(ESandboxKind::User)));

        if (UserJobSpec_.has_network_project_id()) {
            Environment_.push_back(Format("YT_NETWORK_PROJECT_ID=%v", UserJobSpec_.network_project_id()));
        }

        Environment_.push_back(Format("YT_JOB_PROXY_SOCKET_PATH=%v", Host_->GetJobProxyUnixDomainSocketPath()));

        for (int i = 0; i < UserJobSpec_.environment_size(); ++i) {
            Environment_.emplace_back(formatter.Format(UserJobSpec_.environment(i)));
        }

        if (Config_->TestRootFS && Config_->RootPath) {
            Environment_.push_back(Format("YT_ROOT_FS=%v", *Config_->RootPath));
        }

        for (int index = 0; index < std::ssize(Ports_); ++index) {
            Environment_.push_back(Format("YT_PORT_%v=%v", index, Ports_[index]));
        }

        if (auto jobProfilerSpec = JobProfiler_->GetUserJobProfilerSpec()) {
            auto spec = ConvertToYsonString(jobProfilerSpec, EYsonFormat::Text);
            Environment_.push_back(Format("YT_JOB_PROFILER_SPEC=%v", spec));

            YT_LOG_INFO("User job profiler is enabled (Spec: %v)", spec);
        }

        if (!UserJobSpec_.use_yamr_descriptors()) {
            int jobFirstOutputTableFD = GetJobFirstOutputTableFDFromSpec(UserJobSpec_);
            Environment_.push_back(Format("YT_FIRST_OUTPUT_TABLE_FD=%v", jobFirstOutputTableFD));
        }

        const auto& environment = UserJobEnvironment_->GetEnvironmentVariables();
        Environment_.insert(Environment_.end(), environment.begin(), environment.end());
    }

    void AddCustomStatistics(const INodePtr& sample)
    {
        auto guard = Guard(StatisticsLock_);
        CustomStatistics_.AddSample("/custom", sample);

        int customStatisticsCount = 0;
        for (const auto& [path, summary] : CustomStatistics_.Data()) {
            if (HasPrefix(path, "/custom")) {
                if (path.size() > MaxCustomStatisticsPathLength) {
                    THROW_ERROR_EXCEPTION(
                        "Custom statistics path is too long: %v > %v",
                        path.size(),
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

    std::optional<TJobEnvironmentCpuStatistics> GetUserJobCpuStatistics() const override
    {
        try {
            auto statistic = UserJobEnvironment_->GetCpuStatistics()
                .ValueOrThrow();
            return statistic;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Unable to get CPU statistics for user job");
            return std::nullopt;
        }
    }

    IJob::TStatistics GetStatistics() const override
    {
        IJob::TStatistics result;

        auto& statistics = result.Statstics;
        {
            auto guard = Guard(StatisticsLock_);
            statistics = CustomStatistics_;
        }

        if (const auto& dataStatistics = UserJobReadController_->GetDataStatistics()) {
            result.TotalInputStatistics.DataStatistics = *dataStatistics;
        }

        statistics.AddSample("/data/input/not_fully_consumed", NotFullyConsumed_.load() ? 1 : 0);

        if (const auto& codecStatistics = UserJobReadController_->GetDecompressionStatistics()) {
            result.TotalInputStatistics.CodecStatistics = *codecStatistics;
        }

        if (const auto& timingStatistics = UserJobReadController_->GetTimingStatistics()) {
            result.TimingStatistics = *timingStatistics;
        }

        result.ChunkReaderStatistics = ChunkReadOptions_.ChunkReaderStatistics;

        auto writers = UserJobWriteController_->GetWriters();
        for (const auto& writer : writers) {
            result.OutputStatistics.emplace_back() = {
                .DataStatistics = writer->GetDataStatistics(),
                .CodecStatistics = writer->GetCompressionStatistics(),
            };
        }

        // Job environment statistics.
        if (Prepared_) {
            if (auto userJobCpuStatistics = GetUserJobCpuStatistics()) {
                statistics.AddSample("/user_job/cpu", *userJobCpuStatistics);
            }

            try {
                auto blockIOStatistics = UserJobEnvironment_->GetBlockIOStatistics()
                    .ValueOrThrow();
                statistics.AddSample("/user_job/block_io", blockIOStatistics);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Unable to get block io statistics for user job");
            }

            try {
                auto networkStatistics = UserJobEnvironment_->GetNetworkStatistics()
                    .ValueOrThrow();
                if (networkStatistics) {
                    statistics.AddSample("/user_job/network", *networkStatistics);
                }
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Unable to get network statistics for user job");
            }

            statistics.AddSample("/user_job/woodpecker", Woodpecker_ ? 1 : 0);
        }

        try {
            TmpfsManager_->DumpTmpfsStatistics(&statistics, "/user_job");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to dump user job tmpfs statistics");
        }

        try {
            MemoryTracker_->DumpMemoryUsageStatistics(&statistics, "/user_job");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to dump user job memory usage statistics");
        }

        YT_VERIFY(UserJobSpec_.memory_limit() > 0);
        statistics.AddSample("/user_job/memory_limit", UserJobSpec_.memory_limit());
        statistics.AddSample("/user_job/memory_reserve", UserJobSpec_.memory_reserve());

        statistics.AddSample(
            "/user_job/memory_reserve_factor_x10000",
            static_cast<int>((1e4 * UserJobSpec_.memory_reserve()) / UserJobSpec_.memory_limit()));

        // Pipe statistics.
        if (Prepared_) {
            IJob::TStatistics::TMultiPipeStatistics pipeStatistics;

            pipeStatistics.InputPipeStatistics = {
                .ConnectionStatistics = TablePipeWriters_[0]->GetWriteStatistics(),
                .Bytes = TablePipeWriters_[0]->GetWriteByteCount(),
            };

            for (int i = 0; i < std::ssize(TablePipeReaders_); ++i) {
                const auto& tablePipeReader = TablePipeReaders_[i];

                auto& outputPipeStatistics = pipeStatistics.OutputPipeStatistics.emplace_back();
                outputPipeStatistics = {
                    .ConnectionStatistics = tablePipeReader->GetReadStatistics(),
                    .Bytes = tablePipeReader->GetReadByteCount(),
                };

                pipeStatistics.TotalOutputPipeStatistics.ConnectionStatistics.IdleDuration += outputPipeStatistics.ConnectionStatistics.IdleDuration;
                pipeStatistics.TotalOutputPipeStatistics.ConnectionStatistics.BusyDuration += outputPipeStatistics.ConnectionStatistics.BusyDuration;
                pipeStatistics.TotalOutputPipeStatistics.Bytes += outputPipeStatistics.Bytes;
            }

            result.PipeStatistics = pipeStatistics;
        }
        return result;
    }


    void OnIOErrorOrFinished(const TError& error, const TString& message)
    {
        if (error.IsOK() || error.FindMatching(NNet::EErrorCode::Aborted)) {
            return;
        }

        if (!JobErrorPromise_.TrySet(error)) {
            return;
        }

        YT_LOG_ERROR(TError(message) << error);

        CleanupUserProcesses();

        for (const auto& reader : TablePipeReaders_) {
            YT_UNUSED_FUTURE(reader->Abort());
        }

        for (const auto& writer : TablePipeWriters_) {
            YT_UNUSED_FUTURE(writer->Abort());
        }

        if (StatisticsPipeReader_) {
            YT_UNUSED_FUTURE(StatisticsPipeReader_->Abort());
        }

        if (!JobStarted_) {
            // If start action didn't finish successfully, stderr could have stayed closed,
            // and output action may hang.
            // But if job is started we want to save as much stderr as possible
            // so we don't close stderr in that case.
            YT_UNUSED_FUTURE(StderrPipeReader_->Abort());

            if (ProfilePipeReader_) {
                YT_UNUSED_FUTURE(ProfilePipeReader_->Abort());
            }
        }
    }

    TString GetExecutorConfigPath() const
    {
        const static TString ExecutorConfigFileName = "executor_config.yson";

        return CombinePaths(NFs::CurrentWorkingDirectory(), ExecutorConfigFileName);
    }

    void PrepareExecutorConfig()
    {
        auto executorConfig = New<TUserJobExecutorConfig>();

        executorConfig->Command = UserJobSpec_.shell_command();
        executorConfig->JobId = ToString(JobId_);

        if (UserJobSpec_.has_core_table_spec() || UserJobSpec_.force_core_dump()) {
#ifdef _asan_enabled_
            YT_LOG_WARNING("Core dumps are not allowed in ASAN build");
#else
            executorConfig->EnableCoreDump = true;
#endif
        }

        const auto& executorStderrPath = Host_->GetConfig()->ExecutorStderrPath;
        if (executorStderrPath) {
            executorConfig->StderrPath = *executorStderrPath;
        }

        if (UserId_) {
            executorConfig->Uid = *UserId_;
        }

        executorConfig->Pipes = PipeConfigs_;
        executorConfig->Environment = Environment_;

        {
            auto connectionConfig = New<TUserJobSynchronizerConnectionConfig>();
            auto processWorkingDirectory = CombinePaths(Host_->GetPreparationPath(), GetSandboxRelPath(ESandboxKind::User));
            connectionConfig->BusClientConfig->UnixDomainSocketPath = GetRelativePath(processWorkingDirectory, *Config_->BusServer->UnixDomainSocketPath);
            executorConfig->UserJobSynchronizerConnectionConfig = connectionConfig;
        }

        auto executorConfigPath = GetExecutorConfigPath();
        try {
            TFile configFile(executorConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
            TUnbufferedFileOutput output(configFile);
            NYson::TYsonWriter writer(&output, EYsonFormat::Pretty);
            Serialize(executorConfig, &writer);
            writer.Flush();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to write executor config into %v", executorConfigPath)
                << ex;
        }
    }

    void DoJobIO()
    {
        auto onIOError = BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            OnIOErrorOrFinished(error, "Job input/output error, aborting");
        });

        auto onStartIOError = BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            OnIOErrorOrFinished(error, "Executor input/output error, aborting");
        });

        auto onProcessFinished = BIND([=, this, this_ = MakeStrong(this)] (const TError& userJobError) {
            YT_LOG_INFO("Process finished (UserJobError: %v)", userJobError);

            OnIOErrorOrFinished(userJobError, "Job control process has finished, aborting");

            // If process has crashed before sending notification we stuck
            // on waiting executor promise, so set it here.
            // Do this after JobProxyError is set (if necessary).
            ExecutorPreparedPromise_.TrySet(TExecutorInfo{});
        });

        auto runActions = [&] (
            const std::vector<TCallback<void()>>& actions,
            const NYT::TCallback<void(const TError&)>& onError,
            IInvokerPtr invoker)
        {
            std::vector<TFuture<void>> result;
            for (const auto& action : actions) {
                auto asyncError = BIND(action)
                    .AsyncVia(invoker)
                    .Run();
                result.emplace_back(asyncError.Apply(onError));
            }
            return result;
        };

        auto processFinished = ProcessFinished_.Apply(onProcessFinished);

        // Wait until executor opens and dup named pipes.
        YT_LOG_INFO("Waiting for signal from executor");
        ExecutorInfo_ = WaitFor(ExecutorPreparedPromise_.ToFuture())
            .ValueOrThrow();

        MemoryWatchdogExecutor_->Start();
        ThrashingDetector_->Start();

        if (!JobErrorPromise_.IsSet()) {
            Host_->OnPrepared();
            // Now writing pipe is definitely ready, so we can start blinking.
            InputPipeBlinker_->Start();
            JobStarted_ = true;
        } else if (!ExecutorInfo_ || ExecutorInfo_->ProcessPid == 0) {
            // Actually, ExecutorInfo_ must be non-null at this point, since it is
            // explicitly set a few lines before. We still keep the condition as a
            // defensive measure from possible future code changes.
            YT_LOG_ERROR(JobErrorPromise_.Get(), "Failed to prepare executor");
            return;
        }
        YT_LOG_INFO("Start actions finished (UserProcessPid: %v)", ExecutorInfo_->ProcessPid);
        auto inputFutures = runActions(InputActions_, onIOError, PipeIOPool_->GetInvoker());
        auto outputFutures = runActions(OutputActions_, onIOError, PipeIOPool_->GetInvoker());
        auto stderrFutures = runActions(StderrActions_, onIOError, ReadStderrInvoker_);

        // First, wait for all job output pipes.
        // If job successfully completes or dies prematurely, they close automatically.
        WaitFor(AllSet(outputFutures))
            .ThrowOnError();
        YT_LOG_INFO("Output actions finished");

        WaitFor(AllSet(stderrFutures))
            .ThrowOnError();
        YT_LOG_INFO("Error actions finished");

        // Then, wait for job process to finish.
        // Theoretically, process could have explicitly closed its output pipes
        // but still be doing some computations.
        YT_VERIFY(WaitFor(processFinished).IsOK());
        YT_LOG_INFO("Job process finished (Error: %v)", JobErrorPromise_.ToFuture().TryGet());

        // Abort input pipes unconditionally.
        // If the job didn't read input to the end, pipe writer could be blocked,
        // because we didn't close the reader end (see check_input_fully_consumed).
        for (const auto& writer : TablePipeWriters_) {
            YT_UNUSED_FUTURE(writer->Abort());
        }

        // Now make sure that input pipes are also completed.
        WaitFor(AllSet(inputFutures))
            .ThrowOnError();
        YT_LOG_INFO("Input actions finished");
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

    void CheckMemoryUsage()
    {
        i64 memoryUsage;
        try {
            memoryUsage = MemoryTracker_->GetMemoryUsage();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get user job memory usage");
            return;
        }

        auto memoryLimit = UserJobSpec_.memory_limit();
        YT_LOG_DEBUG("Checking memory usage (MemoryUsage: %v, MemoryLimit: %v)",
            memoryUsage,
            memoryLimit);

        if (memoryUsage > memoryLimit && Config_->CheckUserJobMemoryLimit) {
            auto memoryStatistics = MemoryTracker_->GetMemoryStatistics();

            auto processesStatistics = memoryStatistics->ProcessesStatistics;
            std::sort(
                processesStatistics.begin(),
                processesStatistics.end(),
                [] (const TProcessMemoryStatisticsPtr& lhs, const TProcessMemoryStatisticsPtr& rhs) {
                    return lhs->Rss > rhs->Rss;
                });

            YT_LOG_INFO("Memory limit exceeded");
            auto error = TError(
                NJobProxy::EErrorCode::MemoryLimitExceeded,
                "Memory limit exceeded")
                << TErrorAttribute("usage", memoryUsage)
                << TErrorAttribute("limit", memoryLimit)
                << TErrorAttribute("tmpfs_usage", memoryStatistics->TmpfsUsage)
                << TErrorAttribute("processes", processesStatistics);
            JobErrorPromise_.TrySet(error);
            CleanupUserProcesses();
        }

        Host_->SetUserJobMemoryUsage(memoryUsage);
    }

    void CheckThrashing()
    {
        i64 currentFaultCount;
        try {
            currentFaultCount = UserJobEnvironment_->GetMajorPageFaultCount();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(
                ex,
                "Error getting information about major page faults in user job container");
            return;
        }

        if (currentFaultCount != LastMajorPageFaultCount_) {
            HandleMajorPageFaultCountIncrease(currentFaultCount);
        }
    }

    void HandleMajorPageFaultCountIncrease(i64 currentFaultCount)
    {
        auto config = ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment)->JobThrashingDetector;
        YT_LOG_DEBUG(
            "Increased rate of major page faults in user job container detected "
            "(MajorPageFaultCount: %v -> %v, Delta: %v, Threshold: %v, Period: %v, PageFaultLimitOverflowCount: %v)",
            LastMajorPageFaultCount_,
            currentFaultCount,
            currentFaultCount - LastMajorPageFaultCount_,
            config->MajorPageFaultCountLimit,
            config->CheckPeriod,
            PageFaultLimitOverflowCount_);

        if (config->Enabled &&
            currentFaultCount - LastMajorPageFaultCount_ > config->MajorPageFaultCountLimit)
        {
            ++PageFaultLimitOverflowCount_;

            if (PageFaultLimitOverflowCount_ > config->LimitOverflowCountThresholdToAbortJob) {
                YT_LOG_DEBUG(
                    "Too many times in a row page fault count limit was violated, aborting job "
                    "(PageFaultLimitOverflowCountThresholdToAbortJob: %v, MajorPageFaultCountLimit: %v, CheckPeriod: %v)",
                    config->LimitOverflowCountThresholdToAbortJob,
                    config->MajorPageFaultCountLimit,
                    config->CheckPeriod);
                Host_->OnJobMemoryThrashing();
            }
        } else {
            PageFaultLimitOverflowCount_ = 0;
        }

        LastMajorPageFaultCount_ = currentFaultCount;
    }

    void CheckBlockIOUsage()
    {
        std::optional<TJobEnvironmentBlockIOStatistics> blockIOStats;
        try {
            blockIOStats = UserJobEnvironment_->GetBlockIOStatistics()
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Unable to get block io statistics to find a woodpecker");
            return;
        }

        if (blockIOStats) {
            if (UserJobSpec_.has_iops_threshold() &&
                blockIOStats->IOOps > static_cast<i64>(UserJobSpec_.iops_threshold()) &&
                !Woodpecker_)
            {
                YT_LOG_INFO("Woodpecker detected (IORead: %v, IOTotal: %v, Threshold: %v)",
                    blockIOStats->IOReadOps,
                    blockIOStats->IOOps,
                    UserJobSpec_.iops_threshold());
                Woodpecker_ = true;

                if (UserJobSpec_.has_iops_throttler_limit()) {
                    YT_LOG_INFO("Setting IO throttle (Iops: %v)", UserJobSpec_.iops_throttler_limit());
                    UserJobEnvironment_->SetIOThrottle(UserJobSpec_.iops_throttler_limit());
                }
            }
        } else {
            YT_LOG_WARNING("Cannot get block io statistics from job environment");
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

    // NB(psushin): YT-5629.
    void BlinkInputPipe() const
    {
        // This method is called after preparation and before finalization.
        // Reader must be opened and ready, so open must succeed.
        // Still an error can occur in case of external forced sandbox clearance (e.g. in integration tests).
        auto fd = HandleEintr(::open, InputPipePath_.c_str(), O_WRONLY |  O_CLOEXEC | O_NONBLOCK);
        if (fd >= 0) {
            ::close(fd);
        } else {
            YT_LOG_WARNING(TError::FromSystem(), "Failed to blink input pipe (Path: %v)", InputPipePath_);
        }
    }

    static NContainers::EEnablePorto TranslateEnablePorto(NScheduler::EEnablePorto value)
    {
        switch (value) {
            case NScheduler::EEnablePorto::None:    return NContainers::EEnablePorto::None;
            case NScheduler::EEnablePorto::Isolate: return NContainers::EEnablePorto::Isolate;
            default:                                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(
    IJobHostPtr host,
    const TUserJobSpec& userJobSpec,
    TJobId jobId,
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
    TJobId jobId,
    const std::vector<int>& ports,
    std::unique_ptr<TUserJobWriteController> userJobWriteController)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
