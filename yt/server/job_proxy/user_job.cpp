#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "user_job.h"
#include "user_job_io.h"
#include "stderr_output.h"
#include "table_output.h"
#include "pipes.h"

#include <server/exec_agent/public.h>

#include <core/yson/writer.h>

#include <core/ytree/convert.h>

#include <core/rpc/channel.h>

#include <core/actions/invoker_util.h>

#include <core/misc/fs.h>
#include <core/misc/proc.h>
#include <core/misc/process.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/pattern_formatter.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/action_queue.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/cgroup/cgroup.h>

#include <util/folder/dirut.h>

#include <util/stream/null.h>

#include <errno.h>

#include <util/folder/dirut.h>
#include <util/system/execpath.h>

#ifdef _linux_
    #include <unistd.h>
    #include <signal.h>
    #include <fcntl.h>

    #include <sys/types.h>
    #include <sys/time.h>
    #include <sys/wait.h>
    #include <sys/resource.h>
    #include <sys/stat.h>
    #include <sys/epoll.h>
#endif

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NFormats;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static i64 MemoryLimitBoost = (i64) 2 * 1024 * 1024 * 1024;
static const char* CGroupPrefix = "user_jobs/yt-job-";

class TUserJob
    : public TJob
{
public:
    TUserJob(
        IJobHost* host,
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        const NJobAgent::TJobId& jobId,
        std::unique_ptr<TUserJobIO> userJobIO)
        : TJob(host)
        , JobIO(std::move(userJobIO))
        , UserJobSpec(userJobSpec)
        , JobId(jobId)
        , InitCompleted(false)
        , MemoryUsage(UserJobSpec.memory_reserve())
        , Process(GetExecPath(), false)
        , CpuAccounting(CGroupPrefix + ToString(jobId))
        , BlockIO(CGroupPrefix + ToString(jobId))
        , Memory(CGroupPrefix + ToString(jobId))
    {
        auto config = host->GetConfig();
        MemoryWatchdogExecutor = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TUserJob::CheckMemoryUsage, MakeWeak(this)),
            config->MemoryWatchdogPeriod);
    }

    virtual NJobTrackerClient::NProto::TJobResult Run() override
    {
        // ToDo(psushin): use tagged logger here.
        LOG_DEBUG("Starting job process");

        InitPipes();

        InitCompleted = true;

        if (UserJobSpec.enable_accounting()) {
            CreateCGroup(CpuAccounting);
            CreateCGroup(BlockIO);
            CreateCGroup(Memory);

            Memory.SetLimitInBytes(UserJobSpec.memory_limit());
            Memory.DisableOom();
            OomEvent = Memory.GetOomEvent();
        }

        ProcessStartTime = TInstant::Now();

        auto host = Host.Lock();
        Process.AddArgument("--executor");
        for (auto& pipe : InputPipes) {
            auto jobPipe = pipe->GetJobPipe();
            Process.AddCloseFileAction(jobPipe.WriteFd);
            Process.AddCloseFileAction(jobPipe.PipeIndex);
            Process.AddDup2FileAction(jobPipe.ReadFd, jobPipe.PipeIndex);
            Process.AddCloseFileAction(jobPipe.ReadFd);

            Process.AddArgument("--prepare-pipe");
            Process.AddArgument(::ToString(jobPipe.PipeIndex));
        }

        for (auto& pipe : OutputPipes) {
            auto jobPipe = pipe->GetJobPipe();
            Process.AddCloseFileAction(jobPipe.ReadFd);
            Process.AddCloseFileAction(jobPipe.PipeIndex);
            Process.AddDup2FileAction(jobPipe.WriteFd, jobPipe.PipeIndex);
            Process.AddCloseFileAction(jobPipe.WriteFd);

            Process.AddArgument("--prepare-pipe");
            Process.AddArgument(::ToString(jobPipe.PipeIndex));
        }

        if (UserJobSpec.enable_accounting()) {
            Process.AddArgument("--cgroup");
            Process.AddArgument(CpuAccounting.GetFullPath());

            Process.AddArgument("--cgroup");
            Process.AddArgument(BlockIO.GetFullPath());

            Process.AddArgument("--cgroup");
            Process.AddArgument(Memory.GetFullPath());
        }

        if (UserJobSpec.use_yamr_descriptors()) {
            // This hack is to work around the fact that output pipe accepts single job descriptor,
            // whilst yamr convention requires fds 1 and 3 to be the same.
            Process.AddDup2FileAction(3, 1);
        }

        auto config = host->GetConfig();
        Process.AddArgument("--config");
        Process.AddArgument(NFS::CombinePaths(GetCwd(), NExecAgent::ProxyConfigFileName));
        Process.AddArgument("--working-dir");
        Process.AddArgument(config->SandboxName);

        if (UserJobSpec.enable_vm_limit()) {
            auto memoryLimit = static_cast<rlim_t>(UserJobSpec.memory_limit() * config->MemoryLimitMultiplier);
            memoryLimit += MemoryLimitBoost;
            Process.AddArgument("--vm-limit");
            Process.AddArgument(::ToString(memoryLimit));
        }

        if (!UserJobSpec.enable_core_dump()) {
            Process.AddArgument("--enable-core-dumps");
        }

        if (config->UserId > 0) {
            Process.AddArgument("--uid");
            Process.AddArgument(::ToString(config->UserId));

            if (UserJobSpec.enable_io_prio()) {
                Process.AddArgument("--enable-io-prio");
            }
        }

        Process.AddArgument("--command");
        Process.AddArgument(UserJobSpec.shell_command());

        TPatternFormatter formatter;
        formatter.AddProperty("SandboxPath", GetCwd());

        for (int i = 0; i < UserJobSpec.environment_size(); ++i) {
            Process.AddEnvVar(formatter.Format(UserJobSpec.environment(i)));
        }

        auto error = Process.Spawn();
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to spawn the job")
                << error;
        }

        NJobTrackerClient::NProto::TJobResult result;

        LOG_INFO("Job process started");

        MemoryWatchdogExecutor->Start();
        DoJobIO();
        MemoryWatchdogExecutor->Stop();

        LOG_INFO(JobExitError, "Job process completed");
        ToProto(result.mutable_error(), JobExitError);

        if (UserJobSpec.enable_accounting()) {
            RetrieveStatistics(CpuAccounting, [&] (NCGroup::TCpuAccounting& cgroup) {
                    CpuAccountingStats = cgroup.GetStatistics();
                });
            RetrieveStatistics(BlockIO, [&] (NCGroup::TBlockIO& cgroup) {
                    BlockIOStats = cgroup.GetStatistics();
                });
            RetrieveStatistics(Memory, [&] (NCGroup::TMemory& cgroup) { });

            DestroyCGroup(CpuAccounting);
            DestroyCGroup(BlockIO);
            OomEvent.Destroy();
            DestroyCGroup(Memory);
        }

        if (ErrorOutput) {
            auto stderrChunkId = ErrorOutput->GetChunkId();
            if (stderrChunkId != NChunkServer::NullChunkId) {
                auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                ToProto(schedulerResultExt->mutable_stderr_chunk_id(), stderrChunkId);
                LOG_INFO("Stderr chunk generated (ChunkId: %s)", ~ToString(stderrChunkId));
            }
        }

        if (JobExitError.IsOK()) {
            JobIO->PopulateResult(&result);
        }

        return result;
    }

    virtual double GetProgress() const override
    {
        return InitCompleted ? JobIO->GetProgress() : 0;
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return JobIO->GetFailedChunkIds();
    }

private:
    void InitPipes()
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

        int maxReservedDescriptor = 0;

        if (UserJobSpec.use_yamr_descriptors()) {
            maxReservedDescriptor = 2 + JobIO->GetOutputCount();
        } else {
            maxReservedDescriptor = std::max(
                JobIO->GetInputCount(),
                JobIO->GetOutputCount()) * 3;
        }

        YASSERT(maxReservedDescriptor > 0);

        // To avoid descriptor collisions between pipes on this, proxy side,
        // and "standard" descriptor numbers in forked job (see comments above)
        // we ensure that enough lower descriptors are allocated before creating pipes.

        std::vector<int> reservedDescriptors;
        auto createPipe = [&] (int fd[2]) {
            while (true) {
                SafePipe(fd);
                if (fd[0] < maxReservedDescriptor || fd[1] < maxReservedDescriptor) {
                    reservedDescriptors.push_back(fd[0]);
                    reservedDescriptors.push_back(fd[1]);
                } else {
                    break;
                }
            }
        };

        int pipe[2];
        createPipe(pipe);

        // Configure stderr pipe.
        TOutputStream* stderrOutput = &NullErrorOutput;
        if (UserJobSpec.has_stderr_transaction_id()) {
            auto stderrTransactionId = FromProto<TTransactionId>(UserJobSpec.stderr_transaction_id());
            ErrorOutput = JobIO->CreateErrorOutput(
                stderrTransactionId,
                UserJobSpec.max_stderr_size());
            stderrOutput = ErrorOutput.get();
        }
        
        OutputPipes.push_back(New<TOutputPipe>(pipe, stderrOutput, STDERR_FILENO));

        // Make pipe for each input and each output table.
        {
            YCHECK(!UserJobSpec.use_yamr_descriptors() || JobIO->GetInputCount() == 1);

            auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec.input_format()));
            for (int i = 0; i < JobIO->GetInputCount(); ++i) {
                std::unique_ptr<TBlobOutput> buffer(new TBlobOutput());
                auto consumer = CreateConsumerForFormat(
                    format,
                    EDataType::Tabular,
                    buffer.get());

                createPipe(pipe);
                InputPipes.push_back(New<TInputPipe>(
                    pipe,
                    JobIO->CreateTableInput(i, consumer.get()),
                    std::move(buffer),
                    std::move(consumer),
                    3 * i));
            }
        }

        {
            auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec.output_format()));
            int outputCount = JobIO->GetOutputCount();
            TableOutput.resize(outputCount);

            Writers.reserve(outputCount);
            for (int i = 0; i < outputCount; ++i) {
                auto writer = JobIO->CreateTableOutput(i);
                Writers.push_back(writer);
            }

            for (int i = 0; i < outputCount; ++i) {
                std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(Writers, i));
                auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer.get());
                TableOutput[i].reset(new TTableOutput(
                    std::move(parser),
                    std::move(consumer)));
                createPipe(pipe);

                int jobDescriptor = UserJobSpec.use_yamr_descriptors()
                    ? 3 + i
                    : 3 * i + 1;

                OutputPipes.push_back(New<TOutputPipe>(pipe, TableOutput[i].get(), jobDescriptor));
            }
        }

        // Close reserved descriptors.
        for (int fd : reservedDescriptors) {
            SafeClose(fd);
        }

        LOG_DEBUG("Pipes initialized");
    }

    void SetError(const TError& error)
    {
        if (error.IsOK()) {
            return;
        }

        TGuard<TSpinLock> guard(SpinLock);
        if (JobExitError.IsOK()) {
            JobExitError = TError("User job failed");
        };

        JobExitError.InnerErrors().push_back(error);
    }

    void DoJobIO()
    {
        for (auto& pipe : InputPipes) {
            pipe->PrepareProxyDescriptors();
        }
        for (auto& pipe : OutputPipes) {
            pipe->PrepareProxyDescriptors();
        }

        auto queue = New<NConcurrency::TActionQueue>("PipesIO");

        std::vector<TAsyncError> inputFinishEvents;
        std::vector<TAsyncError> outputFinishEvents;

        auto doAll = [this] (IDataPipePtr pipe) {
            auto error = pipe->DoAll();
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Pipe has failed");
                auto closeError = pipe->Close();
                if (!closeError.IsOK()) {
                    SetError(closeError);
                }
            }
            return error;
        };

        for (auto& pipe : InputPipes) {
            inputFinishEvents.push_back(BIND(doAll, pipe).AsyncVia(queue->GetInvoker()).Run());
        }

        for (auto& pipe : OutputPipes) {
            outputFinishEvents.push_back(BIND(doAll, pipe).AsyncVia(queue->GetInvoker()).Run());
        }

        for (auto& asyncError : outputFinishEvents) {
            auto error = asyncError.Get();
            if (!error.IsOK()) {
                SetError(error);
            }
        }

        LOG_DEBUG("Done processing job outputs");

        auto error = Process.Wait();
        if (!error.IsOK()) {
            SetError(error);
        }

        LOG_DEBUG("Child process has finished");

        auto finishPipe = [&] (IDataPipePtr pipe) {
            try {
                pipe->Finish();
            } catch (const std::exception& ex) {
                SetError(TError(ex));
            }
        };

        // Stderr output pipe finishes first.
        for (auto& pipe : OutputPipes) {
            finishPipe(pipe);
        }

        for (auto& pipe : InputPipes) {
            finishPipe(pipe);
        }

        LOG_DEBUG("Finished pipes");

        for (auto& writer : Writers) {
            try {
                writer->Close();
            } catch (const std::exception& ex) {
                SetError(TError(ex));
            }
        }

        LOG_DEBUG("Closed writers");

        for (auto& asyncError : inputFinishEvents) {
            auto error = asyncError.Get();
            if (!error.IsOK()) {
                SetError(error);
            }
        }

        LOG_DEBUG("Done processing job inputs");
    }

    void CheckMemoryUsage()
    {
        auto host = Host.Lock();
        if (!host)
            return;

        int uid = host->GetConfig()->UserId;
        if (uid <= 0) {
            return;
        }

        if (!Memory.IsCreated()) {
            return;
        }

        try {
            i64 memoryLimit = UserJobSpec.memory_limit();
            auto statistics = Memory.GetStatistics();
            LOG_DEBUG("Get memory usage (JobId: %s, UsageInBytes: %" PRId64 ", MemoryLimit: %" PRId64 ")",
                ~ToString(JobId),
                statistics.UsageInBytes,
                memoryLimit);

            if (OomEvent.Fired()) {
                SetError(TError(NJobProxy::EErrorCode::MemoryLimitExceeded, "Memory limit exceeded")
                    << TErrorAttribute("time_since_start", TInstant::Now() - ProcessStartTime)
                    << TErrorAttribute("usage_in_bytes", statistics.UsageInBytes)
                    << TErrorAttribute("limit", memoryLimit));
                NCGroup::RunKiller(Memory.GetFullPath());
                return;
            }

            if (statistics.UsageInBytes > MemoryUsage) {
                i64 delta = statistics.UsageInBytes - MemoryUsage;
                LOG_INFO("Memory usage increased by %" PRId64, delta);

                MemoryUsage += delta;

                auto resourceUsage = host->GetResourceUsage();
                resourceUsage.set_memory(resourceUsage.memory() + delta);
                host->SetResourceUsage(resourceUsage);
            }
        } catch (const std::exception& ex) {
            SetError(ex);
            NCGroup::RunKiller(Memory.GetFullPath());
        }
    }

    virtual NJobTrackerClient::NProto::TJobStatistics GetStatistics() const override
    {
        NJobTrackerClient::NProto::TJobStatistics result;
        result.set_time(GetElapsedTime().MilliSeconds());

        ToProto(result.mutable_input(), JobIO->GetInputDataStatistics());
        ToProto(result.mutable_output(), JobIO->GetOutputDataStatistics());

        if (UserJobSpec.enable_accounting()) {
            ToProto(result.mutable_cpu(), CpuAccountingStats);
            ToProto(result.mutable_block_io(), BlockIOStats);
        }

        return result;
    }

    void CreateCGroup(NCGroup::TCGroup& cgroup)
    {
        try {
            cgroup.Create();
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Unable to create cgroup %s", ~cgroup.GetFullPath().Quote());
        }
    }

    template <typename T, typename Func>
    void RetrieveStatistics(T& cgroup, Func retriever)
    {
        if (cgroup.IsCreated()) {
            try {
                retriever(cgroup);
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Unable to retrieve statistics from cgroup %s", ~cgroup.GetFullPath().Quote());
            }
        }
    }

    void DestroyCGroup(NCGroup::TCGroup& cgroup)
    {
        if (cgroup.IsCreated()) {
            try {
                NCGroup::RunKiller(cgroup.GetFullPath());
                cgroup.Destroy();
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Unable to destroy cgroup %s", ~cgroup.GetFullPath().Quote());
            }
        }
    }

    std::unique_ptr<TUserJobIO> JobIO;

    const NScheduler::NProto::TUserJobSpec& UserJobSpec;
    NJobAgent::TJobId JobId;

    volatile bool InitCompleted;

    std::vector<IDataPipePtr> InputPipes;
    std::vector<IDataPipePtr> OutputPipes;

    std::vector<ISyncWriterPtr> Writers;

    TSpinLock SpinLock;
    TError JobExitError;

    i64 MemoryUsage;

    TPeriodicExecutorPtr MemoryWatchdogExecutor;

    std::unique_ptr<TErrorOutput> ErrorOutput;
    TNullOutput NullErrorOutput;
    std::vector< std::unique_ptr<TOutputStream> > TableOutput;

    TInstant ProcessStartTime;

    TProcess Process;

    NCGroup::TCpuAccounting CpuAccounting;
    NCGroup::TCpuAccounting::TStatistics CpuAccountingStats;

    NCGroup::TBlockIO BlockIO;
    NCGroup::TBlockIO::TStatistics BlockIOStats;

    NCGroup::TMemory Memory;
    NCGroup::TEvent OomEvent;
};

TJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    std::unique_ptr<TUserJobIO> userJobIO,
    const NJobAgent::TJobId& jobId)
{
    return New<TUserJob>(
        host,
        userJobSpec,
        jobId,
        std::move(userJobIO));
}

#else

TJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    std::unique_ptr<TUserJobIO> userJobIO,
    const NJobAgent::TJobId& jobId)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Linux");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
