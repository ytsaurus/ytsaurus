#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "user_job.h"
#include "user_job_io.h"
#include "stderr_output.h"
#include "table_output.h"
#include "pipes.h"

#include <ytlib/formats/format.h>

#include <ytlib/yson/yson_writer.h>
#include <ytlib/formats/parser.h>
#include <ytlib/ytree/convert.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/rpc/channel.h>

#include <ytlib/actions/invoker_util.h>

#include <ytlib/misc/proc.h>
#include <ytlib/misc/periodic_invoker.h>

#include <util/folder/dirut.h>

#include <util/stream/null.h>

#include <errno.h>

#ifdef _linux_
    #include <unistd.h>
    #include <signal.h>
    #include <sys/types.h>
    #include <sys/time.h>
    #include <sys/wait.h>
    #include <sys/resource.h>

    #include <sys/stat.h>
    #include <fcntl.h>
    #include <sys/epoll.h>
#endif

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NFormats;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TUserJob
    : public TJob
{
public:
    TUserJob(
        IJobHost* host,
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        TAutoPtr<TUserJobIO> userJobIO)
        : TJob(host)
        , JobIO(userJobIO)
        , UserJobSpec(userJobSpec)
        , InputThread(InputThreadFunc, (void*) this)
        , OutputThread(OutputThreadFunc, (void*) this)
        , MemoryUsage(UserJobSpec.memory_reserve())
        , ProcessId(-1)
    {
        MemoryWatchdogInvoker = New<TPeriodicInvoker>(
            GetSyncInvoker(),
            BIND(&TUserJob::CheckMemoryUsage, MakeWeak(this)),
            Host->GetConfig()->MemoryWatchdogPeriod);
    }

    virtual NScheduler::NProto::TJobResult Run() override
    {
        // ToDo(psushin): use tagged logger here.
        LOG_DEBUG("Starting job process");

        InitPipes();

        ProcessId = fork();
        if (ProcessId < 0) {
            THROW_ERROR_EXCEPTION("Failed to start the job: fork failed")
                << TError::FromSystem();
        }

        NScheduler::NProto::TJobResult result;

        if (ProcessId == 0) {
            // Child process.
            StartJob();
            YUNREACHABLE();
        }

        LOG_INFO("Job process started");

        MemoryWatchdogInvoker->Start();
        DoJobIO();
        MemoryWatchdogInvoker->Stop();

        LOG_INFO(JobExitError, "Job process completed");
        ToProto(result.mutable_error(), JobExitError);

        if (~ErrorOutput) {
            // ToDo(psushin): fix this strange volleyball with StderrChunkId.
            // Keep reference to ErrorOutput in user_job_io.
            auto stderrChunkId = ErrorOutput->GetChunkId();
            if (stderrChunkId != NChunkServer::NullChunkId) {
                JobIO->SetStderrChunkId(stderrChunkId);
                LOG_INFO("Stderr chunk generated (ChunkId: %s)", ~stderrChunkId.ToString());
            }
        }

        JobIO->PopulateResult(&result);

        return result;
    }

    virtual double GetProgress() const override
    {
        return JobIO->GetProgress();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunks() const override
    {
        return JobIO->GetFailedChunks();
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

        int maxReservedDescriptor = std::max(
            JobIO->GetInputCount(),
            JobIO->GetOutputCount()) * 3;

        YASSERT(maxReservedDescriptor > 0);

        // To avoid descriptor collisions between pipes on this, proxy side,
        // and "standard" descriptor numbers in forked job (see comments above)
        // we ensure that lower 3 * N descriptors are allocated before creating pipes.

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
        TOutputStream* stdErrOutput;
        if (UserJobSpec.has_stderr_transaction_id()) {
            auto stderrTransactionId = NTransactionClient::TTransactionId::FromProto(UserJobSpec.stderr_transaction_id());
            ErrorOutput = JobIO->CreateErrorOutput(stderrTransactionId);
            stdErrOutput = ~ErrorOutput;
        } else {
            stdErrOutput = &NullErrorOutput;
        }
        OutputPipes.push_back(New<TOutputPipe>(pipe, stdErrOutput, STDERR_FILENO));

        // Make pipe for each input and each output table.
        {
            auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec.input_format()));
            for (int i = 0; i < JobIO->GetInputCount(); ++i) {
                TAutoPtr<TBlobOutput> buffer(new TBlobOutput());
                TAutoPtr<IYsonConsumer> consumer = CreateConsumerForFormat(
                    format,
                    EDataType::Tabular,
                    buffer.Get());

                createPipe(pipe);
                InputPipes.push_back(New<TInputPipe>(
                    pipe,
                    JobIO->CreateTableInput(i, consumer.Get()),
                    buffer,
                    consumer,
                    3 * i));
            }
        }

        {
            auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec.output_format()));
            int outputCount = JobIO->GetOutputCount();
            TableOutput.resize(outputCount);

            for (int i = 0; i < outputCount; ++i) {
                auto writer = JobIO->CreateTableOutput(i);
                TAutoPtr<IYsonConsumer> consumer(new TTableConsumer(writer));
                auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer.Get());
                TableOutput[i] = new TTableOutput(parser, consumer, writer);
                createPipe(pipe);
                OutputPipes.push_back(New<TOutputPipe>(pipe, ~TableOutput[i], 3 * i + 1));
            }
        }

        // Close reserved descriptors.
        FOREACH (int fd, reservedDescriptors) {
            SafeClose(fd);
        }

        LOG_DEBUG("Pipes initialized");
    }

    static void* InputThreadFunc(void* param)
    {
        NThread::SetCurrentThreadName("JobProxyInput");
        TIntrusivePtr<TUserJob> job = (TUserJob*)param;
        job->ProcessPipes(job->InputPipes);
        return NULL;
    }

    static void* OutputThreadFunc(void* param)
    {
        NThread::SetCurrentThreadName("JobProxyOutput");
        TIntrusivePtr<TUserJob> job = (TUserJob*)param;
        job->ProcessPipes(job->OutputPipes);
        return NULL;
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

    void ProcessPipes(std::vector<IDataPipePtr>& pipes)
    {
        // TODO(babenko): rewrite using libuv
        try {
            int activePipeCount = pipes.size();

            FOREACH (auto& pipe, pipes) {
                pipe->PrepareProxyDescriptors();
            }

            const int fdCountHint = 10;
            int epollFd = epoll_create(fdCountHint);
            if (epollFd < 0) {
                THROW_ERROR_EXCEPTION("Error during job IO: epoll_create failed")
                    << TError::FromSystem();
            }

            FOREACH (auto& pipe, pipes) {
                epoll_event evAdd;
                evAdd.data.u64 = 0ULL;
                evAdd.events = pipe->GetEpollFlags();
                evAdd.data.ptr = ~pipe;

                if (epoll_ctl(epollFd, EPOLL_CTL_ADD, pipe->GetEpollDescriptor(), &evAdd) != 0) {
                    THROW_ERROR_EXCEPTION("Error during job IO: epoll_ctl failed")
                        << TError::FromSystem();
                }
            }

            const int maxEvents = 10;
            epoll_event events[maxEvents];
            memset(events, 0, maxEvents * sizeof(epoll_event));

            while (activePipeCount > 0) {
                {
                    TGuard<TSpinLock> guard(SpinLock);
                    if (!JobExitError.IsOK()) {
                        break;
                    }
                }

                LOG_TRACE("Waiting on epoll, %d pipes active", activePipeCount);

                int epollResult = epoll_wait(epollFd, &events[0], maxEvents, -1);

                if (epollResult < 0) {
                    if (errno == EINTR) {
                        errno = 0;
                        continue;
                    }
                    THROW_ERROR_EXCEPTION("Error during job IO: epoll_wait failed")
                        << TError::FromSystem();
                }

                for (int pipeIndex = 0; pipeIndex < epollResult; ++pipeIndex) {
                    auto pipe = reinterpret_cast<IDataPipe*>(events[pipeIndex].data.ptr);
                    if (!pipe->ProcessData(events[pipeIndex].events)) {
                        --activePipeCount;
                    }
                }
            }

            SafeClose(epollFd);
        } catch (const std::exception& ex) {
            SetError(TError(ex));
        } catch (...) {
            SetError(TError("Unknown error during job IO"));
        }

        FOREACH (auto& pipe, pipes) {
            // Close can throw exception which will cause JobProxy death.
            // For now let's assume it is unrecoverable.
            // Anyway, system seems to be in a very bad state if this happens.
            pipe->CloseHandles();
        }
    }

    void DoJobIO()
    {
        InputThread.Start();
        OutputThread.Start();
        OutputThread.Join();

        int status = 0;
        int waitpidResult = waitpid(ProcessId, &status, 0);
        if (waitpidResult < 0) {
            SetError(TError("waitpid failed") << TError::FromSystem());
        } else {
            SetError(StatusToError(status));
        }

        auto finishPipe = [&] (IDataPipePtr pipe) {
            try {
                pipe->Finish();
            } catch (const std::exception& ex) {
                SetError(TError(ex));
            }
        };

        // Stderr output pipe finishes first.
        FOREACH (auto& pipe, OutputPipes) {
            finishPipe(pipe);
        }

        FOREACH (auto& pipe, InputPipes) {
            finishPipe(pipe);
        }

        // If user process fais, InputThread may be blocked on epoll
        // because reading end of input pipes is left open to
        // check that all data was consumed. That is why we should join the
        // thread after pipe finish.
        InputThread.Join();
    }

    // Called from the forked process.
    void StartJob()
    {
        try {
            FOREACH (auto& pipe, InputPipes) {
                pipe->PrepareJobDescriptors();
            }

            FOREACH (auto& pipe, OutputPipes) {
                pipe->PrepareJobDescriptors();
            }

            // ToDo(psushin): handle errors.
            auto config = Host->GetConfig();
            ChDir(config->SandboxName);

            Stroka cmd = UserJobSpec.shell_command();

            std::vector<const char*> envp(UserJobSpec.environment_size() + 1);
            for (int i = 0; i < UserJobSpec.environment_size(); ++i) {
                envp[i] = ~UserJobSpec.environment(i);
            }
            envp[UserJobSpec.environment_size()] = NULL;

            auto memoryLimit = static_cast<rlim_t>(UserJobSpec.memory_limit() * config->MemoryLimitMultiplier);
            struct rlimit rlimit = {memoryLimit, RLIM_INFINITY};

            auto res = setrlimit(RLIMIT_AS, &rlimit);

            if (res) {
                fprintf(stderr, "Failed to set resource limits (MemoryLimit: %" PRId64 ")\n%s",
                    rlimit.rlim_max,
                    strerror(errno));
                _exit(EJobProxyExitCode::SetRLimitFailed);
            }

            if (config->UserId > 0) {
                // Set unprivileged uid and gid for user process.
                YCHECK(setuid(0) == 0);
                YCHECK(setresgid(config->UserId, config->UserId, config->UserId) == 0);
                YCHECK(setuid(config->UserId) == 0);
            }

            // do not search the PATH, inherit environment
            execle("/bin/sh",
                "/bin/sh",
                "-c",
                ~cmd,
                (void*)NULL,
                envp.data());

            int _errno = errno;

            fprintf(stderr, "Failed to exec job (/bin/sh -c '%s'): %s\n",
                ~cmd,
                strerror(_errno));
            _exit(EJobProxyExitCode::ExecFailed);
        } catch (const std::exception& ex) {
            fprintf(stderr, "%s", ex.what());
            _exit(EJobProxyExitCode::UncaughtException);
        }
    }

    void Kill()
    {
        auto uid = Host->GetConfig()->UserId;
        KillallByUser(uid);
    }

    void CheckMemoryUsage()
    {
        auto uid = Host->GetConfig()->UserId;
        if (uid <= 0) {
            return;
        }

        try {
            auto rss = GetUserRss(uid);
            LOG_DEBUG(
                "Checking memory usage (MemoryLimit: %" PRId64 ", RSS: %" PRId64 ")",
                UserJobSpec.memory_limit(),
                rss);

            if (rss > UserJobSpec.memory_limit()) {
                SetError(TError(
                    "Memory limit exceeded (MemoryLimit: %" PRId64 ", RSS: %" PRId64 ")",
                    UserJobSpec.memory_limit(),
                    rss));
                Kill();
                return;
            }

            if (rss > MemoryUsage) {
                auto delta = rss - MemoryUsage;
                LOG_INFO("Increase memory usage (Delta %" PRId64 ")", delta);
                auto resourceUsage = Host->GetResourceUsage();
                resourceUsage.set_memory(resourceUsage.memory() + delta);
                Host->SetResourceUsage(resourceUsage);
                MemoryUsage += delta;
            }

            MemoryWatchdogInvoker->ScheduleNext();
        } catch (const std::exception& ex) {
            SetError(TError(ex));
            Kill();
        }
    }


    TAutoPtr<TUserJobIO> JobIO;
    NScheduler::NProto::TUserJobSpec UserJobSpec;

    std::vector<IDataPipePtr> InputPipes;
    std::vector<IDataPipePtr> OutputPipes;

    TThread InputThread;
    TThread OutputThread;

    TSpinLock SpinLock;
    TError JobExitError;

    i64 MemoryUsage;

    TPeriodicInvokerPtr MemoryWatchdogInvoker;

    TAutoPtr<TErrorOutput> ErrorOutput;
    TNullOutput NullErrorOutput;
    std::vector< TAutoPtr<TOutputStream> > TableOutput;

    int ProcessId;
};

TJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TAutoPtr<TUserJobIO> userJobIO)
{
    return New<TUserJob>(
        host,
        userJobSpec,
        userJobIO);
}

#else

TJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TAutoPtr<TUserJobIO> userJobIO)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Linux");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
