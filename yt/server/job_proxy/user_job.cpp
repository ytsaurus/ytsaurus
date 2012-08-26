#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "user_job.h"
#include "user_job_io.h"
#include "stderr_output.h"
#include "table_output.h"

#include <ytlib/formats/format.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/parser.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/rpc/channel.h>

#include <util/folder/dirut.h>

#include "pipes.h"
#include <errno.h>

#ifdef _linux_

#include <unistd.h>
#include <sys/types.h> 
#include <sys/time.h>
#include <sys/wait.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <sys/epoll.h>

#endif

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NTableClient;
using namespace NFormats;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

// ToDo(psushin): set sigint handler?
// ToDo(psushin): extract to a separate file.
TError StatusToError(int status)
{
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        return TError("Process terminated by signal %d",  WTERMSIG(status));
    } else if (WIFSTOPPED(status)) {
        return TError("Process stopped by signal %d",  WSTOPSIG(status));
    } else if (WIFEXITED(status)) {
        return TError("Process exited with value %d",  WEXITSTATUS(status));
    } else {
        return TError("Status %d", status);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TUserJob
    : public TJob
{
public:
    TUserJob(
        IJobHost* host,
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        TAutoPtr<TUserJobIO> userJobIO)
        : TJob(host)
        , UserJobSpec(userJobSpec)
        , JobIO(userJobIO)
        , ProcessId(-1)
        , InputThread(InputThreadFunc, (void*) this)
        , OutputThread(OutputThreadFunc, (void*) this)
    { }

    virtual NScheduler::NProto::TJobResult Run() override
    {
        // ToDo(psushin): use tagged logger here.
        LOG_DEBUG("Starting job process");

        InitPipes();

        ProcessId = fork();
        if (ProcessId < 0) {
            ythrow yexception() << Sprintf("Failed to start the job: fork failed (errno: %d)", errno);
        }

        NScheduler::NProto::TJobResult result;
        try {
            if (ProcessId == 0) {
                // Child process.
                StartJob();
                YUNREACHABLE();
            }

            LOG_DEBUG("Job process started");

            DoJobIO();

            LOG_DEBUG("Job process finished successfully");
            *result.mutable_error() = JobExitStatus.ToProto();
        } catch (const std::exception& ex) {
            LOG_ERROR("Job failed\n%s", ex.what());

            result.mutable_error()->set_code(TError::Fail);
            result.mutable_error()->set_message(ex.what());
        }

        // ToDo(psushin): fix this strange volleyball with StderrChunkId.
        // Keep reference to ErrorOutput in user_job_io.
        auto stderrChunkId = ErrorOutput->GetChunkId();
        if (stderrChunkId != NChunkServer::NullChunkId) {
            JobIO->SetStderrChunkId(stderrChunkId);
            LOG_INFO("Stderr chunk generated (ChunkId: %s)", ~stderrChunkId.ToString());
        }

        JobIO->PopulateResult(&result);

        return result;
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
        do {
            reservedDescriptors.push_back(SafeDup(STDIN_FILENO));
        } while (reservedDescriptors.back() < maxReservedDescriptor);


        ErrorOutput = JobIO->CreateErrorOutput();
        OutputPipes.push_back(New<TOutputPipe>(~ErrorOutput, STDERR_FILENO));

        // Make pipe for each input and each output table.
        {
            auto format = TFormat::FromYson(TYsonString(UserJobSpec.input_format()));
            for (int i = 0; i < JobIO->GetInputCount(); ++i) {
                TAutoPtr<TBlobOutput> buffer(new TBlobOutput());
                TAutoPtr<IYsonConsumer> consumer = CreateConsumerForFormat(
                    format, 
                    EDataType::Tabular, 
                    buffer.Get());

                InputPipes.push_back(New<TInputPipe>(
                    JobIO->CreateTableInput(i, consumer.Get()),
                    buffer,
                    consumer,
                    3 * i));
            }
        }

        {
            auto format = TFormat::FromYson(TYsonString(UserJobSpec.output_format()));
            int outputCount = JobIO->GetOutputCount();
            TableOutput.resize(outputCount);

            for (int i = 0; i < outputCount; ++i) {
                auto writer = JobIO->CreateTableOutput(i);
                TAutoPtr<IYsonConsumer> consumer(new TTableConsumer(writer));
                auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer.Get());
                TableOutput[i] = new TTableOutput(parser, consumer, writer);
                OutputPipes.push_back(New<TOutputPipe>(~TableOutput[i], 3 * i + 1));
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
        TUserJob* job = (TUserJob*)param;
        job->ProcessPipes(job->InputPipes);
    }

    static void* OutputThreadFunc(void* param) 
    {
        NThread::SetCurrentThreadName("JobProxyOutput");
        TUserJob* job = (TUserJob*)param;
        job->ProcessPipes(job->OutputPipes);
    }

    void ProcessPipes(std::vector<TDataPipePtr>& pipes)
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
                ythrow yexception() << Sprintf("Error during job IO: epoll_create failed (errno: %d)", errno);
            }

            FOREACH (auto& pipe, pipes) {
                epoll_event evAdd;
                evAdd.data.u64 = 0ULL;
                evAdd.events = pipe->GetEpollFlags();
                evAdd.data.ptr = ~pipe;

                if (epoll_ctl(epollFd, EPOLL_CTL_ADD, pipe->GetEpollDescriptor(), &evAdd) != 0) {
                    ythrow yexception() << Sprintf("Error during job IO: epoll_ctl failed (errno: %d)", errno);
                }
            }

            const int maxEvents = 10;
            epoll_event events[maxEvents];
            memset(events, 0, maxEvents * sizeof(epoll_event));

            while (activePipeCount > 0 && JobExitStatus.IsOK()) {
                LOG_TRACE("Waiting on epoll, %d pipes active", activePipeCount);

                int epollResult = epoll_wait(epollFd, &events[0], maxEvents, -1);

                if (epollResult < 0) {
                    if (errno == EINTR) {
                        errno = 0;
                        continue;
                    }
                    ythrow yexception() << Sprintf("Error during job IO: epoll_wait failed (errno: %d)", errno);
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
            TGuard<TSpinLock> guard(SpinLock);
            if (JobExitStatus.IsOK()) {
                JobExitStatus = TError(ex.what());
            }
        } catch (...) {
            TGuard<TSpinLock> guard(SpinLock);
            if (JobExitStatus.IsOK()) {
                JobExitStatus = TError("Unknown error during job IO");
            }
        }
    }

    void DoJobIO()
    {
        try {
            InputThread.Start();
            OutputThread.Start();
            OutputThread.Join();
            InputThread.Detach();

            // Finish all output pipes before waitpid.
            FOREACH (auto& pipe, OutputPipes) {
                pipe->Finish();
            }

            int status = 0;
            int waitpidResult = waitpid(ProcessId, &status, 0);
            if (waitpidResult < 0) {
                ythrow yexception() << Sprintf("Waitpid failed (errno: %d)", errno);
            }

            if (!JobExitStatus.IsOK()) {
                ythrow yexception() << Sprintf("User job IO failed: %s", ~JobExitStatus.GetMessage());
            }

            JobExitStatus = StatusToError(status);
            if (!JobExitStatus.IsOK()) {
                ythrow yexception() << Sprintf("User job failed with status: %s", ~JobExitStatus.GetMessage());
            }

            FOREACH (auto& pipe, InputPipes) {
                pipe->Finish();
            }
        } catch (...) {
            // Try to close output pipes despite any errors.
            FOREACH (auto& pipe, OutputPipes) {
                try {
                    pipe->Finish();
                } catch (...) 
                { }
            }
            throw;
        }
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
            // do not search the PATH, inherit environment
            execl("/bin/sh",
                "/bin/sh", 
                "-c", 
                ~cmd, 
                (void*)NULL);
            int _errno = errno;

            fprintf(stderr, "Failed to exec job (/bin/sh -c '%s'): %s\n",
                ~cmd, 
                strerror(_errno));

            // TODO(babenko): extract error code constant
            _exit(7);
        }
        catch (const std::exception& e) {
            auto writeResult = ::write(STDERR_FILENO, e.what(), strlen(e.what()));
        }
        // catch (...) {
        //     // Use some exotic error codes here to analyze errors later.
        //     exit(6);
        // }
    }


    TAutoPtr<TUserJobIO> JobIO;
    NScheduler::NProto::TUserJobSpec UserJobSpec;

    std::vector<TDataPipePtr> InputPipes;
    std::vector<TDataPipePtr> OutputPipes;

    TThread InputThread;
    TThread OutputThread;

    TSpinLock SpinLock;
    TError JobExitStatus;

    TAutoPtr<TErrorOutput> ErrorOutput;
    std::vector< TAutoPtr<TOutputStream> > TableOutput;

    int ProcessId;
};

TAutoPtr<IJob> CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TAutoPtr<TUserJobIO> userJobIO)
{
    return new TUserJob(
        host,
        userJobSpec,
        userJobIO);
}

#else

TAutoPtr<IJob> CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TAutoPtr<TUserJobIO> userJobIO)
{
    ythrow yexception() << "Streaming jobs are supported only under Linux";    
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
