#include "stdafx.h"

#include "private.h"
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

static NLog::TLogger& Logger = JobProxyLogger;

using namespace NScheduler::NProto;
using namespace NYTree;
using namespace NTableClient;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

// ToDo(psushin): set sigint handler?
// ToDo(psushin): extract it to separate file.
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

#endif

////////////////////////////////////////////////////////////////////////////////

TUserJob::TUserJob(
    TJobProxyConfigPtr proxyConfig,
    const TJobSpec& jobSpec)
    : Config(proxyConfig)
    , ActivePipeCount(0)
    , ProcessId(-1)
{
    YCHECK(jobSpec.HasExtension(TUserJobSpecExt::user_job_spec_ext));

    UserJobSpecExt = jobSpec.GetExtension(TUserJobSpecExt::user_job_spec_ext);
    JobIO = CreateUserJobIO(proxyConfig->JobIO, proxyConfig->Masters, jobSpec);
}

#ifdef _linux_

NScheduler::NProto::TJobResult TUserJob::Run()
{
    // ToDo(psushin): use tagged logger here.
    LOG_DEBUG("Running new user job");
    InitPipes();

    ProcessId = fork();
    if (ProcessId < 0) {
        ythrow yexception() << Sprintf("Failed to start the job: fork failed (errno: %d)", errno);
    }

    if (ProcessId == 0) {
        // Child process.
        StartJob();
        YUNREACHABLE();
    }

    LOG_DEBUG("Job process started");

    DoJobIO();

    LOG_DEBUG("Job successfully finished");

    NScheduler::NProto::TJobResult result;
    *result.mutable_error() = JobExitStatus.ToProto();

    {
        auto chunkId = ErrorOutput->GetChunkId();
        if (chunkId != NChunkServer::NullChunkId) {
            auto* resultExt = result.MutableExtension(NScheduler::NProto::TUserJobResultExt::user_job_result_ext);
            *resultExt->mutable_stderr_chunk_id() = chunkId->ToProto();           
        }
    }

    return result;
}


void TUserJob::InitPipes()
{
    LOG_DEBUG("Initializing pipes");

    // We use a special convention to numerate input and output file descriptors
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
    // Special option (ToDo: which?) makes possible to concatenate 
    // all input streams into fd == 0.

    int maxReservedDescriptor = std::max(
        JobIO->GetInputCount(), JobIO->GetOutputCount()) * 3;

    YASSERT(maxReservedDescriptor > 0);

    // To avoid descriptor collisions between pipes on this, proxy side,
    // and "standard" descriptor numbers in forked job (see comments above) 
    // we ensure that lower 3 * N descriptors are allocated before creating pipes.

    std::vector<int> reservedDescriptors;
    do {
        reservedDescriptors.push_back(SafeDup(STDIN_FILENO));
    } while (reservedDescriptors.back() < maxReservedDescriptor);


    ErrorOutput = JobIO->CreateErrorOutput();
    Pipes.push_back(New<TOutputPipe>(~ErrorOutput, STDERR_FILENO));
    ++ActivePipeCount;

    // Make pipe for each input and each output table.
    {
        auto format = TFormat::FromYson(UserJobSpecExt.input_format());

        for (int i = 0; i < JobIO->GetInputCount(); ++i) {
            TAutoPtr<TBlobOutput> buffer(new TBlobOutput());
            TAutoPtr<IYsonConsumer> consumer = CreateConsumerForFormat(
                format, 
                EDataType::Tabular, 
                buffer.Get());

            Pipes.push_back(New<TInputPipe>(
                JobIO->CreateTableInput(i, consumer.Get()),
                buffer,
                consumer,
                3 * i));
        }
    }

    {
        auto format = TFormat::FromYson(UserJobSpecExt.output_format());
        auto outputCount = JobIO->GetOutputCount();
        TableOutput.resize(outputCount);

        for (int i = 0; i < outputCount; ++i) {
            ++ActivePipeCount;
            auto writer = JobIO->CreateTableOutput(i);
            TAutoPtr<IYsonConsumer> consumer(new TTableConsumer(
                Config->JobIO->TableConsumer, 
                writer));
            auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer.Get());
            TableOutput[i] = new TTableOutput(parser, consumer, writer);
            Pipes.push_back(New<TOutputPipe>(~TableOutput[i], 3 * i + 1));
        }

    }

    // Close reserved descriptors.
    FOREACH (int fd, reservedDescriptors) {
        SafeClose(fd);
    }

    LOG_DEBUG("Pipes initialized");
}

void TUserJob::StartJob()
{
    try {
        FOREACH (auto& pipe, Pipes) {
            pipe->PrepareJobDescriptors();
        }

        // ToDo(psushin): handle errors.
        ChDir(Config->SandboxName);


        Stroka cmd = UserJobSpecExt.shell_command();
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

void TUserJob::DoJobIO()
{
    // TODO(babenko): rewrite using libev
    try {
        FOREACH (auto& pipe, Pipes) {
            pipe->PrepareProxyDescriptors();
        }

        const int fdCountHint = 10;
        int epollFd = epoll_create(fdCountHint);
        if (epollFd < 0) {
            ythrow yexception() << Sprintf("Error during job IO: epoll_create failed (errno: %d)", errno);
        }

        FOREACH (auto& pipe, Pipes) {
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

        while (ActivePipeCount > 0) {
            LOG_TRACE("Waiting on epoll, %d pipes active", ActivePipeCount);

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
                    --ActivePipeCount;
                }
            }
        }

        int status = 0;
        int waitpidResult = waitpid(ProcessId, &status, 0);
        if (waitpidResult < 0) {
            ythrow yexception() << Sprintf("Error during job IO: waitpid failed (errno: %d)", errno);
        }

        JobExitStatus = StatusToError(status);

        FOREACH (auto& pipe, Pipes) {
            pipe->Finish();
        }

        SafeClose(epollFd);
    } catch (...) {
        // Try to close all pipes despite any other errors.
        // It is safe to call Finish multiple times.
        FOREACH (auto& pipe, Pipes) {
            try {
                pipe->Finish();
            } catch (...) 
            { }
        }
        throw;
    }
}

#else

// Streaming jobs are not supposed to work on windows for now.

NScheduler::NProto::TJobResult TUserJob::Run()
{
    YUNIMPLEMENTED();
}

void TUserJob::InitPipes()
{
    YUNIMPLEMENTED();
}

void TUserJob::StartJob()
{
    YUNIMPLEMENTED();
}

void TUserJob::DoJobIO()
{
    YUNIMPLEMENTED();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
