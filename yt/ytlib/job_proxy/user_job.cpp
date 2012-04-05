#include "stdafx.h"

#include "private.h"
#include "config.h"
#include "user_job.h"
#include "user_job_io.h"

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
    const TJobProxyConfigPtr& proxyConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
    : Config(proxyConfig)
    , ActivePipesCount(0)
    , ProcessId(-1)
{
    YASSERT(jobSpec.HasExtension(TUserJobSpec::user_job_spec));

    UserJobSpec = jobSpec.GetExtension(TUserJobSpec::user_job_spec);
    JobIO = CreateUserJobIO(proxyConfig->JobIO, proxyConfig->Masters, jobSpec);
}

#ifdef _linux_

NScheduler::NProto::TJobResult TUserJob::Run()
{
    // ToDo(psushin): use tagged logger here.
    LOG_DEBUG("Running new user job");
    InitPipes();
    LOG_DEBUG("Initialized pipes");

    ProcessId = fork();
    if (ProcessId < 0)
        ythrow yexception() << "fork failed with errno: " << errno;

    if (ProcessId == 0) {
        // Child process.
        StartJob();
        YUNREACHABLE();
    }

    LOG_DEBUG("Job process started, begin IO");

    DoJobIO();

    LOG_DEBUG("Job successfully finished");

    NScheduler::NProto::TJobResult result;
    *result.mutable_error() = JobExitStatus.ToProto();
    return result;
}


void TUserJob::InitPipes()
{
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


    DataPipes.push_back(New<TOutputPipe>(JobIO->CreateErrorOutput(), STDERR_FILENO));
    ++ActivePipesCount;

    // Make pipe for each input and each output table.
    for (int i = 0; i < JobIO->GetInputCount(); ++i) {
        TAutoPtr<TBlobOutput> buffer(new TBlobOutput());
        DataPipes.push_back(New<TInputPipe>(
            JobIO->CreateTableInput(i, buffer.Get()) ,
            buffer,
            3 * i));
    }

    for (int i = 0; i < JobIO->GetOutputCount(); ++i) {
        ++ActivePipesCount;
        DataPipes.push_back(New<TOutputPipe>(JobIO->CreateTableOutput(i), 3 * i + 1));
    }

    // Close reserved descriptors.
    FOREACH(auto& fd, reservedDescriptors) {
        SafeClose(fd);
    }
}

void TUserJob::StartJob()
{
    try {
        FOREACH(auto& pipe, DataPipes) {
            pipe->PrepareJobDescriptors();
        }

        // ToDo(psushin): handle errors.
        ChDir(Config->SandboxName);


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

        exit(7);
    }
    catch (const std::exception& e) {
        UNUSED(::write(STDERR_FILENO, e.what(), strlen(e.what())));
    }
    // catch (...) {
    //     // Use some exotic error codes here to analyze errors later.
    //     exit(6);
    // }
}

void TUserJob::DoJobIO()
{
    try {
        FOREACH(auto& pipe, DataPipes) {
            pipe->PrepareProxyDescriptors();
        }

        const int fd_count_hint = 10;
        int efd = epoll_create(fd_count_hint);
        if (efd < 0) {
            ythrow yexception() << "epoll_create failed with errno: " << errno;
        }

        FOREACH(auto& pipe, DataPipes) {
            epoll_event ev_add;
            ev_add.data.u64 = 0ULL;
            ev_add.events = pipe->GetEpollFlags();
            ev_add.data.ptr = ~pipe;

            if (epoll_ctl(efd, EPOLL_CTL_ADD, pipe->GetEpollDescriptor(), &ev_add) != 0) {
                ythrow yexception() << "epoll_ctl failed with errno: " << errno;
            }
        }

        const int maxevents = 10;
        epoll_event ev[maxevents];
        memset(ev, 0, maxevents * sizeof(epoll_event));
        int n;

        while (ActivePipesCount > 0) {
            LOG_TRACE("Waiting on epoll, active pipes: %d", ActivePipesCount);

            n = epoll_wait(efd, &ev[0], maxevents, -1);

            if (n < 0) {
                if (errno == EINTR) {
                    errno = 0;
                    continue;
                }
                ythrow yexception() << "epoll_wait failed with errno: " << errno;
            }

            for (int i = 0; i < n; ++i) {
                auto pipe = reinterpret_cast<IDataPipe*>(ev[i].data.ptr);
                if (!pipe->ProcessData(ev[i].events)) 
                    --ActivePipesCount;
            }
        }

        int status = 0;
        int ret = waitpid(ProcessId, &status, 0);
        if (ret < 0) {
            ythrow yexception() << "waitpid failed with errno: " << errno;
        }

        JobExitStatus = StatusToError(status);

        FOREACH(auto& pipe, DataPipes) {
            pipe->Finish();
        }

        SafeClose(efd);
    } catch (...) {
        // Try to close all pipes despite any other errors.
        // It is safe to call Finish multiple times.
        FOREACH(auto& pipe, DataPipes) {
            try {
                pipe->Finish();
            } catch (...) 
            {}
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
