#include "stdafx.h"

#include "config.h"
#include "job_proxy.h"


#include <ytlib/rpc/channel.h>
//#include <ytlib/misc/linux.h>

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

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

// ToDo(psushin): set sigint handler?
// ToDo(psushin): extract it to separate file.
TError StatusToError(int status)
{
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        return TError(Sprintf("Process terminated by signal %d",  WTERMSIG(status)));
    } else if (WIFSTOPPED(status)) {
        return TError(Sprintf("Process stopped by signal %d",  WSTOPSIG(status)));
    } else if (WIFEXITED(status)) {
        return TError(Sprintf("Process exited with value %d",  WEXITSTATUS(status)));
    } else {
        return TError(Sprintf("Status %d", status));
    }
}

#endif

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfig* config,
    const TJobId& jobId)
    : Config(config)
    , Proxy(~NRpc::CreateBusChannel(config->ExecAgentAddress))
    , JobId(jobId)
    , ProcessId(-1)
    , ActivePipesCount(0)
{
    Proxy.SetDefaultTimeout(config->RpcTimeout);
}

void TJobProxy::GetJobSpec()
{
    LOG_DEBUG("Requesting spec for job %s", ~JobId.ToString());
    auto req = Proxy.GetJobSpec();
    req->set_job_id(JobId.ToProto());

    auto rsp = req->Invoke()->Get();

    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Failed to get job spec (JobId: %s, Error: %s)",
            ~JobId.ToString(),
            ~rsp->GetError().ToString());
    }

    JobSpec = new TJobSpec(
        ~Config->JobIo, 
        ~Config->Masters,
        rsp->job_spec());
}

#ifdef _linux_

void TJobProxy::InitPipes()
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
        JobSpec->GetInputCount(), JobSpec->GetOutputCount()) * 3;

    YASSERT(maxReservedDescriptor > 0);

    // To avoid descriptor collisions between pipes on this, proxy side,
    // and "standard" descriptor numbers in forked job (see comments above) 
    // we ensure that lower 3 * N descriptors are allocated before creating pipes.

    std::vector<int> reservedDescriptors;
    do {
        reservedDescriptors.push_back(SafeDup(STDIN_FILENO));
    } while (reservedDescriptors.back() < maxReservedDescriptor);

    
    DataPipes.push_back(New<TOutputPipe>(JobSpec->GetErrorOutput(), STDERR_FILENO));
    ++ActivePipesCount;

    // Make pipe for each input and each output table.
    for (int i = 0; i < JobSpec->GetInputCount(); ++i) {
        TAutoPtr<TBlobOutput> buffer(new TBlobOutput());
        DataPipes.push_back(New<TInputPipe>(
            JobSpec->GetInputTable(i, buffer.Get()) ,
            buffer,
            3 * i));
    }

    for (int i = 0; i < JobSpec->GetOutputCount(); ++i) {
        ++ActivePipesCount;
        DataPipes.push_back(New<TOutputPipe>(~JobSpec->GetOutputTable(i), 3 * i + 1));
    }

    // Close reserved descriptors.
    FOREACH(auto& fd, reservedDescriptors) {
        SafeClose(fd);
    }

}

void TJobProxy::StartJob()
{
    try {
        FOREACH(auto& pipe, DataPipes) {
            pipe->PrepareJobDescriptors();
        }

        // ToDo(psushin): handle errors.
        ChDir(Config->SandboxName);

        // do not search the PATH, inherit environment
        execl("/bin/sh",
            "/bin/sh", 
            "-c", 
            ~JobSpec->GetShellCommand(), 
            (void*)NULL);
        int _errno = errno;

        fprintf(stderr, "Failed to exec job (/bin/sh -c '%s'): %s\n",
            ~JobSpec->GetShellCommand(),
            strerror(_errno)
            );

        exit(7);
    }
    catch (const std::exception& e) {
        ::write(STDERR_FILENO, e.what(), strlen(e.what()));
    }
    // catch (...) {
    //     // Use some exotic error codes here to analyze errors later.
    //     exit(6);
    // }
}

void TJobProxy::Run()
{
    LOG_DEBUG("Running new job proxy (JobId: %s)", ~JobId.ToString());

    try {
        GetJobSpec();

        LOG_DEBUG("Received job spec (JobId: %s)", ~JobId.ToString());

        InitPipes();

        LOG_DEBUG("Initialized pipes (JobId: %s)", ~JobId.ToString());

        ProcessId = fork();
        if (ProcessId < 0)
            ythrow yexception() << "fork failed with errno: " << errno;

        if (ProcessId == 0) {
            // Child process.
            StartJob();
            YUNREACHABLE();
        }

        LOG_DEBUG("Job process started, begin IO (JobId: %s)", ~JobId.ToString());

        DoJobIO();

        LOG_DEBUG("Job successfully finished (JobId: %s)", ~JobId.ToString());

        {
            NScheduler::NProto::TJobResult result;
            *result.mutable_error() = JobExitStatus.ToProto();
            ReportResult(result);
        }

    } catch (yexception& ex) {
        // ToDo: logging.
        LOG_DEBUG("Job failed (JobId: %s, error: %s)", 
            ~JobId.ToString(),
            ex.what());

        NScheduler::NProto::TJobResult result;
        result.mutable_error()->set_code(TError::Fail);
        result.mutable_error()->set_message(ex.what());

        ReportResult(result);
    }
}

void TJobProxy::ReportResult(
    const NScheduler::NProto::TJobResult& result)
{
    auto req = Proxy.OnJobFinished();
    *(req->mutable_result()) = result;
    *(req->mutable_job_id()) = JobId.ToProto();

    auto rsp = req->Invoke()->Get();
    if (!rsp->IsOK()) {
        // log error, use some exotic exit status.
        exit(1);
    }
}

void TJobProxy::DoJobIO()
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

#elif defined _win_

// Streaming jobs are not supposed to work on windows for now.

void TJobProxy::Run()
{
    YUNIMPLEMENTED();
}

void TJobProxy::InitPipes()
{
    YUNIMPLEMENTED();
}

void TJobProxy::DoJobIO()
{
    YUNIMPLEMENTED();
}

void TJobProxy::StartJob()
{
    YUNIMPLEMENTED();
}


#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
