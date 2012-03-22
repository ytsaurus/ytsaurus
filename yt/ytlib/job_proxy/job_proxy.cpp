#include "stdafx.h"

#include "job_proxy.h"
#include <ytlib/rpc/channel.h>
#include <ytlib/misc/waitpid.h>

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

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static int set_sigpipe()
{
    struct sigaction act;

    sigemptyset(&act.sa_mask);
    sigaddset(&act.sa_mask, SIGPIPE);

    act.sa_flags = 0;
    act.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &act, 0);
}

static int dummy = set_sigpipe();

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TConfig* config,
    const TOperationId& operationId, 
    const int jobIndex)
    : Config(config)
    , Proxy(~NRpc::CreateBusChannel(config->SupervisorServiceAddress))
    , JobId(operationId, jobIndex)
    , ProcessId(-1)
    , ActivePipesCount(0)
{
    Proxy.SetDefaultTimeout(config->RpcTimeout);
}

void TJobProxy::GetJobSpec()
{
    LOG_DEBUG("Requesting spec for job %s", ~JobId.ToString());
    auto req = Proxy.GetJobSpec();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke()->Get();

    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Failed to get job spec (JobId: %s, Error: %s)",
            ~JobId.ToString(),
            ~rsp->GetError().ToString());
    }

    auto txId = TTransactionId::FromProto(rsp->job_spec().transaction_id());

    JobSpec = new TJobSpec(
        ~Config->JobSpec, 
        rsp->job_spec(), 
        txId);
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

    yvector<int> reservedDescriptors;
    do {
        reservedDescriptors.push_back(SafeDup(STDIN_FILENO));
    } while (reservedDescriptors.back() < maxReservedDescriptor);

    DataPipes.push_back(New<TErrorPipe>(JobSpec->GetErrorOutput()));
    ++ActivePipesCount;

    // Make pipe for each input and each output table.
    for (int i = 0; i < JobSpec->GetInputCount(); ++i) {
        DataPipes.push_back(New<TInputPipe>(JobSpec->GetTableInput(i), 3 * i));
    }

    for (int i = 0; i < JobSpec->GetOutputCount(); ++i) {
        ++ActivePipesCount;
        DataPipes.push_back(New<TOutputPipe>(~JobSpec->GetTableOutput(i), 3 * i + 1));
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
    catch (const yexception& e) {
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
            if (JobExitStatus.IsOK()) {
                result.set_is_ok(true);
            } else {
                result.set_is_ok(false);
                result.set_error_message(JobExitStatus.GetMessage());
            }
            ReportResult(result);
        }

    } catch (yexception& ex) {
        // ToDo: logging.
        LOG_DEBUG("Job failed (JobId: %s, error: %s)", 
            ~JobId.ToString(),
            ex.what());

        NScheduler::NProto::TJobResult result;
        result.set_is_ok(false);
        result.set_error_message(ex.what());

        ReportResult(result);
    }
}

void TJobProxy::ReportResult(
    const NScheduler::NProto::TJobResult& result)
{
    auto req = Proxy.OnJobFinished();
    *(req->mutable_job_result()) = result;
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

        JobExitStatus = GetStatusInfo(status);

        FOREACH(auto& pipe, DataPipes) {
            pipe->Finish();
        }

        ::close(efd);
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

void TJobProxy::ReportStatistic() 
{
    // Later.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
