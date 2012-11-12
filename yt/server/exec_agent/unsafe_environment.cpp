#include "stdafx.h"
#include "unsafe_environment.h"
#include "environment.h"
#include "private.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>

#include <util/system/execpath.h>
#include <util/folder/dirut.h>

#include <fcntl.h>

#ifndef _win_

#include <sys/types.h>
#include <sys/wait.h>

#endif

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TUnsafeEnvironmentBuilder
    : public IEnvironmentBuilder
{
public:
    TUnsafeEnvironmentBuilder()
        : ProxyPath(GetExecPath())
    { }

    IProxyControllerPtr CreateProxyController(
        NYTree::INodePtr config, 
        const TJobId& jobId, 
        const Stroka& workingDirectory) override;

private:
    friend class TUnsafeProxyController;

    Stroka ProxyPath;
    TMutex ForkMutex;
};

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

namespace {

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
        return TError("Unknown status %d", status);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TUnsafeProxyController
    : public IProxyController
{
public:
    TUnsafeProxyController(
        const Stroka& proxyPath,
        const TJobId& jobId,
        const Stroka& workingDirectory,
        TUnsafeEnvironmentBuilder* envBuilder)
        : ProxyPath(proxyPath)
        , WorkingDirectory(workingDirectory)
        , JobId(jobId)
        , Logger(ExecAgentLogger)
        , ProcessId(-1)
        , EnvironmentBuilder(envBuilder)
        , OnExit(NewPromise<TError>())
        , ControllerThread(ThreadFunc, this)
    {
        Logger.AddTag(Sprintf("JobId: %s", ~jobId.ToString()));

        // We acquire this mutex second time from Run()
        // and wait on it until waitpid thread actually starts.
        WaitpidMutex.Acquire();
    }

    void Run() 
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        LOG_INFO("Starting job proxy in unsafe environment (WorkDir: %s)", 
            ~WorkingDirectory);

        //ToDo(psushin): Remove this mutex when libc is fixed.
        EnvironmentBuilder->ForkMutex.Acquire();
        ProcessId = fork();

        if (ProcessId == 0) {
            // ToDo(psushin): pass errors to parent process
            // cause logging doesn't work here.
            // Use unnamed pipes with CLOEXEC.

            ChDir(WorkingDirectory);

            // Redirect stderr and stdout to a file.
            int fd = open("stderr.txt", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);

            // Separate process group for that job - required in non-container mode only.
            setpgid(0, 0); 

            // Search the PATH, inherit environment.
            execlp(
                ~ProxyPath, 
                ~ProxyPath, 
                "--job-proxy", 
                "--config", ~ProxyConfigFileName,
                "--job-id", ~JobId.ToString(),
                (void*) NULL);

            fprintf(stderr, "Failed to exec job proxy (ProxyPath: %s, ProxyConfig: %s, JobId: %s, Error: %s)\n",
                ~ProxyPath,
                ~ProxyConfigFileName,
                ~JobId.ToString(),
                strerror(errno));

            // TODO(babenko): use some meaningful constant
            _exit(7);
        }

        EnvironmentBuilder->ForkMutex.Release();

        if (ProcessId < 0) {
            THROW_ERROR_EXCEPTION("Failed to start job proxy: fork failed")
                << TError::FromSystem();
        }

        LOG_INFO("Job proxy started (ProcessId: %d)",
            ProcessId);

        ControllerThread.Start();

        // See comment in ctor.
        WaitpidMutex.Acquire();

        ControllerThread.Detach();
    }

    void Kill(const TError& error) throw() 
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        
        LOG_INFO(error, "Killing job");

        SetError(error);

        if (ProcessId < 0)
            return;

        auto result = killpg(ProcessId, 9);
        if (result != 0) {
            switch (errno) {
                case ESRCH:
                    // Process group doesn't exist already.
                    return;
                default:
                    LOG_FATAL("Failed to kill job: killpg failed (errno: %s)", strerror(errno));
                    break;
            }
        }

        LOG_INFO("Job killed");
    }

    void SubscribeExited(const TCallback<void(TError)>& callback)
    {
        OnExit.Subscribe(callback);
    }

    void UnsubscribeExited(const TCallback<void(TError)>& callback) 
    {
        YUNIMPLEMENTED();
    }

private:
    void SetError(const TError& error)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (Error.IsOK()) {
            Error = error;
        }
    }

    static void* ThreadFunc(void* param)
    {
        auto controller = MakeStrong(static_cast<TUnsafeProxyController*>(param));
        controller->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        WaitpidMutex.Release();
 
        LOG_INFO("Waiting for job proxy to finish");

        int status = 0;
        {
            int result = waitpid(ProcessId, &status, WUNTRACED);
            if (result < 0) {
                SetError(TError("Failed to wait for job proxy to finish: waitpid failed")
                    << TError::FromSystem());
                OnExit.Set(Error);
                return;
            }
            YASSERT(result == ProcessId);
        }

        auto statusError = StatusToError(status);
        auto wrappedError = statusError.IsOK()
            ? TError()
            : TError("Job proxy failed") << statusError;
        SetError(wrappedError);

        LOG_INFO(wrappedError, "Job proxy finished");

        {
            // Kill process group for sanity reasons.
            auto result = killpg(ProcessId, 9);
            if (result != 0 && errno != ESRCH) {
                SetError(TError("Failed to clean up job process group: killpg failed")
                    << TError::FromSystem());
            }
        }

        OnExit.Set(Error);
    }


    const Stroka ProxyPath;
    const Stroka WorkingDirectory;
    const TJobId JobId;

    NLog::TTaggedLogger Logger;

    int ProcessId;
    TIntrusivePtr<TUnsafeEnvironmentBuilder> EnvironmentBuilder;

    TMutex WaitpidMutex;

    TSpinLock SpinLock;
    TError Error;

    TPromise<TError> OnExit;

    TThread ControllerThread;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

#else

//! Dummy stub for windows.
class TUnsafeProxyController
    : public IProxyController
{
public:
    TUnsafeProxyController(const TJobId& jobId)
        : Logger(ExecAgentLogger)
        , OnExit(NewPromise<TError>())
        , ControllerThread(ThreadFunc, this)
    {
        Logger.AddTag(Sprintf("JobId: %s", ~jobId.ToString()));
    }

    void Run() 
    {
        ControllerThread.Start();
        ControllerThread.Detach();

        LOG_INFO("Running dummy job");
    }

    void Kill(const TError& error) 
    {
        LOG_INFO("Killing dummy job");
        OnExit.Get();
    }

    void SubscribeExited(const TCallback<void(TError)>& callback) 
    {
        OnExit.Subscribe(callback);
    }

    void UnsubscribeExited(const TCallback<void(TError)>& callback) 
    {
        YUNIMPLEMENTED();
    }

private:
    static void* ThreadFunc(void* param)
    {
        TIntrusivePtr<TUnsafeProxyController> controller(reinterpret_cast<TUnsafeProxyController*>(param));
        controller->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        // We don't have jobs support for Windows.
        // Just wait for a couple of seconds and report the failure.
        // This might help with scheduler debugging under Windows.
        Sleep(TDuration::Seconds(5));
        LOG_INFO("Dummy job finished");
        OnExit.Set(TError("Jobs are not supported under Windows"));
    }

    NLog::TTaggedLogger Logger;
    TPromise<TError> OnExit;
    TThread ControllerThread;
};

#endif

////////////////////////////////////////////////////////////////////////////////

IProxyControllerPtr TUnsafeEnvironmentBuilder::CreateProxyController(
    NYTree::INodePtr config, 
    const TJobId& jobId, 
    const Stroka& workingDirectory)
{
#ifndef _win_
    return New<TUnsafeProxyController>(ProxyPath, jobId, workingDirectory, this);
#else
    UNUSED(config);
    UNUSED(workingDirectory);
    return New<TUnsafeProxyController>(jobId);
#endif
}

////////////////////////////////////////////////////////////////////////////////

IEnvironmentBuilderPtr CreateUnsafeEnvironmentBuilder()
{
    return New<TUnsafeEnvironmentBuilder>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
