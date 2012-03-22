#include "stdafx.h"
#include "unsafe_environment.h"
#include "environment.h"
#include "private.h"

// TODO(babenko): remove this ASAP
#include <ytlib/actions/action_util.h>

#include <ytlib/misc/thread_affinity.h>

#ifndef _win_

#include <sys/types.h>
#include <sys/wait.h>

#endif

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

namespace {

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

} // namespace <anonymous>

class TUnsafeProxyController
    : public IProxyController
{
public:
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        Stroka ProxyPath;
        Stroka ProxyConfigPath;

        TConfig() 
        {
            Register("proxy_path", ProxyPath).NonEmpty();
            Register("proxy_config_path", ProxyConfigPath).NonEmpty();
        }

        // i64 MemoryLimit;
    };

    TUnsafeProxyController(
        TConfig* config,
        const TJobId& jobId,
        const Stroka& workingDirectory)
        : Config(config)
        , WorkingDirectory(workingDirectory)
        , JobId(jobId)
        , ProcessId(-1)
        , OnExit(New< TFuture<TError> >())
        , ControllerThread(ThreadFunc, this)
    {
        //VERIFY_THREAD_AFFINITY(JobThread);
    }

    void Run() 
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ProcessId = fork();
        if (ProcessId == 0) {
            // ToDo: pass errors to parent process
            // cause logging doesn't work here.
            // Use unnamed pipes with CLOEXEC.
            
            ChDir(WorkingDirectory);

            // separate process group for that job - required in non-container mode only
            setpgid(0, 0); 

            // redirect stderr and stdout to file
            int fd = open("stderr.txt", O_WRONLY | O_CREAT);
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);

            // search the PATH, inherit environment
            execlp(
                ~Config->ProxyPath, 
                ~Config->ProxyPath, 
                "--job-proxy", 
                "--config", ~Config->ProxyConfigPath,
                "--operation-id", ~JobId.OperationId.ToString(),
                "--job-index", ~ToString(JobId.JobIndex),
                (void*)NULL);
            int _errno = errno;

            fprintf(stderr, "Failed to exec job-proxy (%s --job-proxy --config %s --operation-id %s --job-index %s): %s\n",
                ~Config->ProxyPath,
                ~Config->ProxyConfigPath,
                ~JobId.OperationId.ToString(),
                ~ToString(JobId.JobIndex),
                strerror(_errno)
                );

            exit(7);
            //YUNREACHABLE();
        }

        if (ProcessId < 0) {
            ythrow yexception() << Sprintf(
                "Failed to start job proxy: fork failed. pid: %d", 
                ProcessId);
        }

        LOG_DEBUG("Started job-proxy in unsafe environment (JobId: %s, working directory: %s)", 
            ~JobId.ToString(),
            ~WorkingDirectory);


        ControllerThread.Start();
        ControllerThread.Detach();
    }

    void Kill(const TError& error) 
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        
        LOG_DEBUG("Killing job, error: %s", ~error.GetMessage());

        
        SetError(error);

        if (ProcessId < 0)
            return;

        auto res = killpg(ProcessId, 9);

        if (res != 0) {
            if (errno == ESRCH)
                // Process group doesn't exist already.
                return;
            else
                ythrow yexception() << Sprintf(
                    "Failed to kill job - killpg failed (errno: %d)",
                    errno);
        }
    }

    void SubscribeOnExit(IParamAction<TError>* callback) 
    {
        OnExit->Subscribe(callback);
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
        auto* controller = (TUnsafeProxyController*) param;
        controller->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        int status = 0;
        {
            int res = waitpid(ProcessId, &status, WUNTRACED);
            if (res < 0) {
                SetError(TError(Sprintf(
                    "waitpid failed with errno: %d", 
                    errno)));
            }

            YASSERT(res == ProcessId);
        }
        
        LOG_DEBUG("Job-proxy finished (JobId: %s)", ~JobId.ToString());

        TError statusInfo = StatusToError(status);
        SetError(TError(statusInfo.GetCode(), Sprintf(
            "Job proxy exited (JobId: %s, status: %s)",
            ~JobId.ToString(),
            ~statusInfo.GetMessage())));

        {
            // Kill process group for sanity reasons.
            auto res = killpg(ProcessId, 9);

            if (res != 0 && errno != ESRCH) {
                SetError(TError(Sprintf(
                    "Failed to clean up job process group (errno: %d)",
                    errno)));
            }
        }

        OnExit->Set(Error);
    }

    TConfig::TPtr Config;

    const Stroka WorkingDirectory;
    TJobId JobId;

    int ProcessId;

    TSpinLock SpinLock;
    TError Error;

    TFuture<TError>::TPtr OnExit;

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
        : JobId(jobId)
        , ControllerThread(ThreadFunc, this)
    { }

    void Run() 
    {
        ControllerThread.Start();
        ControllerThread.Detach();

        LOG_DEBUG("Run job /dummy stub/ (JobId: %s)", ~JobId.ToString());
    }

    void Kill(const TError& error) 
    {
        LOG_DEBUG("Kill job /dummy stub/ (JobId: %s)", ~JobId.ToString());
        OnExit->Get();
    }

    void SubscribeExited(const TCallback<void(TError)>& callback) 
    {
        OnExit->Subscribe(FromCallback(callback));
    }

    void UnsubscribeExited(const TCallback<void(TError)>& callback) 
    {
        YUNIMPLEMENTED();
    }

private:
    static void* ThreadFunc(void* param)
    {
        auto* controller = (TUnsafeProxyController*) param;
        controller->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        Sleep(TDuration::Seconds(5));
        LOG_DEBUG("Job finished (JobId: %s)", ~JobId.ToString());

        OnExit->Set(TError("This is dummy job!"));
    }

    TJobId JobId;
    TFuture<TError>::TPtr OnExit;
    TThread ControllerThread;
};

#endif

////////////////////////////////////////////////////////////////////////////////

class TUnsafeEnvironmentBuilder
    : public IEnvironmentBuilder
{
public:
    TAutoPtr<IProxyController> TUnsafeEnvironmentBuilder::CreateProxyController(
        NYTree::INodePtr configuration, 
        const TJobId& jobId, 
        const Stroka& workingDirectory)
    {
#ifndef _win_
        auto config = New<TUnsafeProxyController::TConfig>();
        config->Load(configuration);

        return new TUnsafeProxyController(~config, jobId, workingDirectory);
#else
        return new TUnsafeProxyController(jobId);
#endif
    }
};

IEnvironmentBuilderPtr CreateUnsafeEnvironmentBuilder()
{
    return New<TUnsafeEnvironmentBuilder>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
