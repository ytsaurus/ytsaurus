#include "stdafx.h"
#include "unsafe_environment.h"
#include "environment.h"
#include "private.h"

#include <server/exec_agent/slot.h>

#include <core/concurrency/thread_affinity.h>

#include <core/misc/proc.h>

#include <core/logging/tagged_logger.h>

#include <server/job_proxy/public.h>

#include <util/system/execpath.h>

#include <fcntl.h>

#ifndef _win_

#include <sys/types.h>
#include <sys/wait.h>

#endif

namespace NYT {
namespace NExecAgent {

using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ExecAgentLogger;

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
        const TSlot& slot,
        const Stroka& workingDirectory) override;

private:
    friend class TUnsafeProxyController;

    Stroka ProxyPath;
};

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

class TUnsafeProxyController
    : public IProxyController
{
public:
    TUnsafeProxyController(
        const Stroka& proxyPath,
        const TJobId& jobId,
        const TSlot& slot,
        const Stroka& workingDirectory,
        TUnsafeEnvironmentBuilder* envBuilder)
        : ProxyPath(proxyPath)
        , WorkingDirectory(workingDirectory)
        , JobId(jobId)
        , Slot(slot)
        , Logger(ExecAgentLogger)
        , ProcessId(-1)
        , EnvironmentBuilder(envBuilder)
        , OnExit(NewPromise<TError>())
        , ControllerThread(ThreadFunc, this)
    {
        Logger.AddTag(Sprintf("JobId: %s", ~ToString(jobId)));
    }

    TAsyncError Run()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        LOG_INFO("Starting job proxy in unsafe environment (WorkDir: %s)",
            ~WorkingDirectory);

        std::vector<Stroka> arguments;
        arguments.push_back(ProxyPath);
        arguments.push_back("--job-proxy");
        arguments.push_back("--config");
        arguments.push_back(ProxyConfigFileName);
        arguments.push_back("--job-id");
        arguments.push_back(ToString(JobId));
        arguments.push_back("--cgroup");
        arguments.push_back(Slot.GetProcessGroup().GetFullPath());
        arguments.push_back("--working-dir");
        arguments.push_back(WorkingDirectory);
        arguments.push_back("--close-all-fds");

        LOG_INFO("Spawning a job proxy (Path: %s)", ~ProxyPath);

        try {
            ProcessId = Spawn(
                ~ProxyPath,
                arguments);
        } catch (const std::exception& ex) {
            // Failed to exec job proxy
            THROW_ERROR_EXCEPTION("Failed to start job proxy: Spawn failed")
                << ex
                << TError::FromSystem();
        }

        if (ProcessId < 0) {
            THROW_ERROR_EXCEPTION("Failed to start job proxy: fork failed")
                << TError::FromSystem();
        }

        LOG_INFO("Job proxy started (ProcessId: %d)",
            ProcessId);

        // Unref is called in the thread.
        Ref();
        ControllerThread.Start();

        ControllerThread.Detach();

        return OnExit;
    }

    // Safe to call multiple times
    void Kill(const NCGroup::TNonOwningCGroup& group, const TError& error) throw()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        LOG_INFO(error, "Killing job in unsafe environment (ProcessGroup: %s)", ~group.GetFullPath().Quote());

        SetError(error);

        int pid = ProcessId;

        if (pid > 0) {
            auto result = kill(pid, 9);
            if (result != 0) {
                switch (errno) {
                    case ESRCH:
                        // Process group doesn't exist already.
                        return;
                    default:
                        LOG_FATAL("Failed to kill job proxy: kill failed (errno: %s)", strerror(errno));
                        break;
                }
            }
        }

        // Wait until job proxy finishes.
        OnExit.Get();

        try {
            KillAll(BIND(&NCGroup::TNonOwningCGroup::GetTasks, &group));
        } catch (const std::exception& ex) {
            LOG_FATAL(TError(ex));
        }

        LOG_INFO("Job killed");
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
        controller->Unref();
        controller->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        LOG_INFO("Waiting for job proxy to finish");

        int status = 0;
        {
            int pid = ProcessId;
            int result = waitpid(pid, &status, WUNTRACED);

            // Set ProcessId back to -1, so that we don't try to kill it ever after.
            ProcessId = -1;
            if (result < 0) {
                SetError(TError("Failed to wait for job proxy to finish: waitpid failed")
                    << TError::FromSystem());
                OnExit.Set(Error);
                return;
            }
            YASSERT(result == pid);
        }

        auto statusError = StatusToError(status);
        auto wrappedError = statusError.IsOK()
            ? TError()
            : TError("Job proxy failed") << statusError;
        SetError(wrappedError);

        LOG_INFO(wrappedError, "Job proxy finished");

        OnExit.Set(Error);
    }


    const Stroka ProxyPath;
    const Stroka WorkingDirectory;
    const TJobId JobId;
    const TSlot& Slot;

    NLog::TTaggedLogger Logger;

    int ProcessId;
    TIntrusivePtr<TUnsafeEnvironmentBuilder> EnvironmentBuilder;

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
        Logger.AddTag(Sprintf("JobId: %s", ~ToString(jobId)));
    }

    TAsyncError Run()
    {
        ControllerThread.Start();
        ControllerThread.Detach();

        LOG_INFO("Running dummy job");

        return OnExit;
    }

    void Kill(int uid, const TError& error)
    {
        LOG_INFO("Killing dummy job");
        OnExit.Get();
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
    const TSlot& slot,
    const Stroka& workingDirectory)
{
#ifndef _win_
    return New<TUnsafeProxyController>(ProxyPath, jobId, slot, workingDirectory, this);
#else
    UNUSED(config);
    UNUSED(workingDirectory);
    UNUSED(slotId);
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
