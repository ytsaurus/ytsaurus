#include "stdafx.h"
#include "unsafe_environment.h"
#include "environment.h"
#include "private.h"

#include <server/exec_agent/slot.h>

#include <server/job_proxy/public.h>

#include <core/concurrency/thread_affinity.h>

#include <core/misc/process.h>

#include <core/tools/tools.h>

#include <util/system/execpath.h>

namespace NYT {
namespace NExecAgent {

using namespace NJobProxy;
using namespace NCGroup;

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
        , Process(proxyPath)
        , EnvironmentBuilder(envBuilder)
        , OnExit(NewPromise<void>())
        , WaitingThread(ThreadFunc, this)
    {
        Logger.AddTag("JobId: %v", jobId);
    }

    virtual TFuture<void> Run() override
    {
        LOG_INFO("Starting job proxy in unsafe environment (WorkDir: %v)",
            WorkingDirectory);

        Process.AddArguments({
            "--job-proxy",
            "--config",
            ProxyConfigFileName,
            "--job-id",
            ToString(JobId),
            "--working-dir",
            WorkingDirectory
        });

        for (const auto& path : Slot.GetCGroupPaths()) {
            Process.AddArguments({
                "--cgroup",
                path
            });
        }

        LOG_INFO("Spawning a job proxy (Path: %v)", ProxyPath);

        try {
            Process.Spawn();
        } catch (const std::exception& ex) {
            return MakeFuture(TError("Failed to spawn job pxoxy") << ex);
        }

        LOG_INFO("Job proxy started (ProcessId: %v)",
            Process.GetProcessId());

        // Unref is called in the thread.
        Ref();
        WaitingThread.Start();

        WaitingThread.Detach();

        return OnExit;
    }

    // Safe to call multiple times
    virtual void Kill(const TNonOwningCGroup& group) override
    {
        LOG_INFO("Killing job in unsafe environment (ProcessGroup: %v)", group.GetFullPath());

        // One certaily can say that Process.Spawn exited
        // before this line due to thread affinity
        if (Process.Started()) {
            try {
                Process.Kill(9);
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Failed to kill job proxy: kill failed");
            }
        }

        // Wait until job proxy finishes.
        OnExit.Get();

        try {
            RunKiller(group.GetFullPath());
        } catch (const std::exception& ex) {
            LOG_FATAL(TError(ex));
        }

        LOG_INFO("Job killed");
    }

private:
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

        auto error = Process.Wait();
        LOG_INFO(error, "Job proxy finished");

        if (!error.IsOK()) {
            error = TError("Job proxy failed") << error;
        }

        OnExit.Set(error);
    }

    const Stroka ProxyPath;
    const Stroka WorkingDirectory;
    const TJobId JobId;
    const TSlot& Slot;

    NLogging::TLogger Logger;

    TProcess Process;
    TIntrusivePtr<TUnsafeEnvironmentBuilder> EnvironmentBuilder;

    TSpinLock SpinLock;
    TError Error;

    TPromise<void> OnExit;

    TThread WaitingThread;
};

#else

//! Dummy stub for windows.
class TUnsafeProxyController
    : public IProxyController
{
public:
    explicit TUnsafeProxyController(const TJobId& jobId)
        : Logger(ExecAgentLogger)
        , OnExit(NewPromise<void>())
        , WaitingThread(ThreadFunc, this)
    {
        Logger.AddTag("JobId: %v", jobId);
    }

    TFuture<void> Run()
    {
        WaitingThread.Start();
        WaitingThread.Detach();

        LOG_INFO("Running dummy job");

        return OnExit;
    }

    virtual void Kill(const TNonOwningCGroup& group) override
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

    NLogging::TLogger Logger;
    TPromise<void> OnExit;
    TThread WaitingThread;
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
    UNUSED(slot);
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
