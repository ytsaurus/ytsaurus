#include "unsafe_environment.h"
#include "private.h"
#include "environment.h"

#include <yt/server/exec_agent/slot.h>

#include <yt/server/job_proxy/public.h>

#include <yt/core/misc/process.h>
#include <yt/core/misc/proc.h>

#include <yt/core/tools/tools.h>

#include <util/system/execpath.h>

namespace NYT {
namespace NExecAgent {

using namespace NJobProxy;
using namespace NCGroup;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUnsafeEnvironmentBuilder)

class TUnsafeEnvironmentBuilder
    : public IEnvironmentBuilder
{
public:
    TUnsafeEnvironmentBuilder()
        : ProxyPath_(GetExecPath())
    { }

    virtual IProxyControllerPtr CreateProxyController(
        NYTree::INodePtr config,
        const TJobId& jobId,
        const TOperationId& operationId,
        TSlotPtr slot) override;

private:
    friend class TUnsafeProxyController;

    const Stroka ProxyPath_;
};

DEFINE_REFCOUNTED_TYPE(TUnsafeEnvironmentBuilder)

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

class TUnsafeProxyController
    : public IProxyController
{
public:
    TUnsafeProxyController(
        const Stroka& proxyPath,
        const TJobId& jobId,
        const TOperationId& operationId,
        TSlotPtr slot,
        TUnsafeEnvironmentBuilderPtr environmentBuilder)
        : ProxyPath_(proxyPath)
        , JobId_(jobId)
        , OperationId_(operationId)
        , Slot_(slot)
        , Process_(New<TProcess>(proxyPath))
        , EnvironmentBuilder_(environmentBuilder)
        , WaitingThread_(ThreadFunc, this)
    {
        Logger.AddTag("JobId: %v", jobId);
    }

    virtual TFuture<void> Run() override
    {
        LOG_INFO("Starting job proxy in unsafe environment (WorkDir: %v)",
            Slot_->GetWorkingDirectory());

        Process_->AddArguments({
            "--job-proxy",
            "--config", ProxyConfigFileName,
            "--job-id", ToString(JobId_),
            "--operation-id", ToString(OperationId_),
            "--working-dir", Slot_->GetWorkingDirectory()
        });

#ifdef _linux_
        for (const auto& path : Slot_->GetCGroupPaths()) {
            Process_->AddArguments({
                "--cgroup",
                path
            });
        }
#endif

        LOG_INFO("Spawning a job proxy (Path: %v)", ProxyPath_);

        try {
            ProcessFinished_ = Process_->Spawn();
        } catch (const std::exception& ex) {
            return MakeFuture(TError("Failed to spawn job pxoxy") << ex);
        }

        LOG_INFO("Job proxy started (ProcessId: %v)",
            Process_->GetProcessId());

        // Unref is called in the thread.
        Ref();
        WaitingThread_.Start();

        WaitingThread_.Detach();

        return ProxyExited_;
    }

    // Safe to call multiple times
    virtual void Kill(const TNonOwningCGroup& group) override
    {
        LOG_INFO("Killing job in unsafe environment (ProcessGroup: %v)", group.GetFullPath());

        // One can be certain that Spawn is already finished
        // before this line due to thread affinity.
        if (Process_->IsStarted() && !Process_->IsFinished()) {
            try {
                Process_->Kill(SIGKILL);
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Failed to kill job proxy: kill failed");
            }
        }

        // Wait until job proxy finishes.
        ProxyExited_.Get();

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

        auto spawnError = WaitFor(ProcessFinished_);
        auto jobProxyError = BuildJobProxyError(spawnError);
        if (jobProxyError.IsOK()) {
            LOG_INFO("Job proxy completed");
        } else {
            LOG_ERROR(jobProxyError);
        }
        ProxyExited_.Set(jobProxyError);
    }

    static TError BuildJobProxyError(const TError& spawnError)
    {
        if (spawnError.IsOK()) {
            return TError();
        }

        auto jobProxyError = TError("Job proxy failed") <<
            spawnError;

        if (spawnError.GetCode() == EProcessErrorCode::NonZeroExitCode) {
            // Try to translate the numeric exit code into some human readable reason.
            auto reason = EJobProxyExitCode(spawnError.Attributes().Get<int>("exit_code"));
            const auto& validReasons = TEnumTraits<EJobProxyExitCode>::GetDomainValues();
            if (std::find(validReasons.begin(), validReasons.end(), reason) != validReasons.end()) {
                jobProxyError.Attributes().Set("reason", reason);
            }
        }

        return jobProxyError;
    }


    const Stroka ProxyPath_;
    const TJobId JobId_;
    const TOperationId OperationId_;
    const TSlotPtr Slot_;

    const TProcessPtr Process_;
    const TUnsafeEnvironmentBuilderPtr EnvironmentBuilder_;

    TFuture<void> ProcessFinished_;

    TSpinLock SpinLock_;
    TError Error_;

    TPromise<void> ProxyExited_ = NewPromise<void>();

    TThread WaitingThread_;

    NLogging::TLogger Logger = ExecAgentLogger;
};

#else

//! Dummy stub for windows.
class TUnsafeProxyController
    : public IProxyController
{
public:
    TUnsafeProxyController(
        const TJobId& jobId,
        const TOperationId& operationId)
        : Logger(ExecAgentLogger)
        , WaitingThread(ThreadFunc, this)
    {
        Logger.AddTag("JobId: %v, OperationId: %v",
            jobId,
            operationId);
    }

    TFuture<void> Run()
    {
        WaitingThread.Start();
        WaitingThread.Detach();

        LOG_INFO("Running dummy job");

        return ProxyExited_;
    }

    virtual void Kill(const TNonOwningCGroup& group) override
    {
        LOG_INFO("Killing dummy job");
        ProxyExited_.Get();
    }

private:
    static void* ThreadFunc(void* param)
    {
        TIntrusivePtr<TUnsafeProxyController> controller(reinterpret_cast<TUnsafeProxyController*>(param));
        controller->ThreadMain();
        return nullptr;
    }

    void ThreadMain()
    {
        // We don't have jobs support for Windows.
        // Just wait for a couple of seconds and report the failure.
        // This might help with scheduler debugging under Windows.
        Sleep(TDuration::Seconds(5));
        LOG_INFO("Dummy job finished");
        ProxyExited_.Set(TError("Jobs are not supported under Windows"));
    }

    NLogging::TLogger Logger;
    TPromise<void> ProxyExited_ = NewPromise<void>();
    TThread WaitingThread_;
};

#endif

////////////////////////////////////////////////////////////////////////////////

IProxyControllerPtr TUnsafeEnvironmentBuilder::CreateProxyController(
    NYTree::INodePtr /*config*/,
    const TJobId& jobId,
    const TOperationId& operationId,
    TSlotPtr slot)
{
#ifdef _unix_
    return New<TUnsafeProxyController>(
        ProxyPath_,
        jobId,
        operationId,
        slot,
        this);
#else
    UNUSED(slot);
    return New<TUnsafeProxyController>(
        jobId,
        operationId);
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
