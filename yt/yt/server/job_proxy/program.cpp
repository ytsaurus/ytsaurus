#include "program.h"

#include "job_proxy.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/library/signals/signal_blocking.h>

#include <library/cpp/yt/system/exit.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NJobProxy {

constinit const auto Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

YT_TRY_BLOCK_SIGNAL_FOR_PROCESS(SIGRTMIN, [] (bool ok, int threadCount) {
        // NB(pogorelov): In some cases (for example, when using mac os and docker with colima),
        // there may be strange threads at the application start, which we can not send signals to.
        //
        // YT_LOG_INFO("Thread count is not 1, trying to get thread infos (ThreadCount: %v)", threadCount);
        // auto threadInfos = IntrospectThreads();
        // auto descripion = FormatIntrospectionInfos(threadInfos);
        // AbortProcessDramatically(
        //     EProcessExitCode::GenericError,
        //     Format(
        //         "Thread count is not 1, threadCount: %v, threadInfos: %v",
        //         threadCount,
        //         descripion));

        if (!ok) {
            YT_LOG_WARNING(
                "Failed to block SIGRTMIN for process: thread count is not 1 at the process start; delivery fenced write will be disabled (ThreadCount: %v)",
                threadCount);
        }
        DeliveyFencedWriteEnabled = ok;
    });

class TJobProxyProgram
    : public virtual TProgram
    , public TProgramConfigMixin<TJobProxyInternalConfig>
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
{
public:
    TJobProxyProgram()
        : TProgramConfigMixin(Opts_, false)
        , TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
    {
        Opts_
            .AddLongOption(
                "operation-id",
                "Operation id")
            .StoreMappedResultT<TStringBuf>(&OperationId_, &TJobProxyProgram::OperationIdArgMapper)
            .RequiredArgument("ID");
        Opts_
            .AddLongOption(
                "job-id",
                "Job id")
            .StoreMappedResultT<TStringBuf>(&JobId_, &TJobProxyProgram::JobIdArgMapper)
            .RequiredArgument("ID");
        Opts_
            .AddLongOption(
                "stderr-path",
                "Stderr path")
            .StoreResult(&StderrPath_)
            .Optional();
        Opts_
            .AddLongOption(
                "do-not-close-descriptors",
                "Do not close descriptors on startup")
            .NoArgument()
            .SetFlag(&DoNotCloseDescriptors_)
            .Optional();
    }

protected:
    bool ShouldAbortOnHungShutdown() noexcept override
    {
        return false;
    }

    void DoRun() override
    {
        TThread::SetCurrentThreadName("JobProxyMain");

        if (!DoNotCloseDescriptors_) {
            CloseAllDescriptors();
        }
        EnableShutdownLoggingToStderr();
        ConfigureUids();
        ConfigureIgnoreSigpipe();
        EnablePhdrCache();
        ConfigureCrashHandler();
        ConfigureAllocator();
        MlockFileMappings();

        try {
            NFS::MakeDirRecursive(NFS::GetDirectoryName(StderrPath_));
            SafeCreateStderrFile(StderrPath_);
        } catch (const std::exception& ex) {
            Exit(NJobProxy::EJobProxyExitCode::JobProxyPrepareFailed);
        }

        RunMixinCallbacks();

        auto config = GetConfig();

        ConfigureSingletons(config);

        auto jobProxy = New<TJobProxy>(std::move(config), OperationId_, JobId_);
        jobProxy->Run();

        // Everything should be properly destructed.
        if (auto residualRefCount = ResetAndGetResidualRefCount(jobProxy)) {
            YT_LOG_ERROR(
                "Job proxy ref counter is positive at the end of job; memory leak is possible (RefCounter: %v)",
                residualRefCount);
        }

#ifdef _asan_enabled_
        // TODO(babenko): fix leaks.
        Abort(ToUnderlying(EProcessExitCode::OK));
#endif
    }

private:
    NJobTrackerClient::TOperationId OperationId_;
    NJobTrackerClient::TJobId JobId_;
    TString StderrPath_ = "stderr";
    bool DoNotCloseDescriptors_ = false;

    static NJobTrackerClient::TJobId JobIdArgMapper(TStringBuf arg)
    {
        return NJobTrackerClient::TJobId(FromStringArgMapper<NJobTrackerClient::TJobId::TUnderlying>(arg));
    }

    static NJobTrackerClient::TOperationId OperationIdArgMapper(TStringBuf arg)
    {
        return NJobTrackerClient::TOperationId(FromStringArgMapper<NJobTrackerClient::TOperationId::TUnderlying>(arg));
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunJobProxyProgram(int argc, const char** argv)
{
    TJobProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
