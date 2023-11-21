#include "job_proxy.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <util/system/thread.h>

namespace NYT::NJobProxy {

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProxyProgram
    : public TProgram
    , public TProgramConfigMixin<TJobProxyConfig>
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
            .AddLongOption("operation-id", "operation id")
            .StoreMappedResultT<TString>(&OperationId_, &CheckGuidArgMapper)
            .RequiredArgument("ID");
        Opts_
            .AddLongOption("job-id", "job id")
            .StoreMappedResultT<TString>(&JobId_, &CheckGuidArgMapper)
            .RequiredArgument("ID");
        Opts_
            .AddLongOption("stderr-path", "stderr path")
            .StoreResult(&StderrPath_)
            .Optional();
        Opts_
            .AddLongOption("do-not-close-descriptors", "disable machinery to close desctiptors on startup")
            .NoArgument()
            .SetFlag(&DoNotCloseDescriptors_)
            .Optional();
    }

protected:
    bool ShouldAbortOnHungShutdown() noexcept override
    {
        return false;
    }

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
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
        ConfigureAllocator({
            .TCMallocOptimizeSize = true,
        });

        try {
            SafeCreateStderrFile(StderrPath_);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Job proxy preparation (startup) failed");
            Exit(static_cast<int>(NJobProxy::EJobProxyExitCode::JobProxyPrepareFailed));
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        auto jobProxy = New<TJobProxy>(std::move(config), OperationId_, JobId_);
        jobProxy->Run();

        // Everything should be properly destructed.
        if (auto residualRefCount = ResetAndGetResidualRefCount(jobProxy)) {
            YT_LOG_ERROR("Job proxy ref counter is positive at the end of job; memory leak is possible "
                "(RefCounter: %v)",
                residualRefCount);
        }

#ifdef _asan_enabled_
        // TODO(babenko): fix leaks.
        Abort(EProgramExitCode::OK);
#endif
    }

private:
    NJobTrackerClient::TOperationId OperationId_;
    NJobTrackerClient::TJobId JobId_;
    TString StderrPath_ = "stderr";
    bool DoNotCloseDescriptors_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
