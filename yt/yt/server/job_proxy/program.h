#include "job_proxy.h"
#include "private.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_cgroup_mixin.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/program_setsid_mixin.h>
#include <yt/ytlib/program/helpers.h>

#include <yt/server/lib/job_proxy/config.h>

#include <yt/core/misc/proc.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/core/ytalloc/bindings.h>

namespace NYT::NJobProxy {

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProxyProgram
    : public TProgram
    , public TProgramConfigMixin<TJobProxyConfig>
    , public TProgramCgroupMixin
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
{
public:
    TJobProxyProgram()
        : TProgramConfigMixin(Opts_, false)
        , TProgramCgroupMixin(Opts_)
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
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("JobProxyMain");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        CloseAllDescriptors();
        NYTAlloc::EnableYTLogging();
        NYTAlloc::InitializeLibunwindInterop();
        NYTAlloc::ConfigureFromEnv();

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

        // TODO(gritukan): Remove it after TC death.
        if (NYTree::ConvertTo<NExecAgent::TJobEnvironmentConfigPtr>(config->JobEnvironment)->Type == NExecAgent::EJobEnvironmentType::Porto) {
            if (!TrySetUid(0)) {
                YT_LOG_WARNING("Failed to become root. Multiple problems with Porto environment might occur");
            }
        }

        ConfigureSingletons(config);
        StartDiagnosticDump(config);

        if (HandleCgroupOptions()) {
            return;
        }

        // NB: There are some cyclic references here:
        // JobProxy <-> Job
        // JobProxy <-> JobProberService
        // But we (currently) don't care.
        auto jobProxy = New<TJobProxy>(std::move(config), OperationId_, JobId_);
        jobProxy->Run();
    }

private:
    NJobTrackerClient::TOperationId OperationId_;
    NJobTrackerClient::TJobId JobId_;
    TString StderrPath_ = "stderr";
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
