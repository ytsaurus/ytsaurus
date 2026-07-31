#include "companion_process_manager.h"

#include "companion_entrypoint.h"
#include "config.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/library/process/process.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

TCompanionProcessManager::TCompanionProcessManager(
    const IInvokerPtr& invoker,
    const ICompanionClientPtr& companionClient,
    const TExponentialBackoffOptions& backoffOptions,
    const TDuration restartDelay,
    const TDuration healthCheckInterval,
    const TDuration startupGracePeriod,
    const TDuration metricsCollectionInterval,
    const NLogging::TLogger logger,
    const NProfiling::TProfiler profiler,
    const IStatusProfilerPtr& statusProfiler,
    const TCompanionEntrypointPtr& entrypoint)
    : TProcessManagerBase(invoker, companionClient, backoffOptions, restartDelay, healthCheckInterval, startupGracePeriod, metricsCollectionInterval, logger, profiler, statusProfiler)
    , Entrypoint_(entrypoint)
{ }

////////////////////////////////////////////////////////////////////////////////

void TCompanionProcessManager::ValidateParameters() const
{
    THROW_ERROR_EXCEPTION_UNLESS(Entrypoint_,
        "Companion entrypoint is not configured");
    THROW_ERROR_EXCEPTION_UNLESS(!Entrypoint_->Executable.empty(),
        "Companion entrypoint executable is empty");
    if (!NFS::Exists(Entrypoint_->Executable)) {
        THROW_ERROR_EXCEPTION("Companion entrypoint executable does not exist")
            << TErrorAttribute("executable", Entrypoint_->Executable);
    }
}

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TProcessBase> TCompanionProcessManager::CreateProcessIncarnation()
{
    YT_TLOG_INFO("Spawning companion process")
        .With("Executable", Entrypoint_->Executable)
        .With("Args", Entrypoint_->Args)
        .With("CompanionPort", CompanionConfig_->Port);

    auto process = New<TSimpleProcess>(TString(Entrypoint_->Executable), /*copyEnv*/ true);
    process->AddArguments(Entrypoint_->Args);

    auto configTxt = NYson::ConvertToYsonString(CompanionConfig_, NYson::EYsonFormat::Text);
    YT_TLOG_INFO("Set companion config environment variable")
        .With("Config", configTxt);
    process->AddEnvVar(Format("YT_FLOW_COMPANION_CONFIG=%v", configTxt));

    for (const auto& [name, value] : Entrypoint_->Env) {
        process->AddEnvVar(Format("%v=%v", name, value));
    }
    return process;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
