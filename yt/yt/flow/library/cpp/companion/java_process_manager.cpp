#include "java_process_manager.h"
#include "jvm_options.h"

#include "config.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/library/process/process.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

TJavaProcessManager::TJavaProcessManager(
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
    const std::string& jdkBinPath,
    const std::string& classpath,
    const std::string& mainClass)
    : TProcessManagerBase(
        invoker,
        companionClient,
        backoffOptions,
        restartDelay,
        healthCheckInterval,
        startupGracePeriod,
        metricsCollectionInterval,
        logger,
        profiler,
        statusProfiler)
    , JdkBinPath_(jdkBinPath)
    , Classpath_(classpath)
    , MainClass_(mainClass)
{ }

////////////////////////////////////////////////////////////////////////////////

void TJavaProcessManager::ValidateParameters() const
{
    THROW_ERROR_EXCEPTION_UNLESS(!JdkBinPath_.empty(), "JDK binary path is empty");
    THROW_ERROR_EXCEPTION_UNLESS(!MainClass_.empty(), "Main class is empty");
    THROW_ERROR_EXCEPTION_UNLESS(!Classpath_.empty(), "Classpath is empty");
    if (!NFS::Exists(JdkBinPath_)) {
        THROW_ERROR_EXCEPTION("JDK binary file does not exist")
            << TErrorAttribute("jdk_bin_path", JdkBinPath_);
    }
    if (CompanionConfig_->CompanionProcessCount != 0) {
        THROW_ERROR_EXCEPTION(
                "%Qv is a Python-companion-only knob and must not be set for the Java companion",
                "companion_process_count")
            << TErrorAttribute("companion_process_count", CompanionConfig_->CompanionProcessCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TProcessBase> TJavaProcessManager::CreateProcessIncarnation()
{
    auto effectiveJvmOptions = ResolveJvmOptions();

    YT_LOG_INFO("Spawning java companion process (Classpath: %v, MainClass: %v, CompanionPort: %v, JdkBinPath: %v, JvmOptions: %v)",
        Classpath_,
        MainClass_,
        CompanionConfig_->Port,
        JdkBinPath_,
        effectiveJvmOptions);
    auto process = New<TSimpleProcess>(JdkBinPath_, /*copyEnv*/ true);

    std::vector<std::string> args;
    for (const auto& opt : effectiveJvmOptions) {
        args.push_back(opt);
    }
    args.push_back("-cp");
    args.push_back(Classpath_);
    args.push_back(MainClass_);
    process->AddArguments(args);

    auto confixTxt =
        NYson::ConvertToYsonString(CompanionConfig_, NYson::EYsonFormat::Text);
    YT_LOG_INFO("Set companion config environment variable (Config: %v)", confixTxt);
    process->AddEnvVar(Format("YT_FLOW_COMPANION_CONFIG=%v", confixTxt));
    return process;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
