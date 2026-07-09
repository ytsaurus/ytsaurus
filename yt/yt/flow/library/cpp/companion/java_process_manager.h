#pragma once

#include "process_manager_base.h"
#include "public.h"
#include <yt/yt/core/misc/config.h>

namespace NYT::NFlow::NCompanion {

//////////////////////////////////////////////////////////////////////////////////

//! Manages the lifecycle of a Java companion process.
/*!
 *  Spawns a JVM process using the configured JDK binary, classpath, and main class.
 *  Inherits auto-restart, health-check, and metrics-collection logic from TProcessManagerBase.
 */
class TJavaProcessManager
    : public TProcessManagerBase
{
public:
    TJavaProcessManager(
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
        const std::string& mainClass);

protected:
    //! Validates that JDK binary path, main class, and classpath are properly configured.
    void ValidateParameters() const override;

    //! Creates a new JVM process incarnation using the configured JDK, classpath, and main class.
    TIntrusivePtr<TProcessBase> CreateProcessIncarnation() override;

private:
    //! Path to the JDK bin directory (e.g. containing the `java` executable).
    const std::string JdkBinPath_;

    //! Java classpath passed to the JVM on startup.
    const std::string Classpath_;

    //! Fully-qualified main class name to execute.
    const std::string MainClass_;
};

DEFINE_REFCOUNTED_TYPE(TJavaProcessManager);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
