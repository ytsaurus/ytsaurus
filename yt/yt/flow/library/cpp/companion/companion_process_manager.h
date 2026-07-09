#pragma once

#include "process_manager_base.h"
#include "public.h"

#include <yt/yt/core/misc/config.h>

namespace NYT::NFlow::NCompanion {

//////////////////////////////////////////////////////////////////////////////////

//! Manages the lifecycle of a companion process described by a language-agnostic entrypoint.
/*!
 *  Spawns `executable + args + env` from the entrypoint and sets `YT_FLOW_COMPANION_CONFIG`.
 *  Language-specific argv preparation (e.g. JVM options injection) is the responsibility
 *  of the owning companion manager, which mutates the entrypoint before passing it here.
 */
class TCompanionProcessManager
    : public TProcessManagerBase
{
public:
    TCompanionProcessManager(
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
        const TCompanionEntrypointPtr& entrypoint);

protected:
    //! Validates that the entrypoint is configured (executable non-empty and exists on disk).
    void ValidateParameters() const override;

    //! Creates a new process incarnation as described by the entrypoint.
    TIntrusivePtr<TProcessBase> CreateProcessIncarnation() override;

private:
    //! Entrypoint describing how to spawn the companion process.
    const TCompanionEntrypointPtr Entrypoint_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionProcessManager);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
