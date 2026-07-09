#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! YSON-serializable parameters for the companion manager.
struct TCompanionManagerParameters
    : public NYTree::TYsonStruct
{
    //! Timeout for individual RPC calls to the companion process.
    TDuration Timeout;

    //! Exponential backoff options used when retrying failed companion requests.
    TExponentialBackoffOptions Backoff;

    //! Whether to launch the companion process; set to false to connect to an already-running process.
    bool RunProcess{};

    //! Description of the companion executable and its arguments.
    TCompanionEntrypointPtr Entrypoint;

    //! Exponential backoff options used while waiting for the companion to become ready after startup.
    TExponentialBackoffOptions InitBackoff;

    //! Interval between periodic health-check pings to the companion process.
    TDuration HealthCheckInterval;

    //! Window after spawning a companion incarnation during which health check failures
    //! do not restart the process, as long as it is alive and has not passed its first health check.
    TDuration StartupGracePeriod;

    //! Interval between periodic metrics collection from the companion process.
    TDuration MetricsCollectionInterval;

    //! Delay before restarting the companion process after a failure.
    TDuration RestartDelay;

    REGISTER_YSON_STRUCT(TCompanionManagerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionManagerParameters);

////////////////////////////////////////////////////////////////////////////////

//! Resource that optionally launches and supervises a companion process described by a
//! language-agnostic entrypoint and exposes an RPC client to it.
/*!
 *  Resolves the companion port from the singleton state and constructs a gRPC client
 *  bound to the local companion address. When RunProcess is enabled, also launches and
 *  supervises the companion process with auto-restart and health-checks.
 */
class TCompanionManager
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCompanionManagerParameters);

    TCompanionManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    //! Creates a new companion RPC client with the given status profiler.
    ICompanionClientPtr CreateCompanionClient(IStatusProfilerPtr statusProfiler);

    //! Starts the companion process (if RunProcess is enabled) and waits for it to become ready.
    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

protected:
    //! Creates the process manager responsible for spawning and supervising the companion.
    virtual TProcessManagerBasePtr CreateProcessManager();

    //! Companion config with port, monitoring port, cluster url and pipeline path.
    const TCompanionExecutionConfigPtr CompanionConfig_;

    //! Full local address (host:port) used to connect to the companion process.
    const std::string CompanionAddress_;

    //! gRPC client used to send requests to the companion process.
    ICompanionClientPtr CompanionClient_;

    //! Profiler scoped to this companion manager instance.
    NProfiling::TProfiler Profiler_;

    //! Process manager; constructed lazily in Load() once CreateProcessManager() is available.
    TProcessManagerBasePtr ProcessManager_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
