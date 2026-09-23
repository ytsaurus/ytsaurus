#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, ControllerLogger, "FlowController");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, BalancerLogger, "FlowBalancer");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, WorkerTrackerLogger, "FlowWorkerTracker");

////////////////////////////////////////////////////////////////////////////////

//! Whether a failed commit is worth retrying rather than treating as a breakdown. A tablet in the
//! middle of a smooth movement rejects everything sent to it and comes back within seconds,
//! carrying a redirection hint; anything else (a tablet that is genuinely down, a bad request)
//! outlives a scheduling iteration.
bool IsTransientTabletError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELeaderConfirmationResult,
    // The cluster reached the controller at the published address.
    (Confirmed)
    // Not attempted: skipped by #SkipLeaderProxyConfirmationEnvVarName.
    (SkippedByEnvironment)
    // Attempted and hopeless: the cluster requires TLS the controller cannot serve.
    (SkippedWithoutTlsMaterial)
    // Attempted and failed; worth retrying.
    (Failed)
);

//! Classifies the outcome of the leadership confirmation through the RPC proxy. |confirmationError|
//! is OK when the confirmation succeeded and is not inspected when the confirmation was skipped by
//! the environment. |busServerHasTlsMaterial| tells whether the config gives the controller bus server
//! both a certificate chain and a private key; the self-signed incarnation certificate the controller
//! serves otherwise is not trusted by the cluster.
ELeaderConfirmationResult ClassifyLeaderConfirmation(
    bool skipConfirmationFromEnv,
    const TError& confirmationError,
    const std::string& controllerAddress,
    bool busServerHasTlsMaterial);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
