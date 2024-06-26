#pragma once

#include "public.h"

#include <yt/yt/ytlib/cell_master_client/public.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

//! Mediates regular heartbeat reports to group of masters.
/*!
*  \note
*  Thread affinity: Control
*/
struct IMasterHeartbeatReporter
    : public TRefCounted
{
    virtual void Initialize() = 0;

    virtual void StartHeartbeats() = 0;
    virtual void StartHeartbeatsToCell(NObjectClient::TCellTag cellTag) = 0;

    virtual void Reconfigure(const NConcurrency::TRetryingPeriodicExecutorOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterHeartbeatReporter)

////////////////////////////////////////////////////////////////////////////////

IMasterHeartbeatReporterPtr CreateMasterHeartbeatReporter(
    IBootstrapBase* bootstrap,
    bool reportHeartbeatsToAllSecondaryMasters,
    IMasterHeartbeatReporterCallbacksPtr callbacks,
    NConcurrency::TRetryingPeriodicExecutorOptions options,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
