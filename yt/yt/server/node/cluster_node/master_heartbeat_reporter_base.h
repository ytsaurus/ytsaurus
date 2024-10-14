#pragma once

#include "public.h"

#include <yt/yt/ytlib/cell_master_client/public.h>

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/async_rw_lock.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

//! Mediates regular heartbeat reports to group of masters.
/*!
*  \note
*  Thread affinity: Control
*/
class TMasterHeartbeatReporterBase
    : public virtual TRefCounted
{
public:
    TMasterHeartbeatReporterBase(
        IBootstrapBase* bootstrap,
        bool reportHeartbeatsToAllSecondaryMasters,
        NLogging::TLogger logger);

    virtual void Initialize();
    virtual void StartNodeHeartbeats();
    virtual void StartNodeHeartbeatsToCell(NObjectClient::TCellTag cellTag);
    virtual void Reconfigure(const NConcurrency::TRetryingPeriodicExecutorOptions& options);

protected:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    IBootstrapBase* const Bootstrap_;

    const bool ReportHeartbeatsToAllSecondaryMasters_;

    THashSet<NObjectClient::TCellTag> MasterCellTags_;
    THashMap<NObjectClient::TCellTag, NConcurrency::TAsyncReaderWriterLock> ExecutorLockPerMaster_;

    NConcurrency::TRetryingPeriodicExecutorOptions Options_;
    THashMap<NObjectClient::TCellTag, NConcurrency::TRetryingPeriodicExecutorPtr> Executors_;

    const NLogging::TLogger Logger;

    virtual TFuture<void> DoReportHeartbeat(NObjectClient::TCellTag cellTag) = 0;

    virtual void OnHeartbeatSucceeded(NObjectClient::TCellTag cellTag) = 0;
    virtual void OnHeartbeatFailed(NObjectClient::TCellTag cellTag) = 0;

    virtual void ResetState(NObjectClient::TCellTag cellTag) = 0;

private:
    void DoStartNodeHeartbeatsToCell(NObjectClient::TCellTag cellTag);

    TError ReportHeartbeat(NObjectClient::TCellTag cellTag);

    void OnReadyToUpdateHeartbeatStream(
        const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs);

    NConcurrency::TRetryingPeriodicExecutorPtr FindExecutor(NObjectClient::TCellTag cellTag) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
