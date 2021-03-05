#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>
#include <yt/yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Mediates connection between a node and its master.
/*!
 *  This class is responsible for registering the node and sending
 *  heartbeats. In particular, it reports chunk deltas to the master
 *  and manages jobs.
 *
 *  \note
 *  Thread affinity: any
 */
class TLegacyMasterConnector
    : public TRefCounted
{
public:
    //! Creates an instance.
    TLegacyMasterConnector(
        TDataNodeConfigPtr config,
        const std::vector<TString>& nodeTags,
        NClusterNode::TBootstrap* bootstrap);

    //! Starts interaction with master.
    void Start();

    //! Schedules a new node heartbeat via TDelayedExecutor.
    void ScheduleNodeHeartbeat(NObjectClient::TCellTag cellTag, bool immediately = false);

    void OnMasterConnected();

    //! Resets connection state.
    void Reset();

private:
    const TDataNodeConfigPtr Config_;

    const std::vector<TString> NodeTags_;
    const NClusterNode::TBootstrap* Bootstrap_;
    const IInvokerPtr ControlInvoker_;

    bool Started_ = false;

    //! Forbids concurrent heartbeats.
    THashMap<NObjectClient::TCellTag, std::unique_ptr<NConcurrency::TAsyncReaderWriterLock>> HeartbeatLocks_;

    //! Per-cell amount of heartbeats scheduled by delayed executor.
    THashMap<NObjectClient::TCellTag, int> HeartbeatsScheduled_;

    //! Per-cell incremental heartbeat throttler.
    THashMap<NObjectClient::TCellTag, NConcurrency::IReconfigurableThroughputThrottlerPtr> IncrementalHeartbeatThrottler_;

    //! Period between consequent incremental heartbeats.
    TDuration IncrementalHeartbeatPeriod_;

    //! Splay for incremental heartbeats.
    TDuration IncrementalHeartbeatPeriodSplay_;

    //! Schedules a new node heartbeat via TDelayedExecutor.
    void DoScheduleNodeHeartbeat(NObjectClient::TCellTag cellTag, bool immediately = false);

    //! Sends an appropriate node heartbeat.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out a full heartbeat to Node Tracker.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportFullNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out an incremental heartbeat to Node Tracker.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportIncrementalNodeHeartbeat(NObjectClient::TCellTag cellTag);

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TLegacyMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
