#pragma once

#include "public.h"

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterConnectorState,
    // Not registered.
    (Offline)
    // Registered but did not report the full heartbeat yet.
    (Registered)
    // Registered and reported the full heartbeat.
    (Online)
);

////////////////////////////////////////////////////////////////////////////////

//! Mediates connection between a data node and its master.
/*!
 *  \note
 *  Thread affinity: Control
 */
struct IMasterConnector
    : public TRefCounted
{
    //! Initialize master connector.
    virtual void Initialize() = 0;

    //! Returns full data node master heartbeat request. This function is used only for compatibility
    //! with legacy master connector and will be removed after switching to new heartbeats.
    virtual NDataNodeTrackerClient::NProto::TReqFullHeartbeat GetFullHeartbeatRequest(NObjectClient::TCellTag cellTag) = 0;

    //! Returns incremental data node master heartbeat request. This function is used only for compatibility
    //! with legacy master connector and will be removed after switching to new heartbeats.
    virtual NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat GetIncrementalHeartbeatRequest(NObjectClient::TCellTag cellTag) = 0;

    //! Called when full data node heartbeat is successfully reported.
    virtual void OnFullHeartbeatReported(NObjectClient::TCellTag cellTag) = 0;

    //! Called when incremental data node heartbeat report failed.
    virtual void OnIncrementalHeartbeatFailed(NObjectClient::TCellTag cellTag) = 0;

    //! Called when incremental data node heartbeat is successfully reported.
    virtual void OnIncrementalHeartbeatResponse(
        NObjectClient::TCellTag cellTag,
        const NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat& response) = 0;

    //! Returns state of master connection to a given master cell.
    virtual EMasterConnectorState GetMasterConnectorState(NObjectClient::TCellTag cellTag) = 0;

    //! Returns |True| if full data node heartbeat can be now sent to a given master cell.
    virtual bool CanSendFullNodeHeartbeat(NObjectClient::TCellTag cellTag) const = 0;

    //! Returns a future that becomes set when an incremental data node heartbeat is
    //! successfully sent to a given cell.
    virtual TFuture<void> GetHeartbeatBarrier(NObjectClient::TCellTag cellTag) = 0;

    //! Schedules next data node heartbeat to all the master cells.
    /*!
    *  \note
    *  Thread affinity: any
    */
    virtual void ScheduleHeartbeat(bool immediately) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
