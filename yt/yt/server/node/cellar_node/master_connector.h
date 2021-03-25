#pragma once

#include "public.h"

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

//! Mediates connection between a cellar node and its master.
/*!
 *  \note
 *  Thread affinity: Control
 */
struct IMasterConnector
    : public TRefCounted
{
    using OnHeartbeatRequestedSignature = void(
        NObjectClient::TCellTag,
        NCellarClient::ECellarType,
        NCellarAgent::ICellarPtr,
        NCellarNodeTrackerClient::NProto::TReqCellarHeartbeat*);

    //! Raised when heartbeat is constructed.
    //! Used to add cell statistics known by cellar node.
    DECLARE_INTERFACE_SIGNAL(OnHeartbeatRequestedSignature, HeartbeatRequested);

    //! Initialize master connector.
    virtual void Initialize() = 0;

    //! Schedules next cellar node heartbeat.
    /*!
    *  \note
    *  Thread affinity: any
    */
    virtual void ScheduleHeartbeat(NObjectClient::TCellTag cellTag, bool immediately) = 0;

    //! Return cellar node master heartbeat request for a given cell. This function is used only for compatibility
    //! with legacy master connector and will be removed after switching to new heartbeats.
    virtual NCellarNodeTrackerClient::NProto::TReqHeartbeat GetHeartbeatRequest(NObjectClient::TCellTag cellTag) const = 0;

    //! Process cellar node master heartbeat response. This function is used only for compatibility
    //! with legacy master connector and will be removed after switching to new heartbeats.
    virtual void OnHeartbeatResponse(const NCellarNodeTrackerClient::NProto::TRspHeartbeat& response) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
