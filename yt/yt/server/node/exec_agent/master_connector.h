#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/exec_node_tracker_client/proto/exec_node_tracker_service.pb.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Mediates connection between an exec node and its master.
/*!
 *  \note
 *  Thread affinity: Control
 */
struct IMasterConnector
    : public TRefCounted
{
    //! Initialize master connector.
    virtual void Initialize() = 0;

    //! Return exec node master heartbeat request. This function is used only for compatibility
    //! with legacy master connector and will be removed after switching to new heartbeats.
    virtual NExecNodeTrackerClient::NProto::TReqHeartbeat GetHeartbeatRequest() const = 0;

    //! Process exec node master heartbeat response. This function is used only for compatibility
    //! with legacy master connector and will be removed after switching to new heartbeats.
    virtual void OnHeartbeatResponse(const NExecNodeTrackerClient::NProto::TRspHeartbeat& response) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
