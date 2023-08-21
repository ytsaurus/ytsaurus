#pragma once

#include "public.h"

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/client/object_client/public.h>

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
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
