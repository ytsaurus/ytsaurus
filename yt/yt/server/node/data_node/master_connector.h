#pragma once

#include "public.h"

namespace NYT::NDataNode {

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

    //! Returns a future that becomes set when an incremental data node heartbeat is
    //! successfully sent to a given cell.
    virtual TFuture<void> GetHeartbeatBarrier(NObjectClient::TCellTag cellTag) = 0;

    //! Schedules out-of-order data node heartbeat to all the master cells.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual void ScheduleHeartbeat(bool immediately) = 0;

    //! Schedules out-of-order job heartbeate to a given job tracker.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual void ScheduleJobHeartbeat(const TString& jobTrackerAddress) = 0;

    //! Returns |true| if this node has received full heartbeat responses from all cells.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsOnline() const = 0;

    // COMPAT(kvk1920): Remove after 23.2.
    virtual void SetLocationUuidsRequired(bool value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
