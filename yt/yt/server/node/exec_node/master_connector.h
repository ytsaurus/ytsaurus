#pragma once

#include "public.h"

#include <yt/yt/client/node_tracker_client/public.h>

namespace NYT::NExecNode {

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

    virtual void OnDynamicConfigChanged(
        const TMasterConnectorDynamicConfigPtr& oldConfig,
        const TMasterConnectorDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
