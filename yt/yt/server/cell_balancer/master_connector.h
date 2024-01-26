#pragma once

#include "private.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

//! Mediates communication between cell balancer and master.
/*!
 *  \note Thread affinity: control unless noted otherwise
 */
struct IMasterConnector
    : public TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(
    IBootstrap* bootstrap,
    TCellBalancerMasterConnectorConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
