#pragma once

#include "maintenance_target.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterProxyNode
    : public NCypressServer::TCypressMapNode
    , public TMaintenanceTarget<TClusterProxyNode, EMaintenanceType::Ban>
{
public:
    using NCypressServer::TCypressMapNode::TCypressMapNode;

private:
    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TClusterProxyNode, 0xe4e7051b);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
