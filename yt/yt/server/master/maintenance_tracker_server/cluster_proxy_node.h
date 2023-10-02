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

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
