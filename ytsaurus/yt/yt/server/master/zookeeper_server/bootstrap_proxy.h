#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NZookeeperServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NZookeeperMaster::IBootstrapProxy> CreateZookeeperBootstrapProxy(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
