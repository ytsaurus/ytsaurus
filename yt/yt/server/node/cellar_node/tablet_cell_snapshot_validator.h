#include "public.h"

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

NCellarAgent::ICellarOccupantPtr CreateFakeOccupant(
    IBootstrap* bootstrap,
    NHydra::TCellId cellId,
    std::string tabletCellBundle,
    NApi::TClusterTag clockClusterTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
