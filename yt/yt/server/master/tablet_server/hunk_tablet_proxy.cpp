#include "hunk_tablet_proxy.h"

#include "hunk_tablet.h"
#include "tablet_proxy_base.h"

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class THunkTabletProxy
    : public TTabletProxyBase
{
public:
    THunkTabletProxy(
        TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        THunkTablet* tablet)
        : TTabletProxyBase(bootstrap, metadata, tablet)
    { }

    TYPath GetOrchidPath(TTabletId tabletId) const override
    {
        return Format("hunk_tablets/%v", tabletId);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateHunkTabletProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    THunkTablet* tablet)
{
    return New<THunkTabletProxy>(bootstrap, metadata, tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
