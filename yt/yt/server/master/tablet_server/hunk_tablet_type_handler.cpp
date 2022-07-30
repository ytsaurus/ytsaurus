#include "tablet_type_handler.h"

#include "hunk_tablet.h"
#include "hunk_tablet_proxy.h"
#include "tablet_manager.h"
#include "tablet_type_handler_base.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class THunkTabletTypeHandler
    : public TTabletTypeHandlerBase<THunkTablet>
{
public:
    explicit THunkTabletTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
        , Bootstrap_(bootstrap)
    { }

private:
    using TBase = TTabletTypeHandlerBase<THunkTablet>;

    TBootstrap* const Bootstrap_;

    EObjectType GetType() const override
    {
        return EObjectType::HunkTablet;
    }

    IObjectProxyPtr DoGetProxy(THunkTablet* tablet, TTransaction* /*transaction*/) override
    {
        return CreateHunkTabletProxy(Bootstrap_, &Metadata_, tablet);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateHunkTabletTypeHandler(TBootstrap* bootstrap)
{
    return New<THunkTabletTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
