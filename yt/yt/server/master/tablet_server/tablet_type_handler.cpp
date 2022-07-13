#include "tablet_type_handler.h"

#include "tablet.h"
#include "tablet_proxy.h"
#include "tablet_manager.h"
#include "tablet_type_handler_base.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTabletServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TTabletTypeHandler
    : public TTabletTypeHandlerBase<TTablet>
{
public:
    explicit TTabletTypeHandler(TBootstrap* bootstrap)
        : TTabletTypeHandlerBase<TTablet>(bootstrap)
        , Bootstrap_(bootstrap)
    { }

    EObjectType GetType() const override
    {
        return EObjectType::Tablet;
    }

private:
    TBootstrap* const Bootstrap_;

    IObjectProxyPtr DoGetProxy(TTablet* tablet, TTransaction* /*transaction*/) override
    {
        return CreateTabletProxy(Bootstrap_, &Metadata_, tablet);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateTabletTypeHandler(TBootstrap* bootstrap)
{
    return New<TTabletTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
