#include "tablet_type_handler.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/cell_server/cell_bundle_type_handler.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

namespace NYT::NTabletServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleTypeHandler
    : public TCellBundleTypeHandlerBase<TTabletCellBundle>
{
public:
    using TCellBundleTypeHandlerBase::TCellBundleTypeHandlerBase;

    EObjectType GetType() const override
    {
        return EObjectType::TabletCellBundle;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCellBundle, hintId);
        auto options = attributes->GetAndRemove<TTabletCellOptionsPtr>("options");
        auto holder = TPoolAllocator::New<TTabletCellBundle>(id);
        holder->ResourceUsage().Initialize(Bootstrap_);
        holder->ResourceLimits().TabletCount = DefaultTabletCountLimit;
        return DoCreateObject(std::move(holder), attributes, std::move(options));
    }

private:
    IObjectProxyPtr DoGetProxy(TTabletCellBundle* cellBundle, TTransaction* /*transaction*/) override
    {
        return CreateTabletCellBundleProxy(Bootstrap_, &Metadata_, cellBundle);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateTabletCellBundleTypeHandler(
    TBootstrap* bootstrap)
{
    return New<TTabletCellBundleTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
