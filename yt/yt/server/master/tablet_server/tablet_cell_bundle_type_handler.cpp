#include "tablet_type_handler.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_manager.h"

#include <yt/server/master/cell_server/cell_bundle_type_handler.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/tablet_client/config.h>

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
    explicit TTabletCellBundleTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletCellBundle;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCellBundle, hintId);
        auto holder = std::make_unique<TTabletCellBundle>(id);
        return DoCreateObject(std::move(holder), attributes);
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        TObjectId hintId) override
    {
        return std::make_unique<TTabletCellBundle>(hintId);
    }

private:
    using TBase = TCellBundleTypeHandlerBase<TTabletCellBundle>;

    virtual IObjectProxyPtr DoGetProxy(TTabletCellBundle* cellBundle, TTransaction* /*transaction*/) override
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
