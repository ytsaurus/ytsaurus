#include "tablet_type_handler.h"
#include "tablet_cell.h"
#include "tablet_cell_proxy.h"
#include "tablet_manager.h"

#include <yt/server/master/cell_server/cell_type_handler_base.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NTabletServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellTypeHandler
    : public TCellTypeHandlerBase<TTabletCell>
{
public:
    explicit TTabletCellTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletCell;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCell, hintId);
        auto holder = std::make_unique<TTabletCell>(id);
        holder->GossipStatistics().Initialize(Bootstrap_);
        return DoCreateObject(std::move(holder), attributes);
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        TObjectId hintId) override
    {
        return std::make_unique<TTabletCell>(hintId);
    }

private:
    using TBase = TCellTypeHandlerBase<TTabletCell>;

    virtual IObjectProxyPtr DoGetProxy(TTabletCell* cell, TTransaction* /*transaction*/) override
    {
        return CreateTabletCellProxy(Bootstrap_, &Metadata_, cell);
    }

    virtual void DoZombifyObject(TTabletCell* cell)
    {
        const auto& tabletManager = TBase::Bootstrap_->GetTabletManager();
        tabletManager->ZombifyTabletCell(cell);

        TBase::DoZombifyObject(cell);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateTabletCellTypeHandler(
    TBootstrap* bootstrap)
{
    return New<TTabletCellTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
