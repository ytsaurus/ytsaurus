#include "tablet_type_handler.h"
#include "tablet_cell.h"
#include "tablet_cell_proxy.h"
#include "tablet_manager.h"

#include <yt/server/object_server/type_handler_detail.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NTabletServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTabletCell>
{
public:
    TTabletCellTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TTabletCell>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
        , Bootstrap_(bootstrap)
    { }

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletCell;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override
    {
        auto cellBundleName = attributes->GetAndRemove("tablet_cell_bundle", DefaultTabletCellBundleName);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* cellBundle = tabletManager->GetTabletCellBundleByNameOrThrow(cellBundleName);
        return tabletManager->CreateTabletCell(cellBundle, hintId);
    }

private:
    TBootstrap* const Bootstrap_;

    virtual TCellTagList DoGetReplicationCellTags(const TTabletCell* /*cell*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TTabletCell* cell) override
    {
        return Format("tablet cell %v", cell->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTabletCell* cell, TTransaction* /*transaction*/) override
    {
        return CreateTabletCellProxy(Bootstrap_, &Metadata_, cell);
    }

    virtual void DoZombifyObject(TTabletCell* cell) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(cell);
        // NB: Destroy the cell right away and do not wait for GC to prevent
        // dangling links from occurring in //sys/tablet_cells.
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->DestroyTabletCell(cell);
    }
};

IObjectTypeHandlerPtr CreateTabletCellTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TTabletCell>* map)
{
    return New<TTabletCellTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
