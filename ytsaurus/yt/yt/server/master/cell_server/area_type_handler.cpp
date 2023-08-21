#include "area.h"
#include "area_proxy.h"
#include "area_type_handler.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

namespace NYT::NCellServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NCellarClient;

////////////////////////////////////////////////////////////////////////////////

class TAreaTypeHandler
    : public TObjectTypeHandlerWithMapBase<TArea>
{
public:
    using TObjectTypeHandlerWithMapBase::TObjectTypeHandlerWithMapBase;

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::Area;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        auto cellBundleId = attributes->FindAndRemove<TCellBundleId>("cell_bundle_id");
        auto cellBundleName = attributes->FindAndRemove<TString>("cell_bundle");
        auto cellarType = attributes->FindAndRemove<ECellarType>("cellar_type");

        if (!cellBundleId && !cellBundleName) {
            THROW_ERROR_EXCEPTION("Either \"cell_bundle_id\" or \"cell_bundle\" must be specified");
        }
        if (cellBundleId && cellBundleName) {
            THROW_ERROR_EXCEPTION("Both \"cell_bundle_id\" and \"cell_bundle\" cannot be specified");
        }
        if (cellBundleName && !cellarType) {
            THROW_ERROR_EXCEPTION("When specifying \"cell_bundle\" it is required to specify \"cellar_type\"");
        }

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cellBundle = cellBundleId
            ? cellManager->GetCellBundleByIdOrThrow(
                *cellBundleId,
                /*activeLifeStageOnly*/ true)
            : cellManager->GetCellBundleByNameOrThrow(
                *cellBundleName,
                *cellarType,
                /*activeLifeStageOnly*/ true);

        return cellManager->CreateArea(name, cellBundle, hintId);
    }

private:
    TCellTagList DoGetReplicationCellTags(const TArea* /*area*/) override
    {
        return AllSecondaryCellTags();
    }

    IObjectProxyPtr DoGetProxy(TArea* area, TTransaction* /*transaction*/) override
    {
        return CreateAreaProxy(Bootstrap_, &Metadata_, area);
    }

    void DoZombifyObject(TArea* area) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(area);
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->ZombifyArea(area);
    }
};

IObjectTypeHandlerPtr CreateAreaTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    TEntityMap<TArea>* map)
{
    return New<TAreaTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
