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

////////////////////////////////////////////////////////////////////////////////

class TAreaTypeHandler
    : public TObjectTypeHandlerWithMapBase<TArea>
{
public:
    using TObjectTypeHandlerWithMapBase::TObjectTypeHandlerWithMapBase;

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Area;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        auto cellBundleName = attributes->GetAndRemove<TString>("cell_bundle");
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cellBundle = cellManager->GetCellBundleByNameOrThrow(
            cellBundleName,
            /*activeLifeStageOnly*/ true);
        return cellManager->CreateArea(name, cellBundle, hintId);
    }

private:
    virtual TCellTagList DoGetReplicationCellTags(const TArea* /*area*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual IObjectProxyPtr DoGetProxy(TArea* area, TTransaction* /*transaction*/) override
    {
        return CreateAreaProxy(Bootstrap_, &Metadata_, area);
    }

    virtual void DoZombifyObject(TArea* area) override
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
