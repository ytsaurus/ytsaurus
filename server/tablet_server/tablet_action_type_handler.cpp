#include "tablet_action_type_handler.h"
#include "tablet_action_proxy.h"
#include "tablet_manager.h"

#include <yt/server/object_server/type_handler_detail.h>

#include <yt/client/object_client/helpers.h>

namespace NYT {
namespace NTabletServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NYTree;
using namespace NCellMaster;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletActionTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTabletAction>
{
public:
    TTabletActionTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TTabletAction>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
        , Bootstrap_(bootstrap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletAction;
    }

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::Creatable;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override
    {
        auto kind = attributes->GetAndRemove<ETabletActionKind>("kind");
        auto tabletCount = attributes->FindAndRemove<int>("tablet_count");
        auto skipFreezing = attributes->GetAndRemove<bool>("skip_freezing", false);
        auto freeze = attributes->FindAndRemove<bool>("freeze");
        auto keepFinished = attributes->GetAndRemove<bool>("keep_finished", false);
        auto tabletIds = attributes->GetAndRemove<std::vector<TTabletId>>("tablet_ids");
        auto cellIds = attributes->GetAndRemove<std::vector<TTabletCellId>>(
            "cell_ids",
            std::vector<TTabletCellId>());
        auto pivotKeys = attributes->GetAndRemove<std::vector<TOwningKey>>(
            "pivot_keys",
            std::vector<TOwningKey>());
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        std::vector<TTablet*> tablets;
        std::vector<TTabletCell*> cells;

        for (const auto& tabletId : tabletIds) {
            tablets.push_back(tabletManager->GetTabletOrThrow(tabletId));
        }
        for (const auto& cellId : cellIds) {
            cells.push_back(tabletManager->GetTabletCellOrThrow(cellId));
        }

        return tabletManager->CreateTabletAction(
            hintId,
            kind,
            tablets,
            cells,
            pivotKeys,
            tabletCount,
            skipFreezing,
            freeze,
            keepFinished);
    }

private:
    TBootstrap* const Bootstrap_;

    virtual TCellTagList DoGetReplicationCellTags(const TTabletAction* /*action*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TTabletAction* action) override
    {
        return Format("tablet action %v", action->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTabletAction* action, TTransaction* /*transaction*/) override
    {
        return CreateTabletActionProxy(Bootstrap_, &Metadata_, action);
    }

    virtual void DoDestroyObject(TTabletAction* action) override
    {
        TObjectTypeHandlerWithMapBase::DoDestroyObject(action);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->DestroyTabletAction(action);
    }
};

IObjectTypeHandlerPtr CreateTabletActionTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TTabletAction>* map)
{
    return New<TTabletActionTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
