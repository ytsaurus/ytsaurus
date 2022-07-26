#include "tablet_action_type_handler.h"
#include "tablet_action_proxy.h"
#include "tablet_manager.h"
#include "tablet_action.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTabletServer {

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

    EObjectType GetType() const override
    {
        return EObjectType::TabletAction;
    }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto kind = attributes->GetAndRemove<ETabletActionKind>("kind");
        auto tabletCount = attributes->FindAndRemove<int>("tablet_count");
        auto skipFreezing = attributes->GetAndRemove<bool>("skip_freezing", false);
        auto tabletIds = attributes->GetAndRemove<std::vector<TTabletId>>("tablet_ids");
        auto cellIds = attributes->GetAndRemove<std::vector<TTabletCellId>>(
            "cell_ids",
            std::vector<TTabletCellId>());
        auto pivotKeys = attributes->GetAndRemove<std::vector<TLegacyOwningKey>>(
            "pivot_keys",
            std::vector<TLegacyOwningKey>());
        auto correlationId = attributes->GetAndRemove<TGuid>("correlation_id", TGuid{});

        TInstant expirationTime = TInstant::Zero();
        auto optionalKeepFinished = attributes->FindAndRemove<bool>("keep_finished");
        auto optionalExpirationTime = attributes->FindAndRemove<TInstant>("expiration_time");
        auto optionalExpirationTimeout = attributes->FindAndRemove<TDuration>("expiration_timeout");

        if (static_cast<int>(optionalKeepFinished.has_value()) +
            static_cast<int>(optionalExpirationTime.has_value()) +
            static_cast<int>(optionalExpirationTimeout.has_value()) > 1)
        {
            THROW_ERROR_EXCEPTION("At most one of \"keep_finished\", \"expiration_time\", "
                "\"expiration_timeout\" can be specified");
        } else if (optionalKeepFinished) {
            if (*optionalKeepFinished) {
                expirationTime = TInstant::Max();
            }
        } else if (optionalExpirationTime) {
            expirationTime = *optionalExpirationTime;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        if (attributes->Find<bool>("freeze")) {
            THROW_ERROR_EXCEPTION("Attribute \"freeze\" cannot be specified by user");
        }

        std::vector<TTabletBase*> tablets;
        std::vector<TTabletCell*> cells;

        for (auto tabletId : tabletIds) {
            tablets.push_back(tabletManager->GetTabletOrThrow(tabletId));
        }
        for (auto cellId : cellIds) {
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
            correlationId,
            expirationTime,
            optionalExpirationTimeout);
    }

private:
    TBootstrap* const Bootstrap_;

    TCellTagList DoGetReplicationCellTags(const TTabletAction* /*action*/) override
    {
        return AllSecondaryCellTags();
    }

    IObjectProxyPtr DoGetProxy(TTabletAction* action, TTransaction* /*transaction*/) override
    {
        return CreateTabletActionProxy(Bootstrap_, &Metadata_, action);
    }

    void DoDestroyObject(TTabletAction* action) noexcept override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->DestroyTabletAction(action);

        TObjectTypeHandlerWithMapBase::DoDestroyObject(action);
    }
};

IObjectTypeHandlerPtr CreateTabletActionTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TTabletAction>* map)
{
    return New<TTabletActionTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
