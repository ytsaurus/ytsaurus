#include "config.h"
#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_manager.h"

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cell_server/cell_bundle_proxy.h>

#include <yt/server/master/node_tracker_server/node.h>

#include <yt/server/master/table_server/public.h>

#include <yt/ytlib/tablet_client/config.h>
#include <yt/ytlib/tablet_client/tablet_cell_bundle_ypath_proxy.h>

#include <yt/client/table_client/public.h>

namespace NYT::NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NCellServer;
using namespace NTableServer;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleProxy
    : public TCellBundleProxy
{
public:
    TTabletCellBundleProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTabletCellBundle* cellBundle)
        : TBase(bootstrap, metadata, cellBundle)
    { }

private:
    typedef TCellBundleProxy TBase;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(BalanceTabletCells);
        return TBase::DoInvoke(context);
    }

    virtual void ValidateRemoval() override
    {
        const auto* cellBundle = GetThisImpl<TTabletCellBundle>();
        if (!cellBundle->Cells().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell bundle %Qv since it has %v active tablet cell(s)",
                cellBundle->GetName(),
                cellBundle->Cells().size());
        }
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletBalancerConfig)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true)
            .SetWritePermission(EPermission::Use));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletActions)
            .SetOpaque(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* cellBundle = GetThisImpl<TTabletCellBundle>();

        switch (key) {
            case EInternedAttributeKey::TabletBalancerConfig:
                BuildYsonFluently(consumer)
                    .Value(cellBundle->TabletBalancerConfig());
                return true;

            case EInternedAttributeKey::TabletActions: {
                BuildYsonFluently(consumer)
                    .DoListFor(cellBundle->TabletActions(), [] (TFluentList fluent, TTabletAction* action) {
                        fluent.Item().BeginMap()
                            .Item("tablet_action_id").Value(action->GetId())
                            .Item("kind").Value(action->GetKind())
                            .Item("state").Value(action->GetState())
                            .DoIf(!action->IsFinished(), [action] (TFluentMap fluent) {
                                fluent.Item("tablet_ids").DoListFor(
                                    action->Tablets(), [] (TFluentList fluent, TTablet* tablet) {
                                        fluent.Item().Value(tablet->GetId());
                                    });
                            })
                            .DoIf(!action->Error().IsOK(), [action] (TFluentMap fluent) {
                                fluent.Item("error").Value(action->Error());
                            })
                            .Item("expiration_time").Value(action->GetExpirationTime())
                        .EndMap();
                    });
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* cellBundle = GetThisImpl<TTabletCellBundle>();

        switch (key) {
            case EInternedAttributeKey::TabletBalancerConfig:
                cellBundle->TabletBalancerConfig() = ConvertTo<TTabletBalancerConfigPtr>(value);
                return true;

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    DECLARE_YPATH_SERVICE_METHOD(NTabletClient::NProto, BalanceTabletCells);
};

DEFINE_YPATH_SERVICE_METHOD(TTabletCellBundleProxy, BalanceTabletCells)
{
    DeclareMutating();

    using NYT::FromProto;

    auto movableTableIds = FromProto<std::vector<TTableId>>(request->movable_tables());
    bool keepActions = request->keep_actions();

    context->SetRequestInfo("TableIds: %v, KeepActions: %v, ", movableTableIds, keepActions);

    ValidateNoTransaction();

    auto* trunkNode = GetThisImpl<TTabletCellBundle>();

    std::vector<TTableNode*> movableTables;
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto tableId : movableTableIds) {
        auto* node = objectManager->GetObjectOrThrow(tableId);
        if (node->GetType() != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Unexpected object type: expected %v, got %v", EObjectType::Table, node->GetType())
                << TErrorAttribute("object_id", tableId);
        }
        movableTables.push_back(node->As<TTableNode>());
    }

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    auto tabletActions = tabletManager->SyncBalanceCells(
        trunkNode,
        movableTables.empty() ? std::nullopt : std::make_optional(movableTables),
        keepActions);
    ToProto(response->mutable_tablet_actions(), tabletActions);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateTabletCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTabletCellBundle* cellBundle)
{
    return New<TTabletCellBundleProxy>(bootstrap, metadata, cellBundle);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

