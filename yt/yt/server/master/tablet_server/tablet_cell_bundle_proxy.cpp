#include "config.h"
#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_manager.h"
#include "tablet_resources.h"
#include "tablet_action.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/server/master/object_server/helpers.h>
#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cell_server/cell_bundle_proxy.h>
#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>
#include <yt/yt/ytlib/tablet_client/tablet_cell_bundle_ypath_proxy.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NCellarClient;
using namespace NCellServer;
using namespace NChunkServer;
using namespace NTableServer;
using namespace NTabletBalancer;
using namespace NObjectServer;
using namespace NNodeTrackerServer;
using namespace NSecurityServer;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleProxy
    : public TCellBundleProxy
{
public:
    using TCellBundleProxy::TCellBundleProxy;

private:
    using TBase = TCellBundleProxy;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(BalanceTabletCells);
        return TBase::DoInvoke(context);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        const auto* cellBundle = GetThisImpl<TTabletCellBundle>();

        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletBalancerConfig)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true)
            .SetWritePermission(EPermission::Use));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::BundleControllerTargetConfig)
            .SetWritable(true)
            .SetReplicated(true)
            .SetWritePermission(EPermission::Manage)
            .SetPresent(cellBundle->GetBundleControllerTargetConfig().has_value()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletActions)
            .SetOpaque(true));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceLimits)
            .SetWritable(true)
            .SetReplicated(true));
        attributes->push_back(EInternedAttributeKey::ViolatedResourceLimits);
        attributes->push_back(EInternedAttributeKey::ResourceUsage);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Abc)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true)
            .SetRemovable(true)
            .SetPresent(cellBundle->GetAbcConfig().operator bool()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::FolderId)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true)
            .SetRemovable(true)
            .SetPresent(cellBundle->GetFolderId().has_value()));

        TBase::ListSystemAttributes(attributes);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* cellBundle = GetThisImpl<TTabletCellBundle>();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        switch (key) {
            case EInternedAttributeKey::TabletBalancerConfig:
                BuildYsonFluently(consumer)
                    .Value(cellBundle->TabletBalancerConfig());
                return true;

            case EInternedAttributeKey::BundleControllerTargetConfig:
                if (cellBundle->GetBundleControllerTargetConfig()) {
                    consumer->OnRaw(*cellBundle->GetBundleControllerTargetConfig());
                    return true;
                }
                break;

            case EInternedAttributeKey::TabletActions:
                if (multicellManager->IsSecondaryMaster()) {
                    consumer->OnBeginList();
                    BuildActionsListFragment(consumer);
                    consumer->OnEndList();
                    return true;
                }
                break;

            case EInternedAttributeKey::ResourceLimits:
                Serialize(cellBundle->ResourceLimits(), consumer);
                return true;

            case EInternedAttributeKey::ViolatedResourceLimits: {
                const auto& limits = cellBundle->ResourceLimits();
                const auto& usage = cellBundle->ResourceUsage().Cluster();

                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("tablet_count").Value(usage.TabletCount > limits.TabletCount)
                        .Item("tablet_static_memory").Value(usage.TabletStaticMemory > limits.TabletStaticMemory)
                    .EndMap();

                return true;
            }

            case EInternedAttributeKey::ResourceUsage:
                Serialize(cellBundle->ResourceUsage().Cluster(), consumer);
                return true;

            case EInternedAttributeKey::Abc: {
                if (cellBundle->GetAbcConfig()) {
                    BuildYsonFluently(consumer)
                        .Value(*cellBundle->GetAbcConfig());
                    return true;
                } else {
                    return false;
                }
            }

            case EInternedAttributeKey::FolderId: {
                if (cellBundle->GetFolderId()) {
                    BuildYsonFluently(consumer)
                        .Value(cellBundle->GetFolderId().value());
                    return true;
                } else {
                    return false;
                }
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override
    {
        switch (key) {
            case EInternedAttributeKey::TabletActions: {
                YT_VERIFY(IsPrimaryMaster());

                auto* factory = GetEphemeralNodeFactory();
                auto builder = CreateBuilderFromFactory(factory);

                builder->OnBeginList();
                BuildActionsListFragment(builder.get());
                builder->OnEndList();
                auto node = builder->EndTree()->AsList();

                return FetchFromSwarm<IListNodePtr>(key)
                    .Apply(BIND([listNode = std::move(node)] (
                        const std::vector<IListNodePtr>& remoteActions)
                    {
                        TStringStream output;
                        TYsonWriter writer(&output, EYsonFormat::Binary, EYsonType::Node);

                        writer.OnBeginList();

                        BuildYsonListFragmentFluently(&writer)
                            .Items(listNode);

                        for (const auto& actionsNode : remoteActions) {
                            BuildYsonListFragmentFluently(&writer)
                                .Items(actionsNode);
                        }

                        writer.OnEndList();
                        writer.Flush();

                        return TYsonString(output.Str());
                    })
                    .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    void BuildActionsListFragment(IYsonConsumer* consumer)
    {
        const auto* cellBundle = GetThisImpl<TTabletCellBundle>();
        BuildYsonListFragmentFluently(consumer)
            .DoFor(cellBundle->TabletActions(), [] (TFluentList fluent, const TTabletAction* action) {
                fluent.Item().BeginMap()
                    .Item("tablet_action_id").Value(action->GetId())
                    .Item("kind").Value(action->GetKind())
                    .Item("state").Value(action->GetState())
                    .DoIf(!action->IsFinished(), [action] (TFluentMap fluent) {
                        fluent.Item("tablet_ids").DoListFor(
                            action->Tablets(), [] (TFluentList fluent, TTabletBase* tablet) {
                                fluent.Item().Value(tablet->GetId());
                            });
                    })
                    .DoIf(!action->Error().IsOK(), [action] (TFluentMap fluent) {
                        fluent.Item("error").Value(action->Error());
                    })
                    .Item("expiration_time").Value(action->GetExpirationTime())
                    .DoIf(action->GetExpirationTimeout().has_value(), [action] (TFluentMap fluent) {
                        fluent.Item("expiration_timeout").Value(*action->GetExpirationTimeout());
                    })
                    .DoIf(!action->GetCorrelationId().IsEmpty(), [action] (TFluentMap fluent) {
                        fluent.Item("correlation_id").Value(action->GetCorrelationId());
                    })
                .EndMap();
        });
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* cellBundle = GetThisImpl<TTabletCellBundle>();

        switch (key) {
            case EInternedAttributeKey::TabletBalancerConfig:
                cellBundle->TabletBalancerConfig() = ConvertTo<TMasterBundleTabletBalancerConfigPtr>(value);
                return true;

            case EInternedAttributeKey::BundleControllerTargetConfig:
                cellBundle->SetBundleControllerTargetConfig(value);
                return true;

            case EInternedAttributeKey::ResourceLimits: {
                cellBundle->ResourceLimits() = ConvertTo<TTabletResources>(value);
                return true;
            }

            case EInternedAttributeKey::Abc: {
                cellBundle->SetAbcConfig(ConvertTo<NObjectClient::TAbcConfigPtr>(value));
                return true;
            }

            case EInternedAttributeKey::FolderId: {
                TString newFolderId = ConvertTo<TString>(value);
                ValidateFolderId(newFolderId);
                cellBundle->SetFolderId(std::move(newFolderId));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* cellBundle = GetThisImpl<TTabletCellBundle>();

        switch (key) {
            case EInternedAttributeKey::Abc: {
                cellBundle->SetAbcConfig(nullptr);
                return true;
            }

            case EInternedAttributeKey::FolderId: {
                cellBundle->SetFolderId(std::nullopt);
                return true;
            }

            case EInternedAttributeKey::BundleControllerTargetConfig: {
                cellBundle->SetBundleControllerTargetConfig(std::nullopt);
                return true;
            }

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }

    DECLARE_YPATH_SERVICE_METHOD(NTabletClient::NProto, BalanceTabletCells)
    {
        DeclareMutating();

        auto movableTableIds = FromProto<std::vector<TTableId>>(request->movable_tables());
        bool keepActions = request->keep_actions();

        context->SetRequestInfo("TableIds: %v, KeepActions: %v",
            movableTableIds,
            keepActions);

        ValidateNoTransaction();

        auto* trunkNode = GetThisImpl<TTabletCellBundle>();

        std::vector<TTableNode*> movableTables;
        movableTables.reserve(movableTableIds.size());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto tableId : movableTableIds) {
            auto* node = objectManager->GetObjectOrThrow(tableId);
            if (node->GetType() != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Unexpected object type: expected %Qlv, got %Qlv",
                    EObjectType::Table,
                    node->GetType())
                    << TErrorAttribute("object_id", tableId);
            }
            movableTables.push_back(node->As<TTableNode>());
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto tabletActionIds = tabletManager->SyncBalanceCells(
            trunkNode,
            movableTables.empty() ? std::nullopt : std::optional(movableTables),
            keepActions);
        ToProto(response->mutable_tablet_actions(), tabletActionIds);

        context->Reply();
    }
};

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

