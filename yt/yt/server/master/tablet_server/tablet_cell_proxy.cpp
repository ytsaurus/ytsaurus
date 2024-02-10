#include "tablet_cell_proxy.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_action.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/cell_server/cell_proxy_base.h>
#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/string/cast.h>

namespace NYT::NTabletServer {

using namespace NConcurrency;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NRpc;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NTabletClient;
using namespace NCellServer;

using NYT::ToProto;
using ::ToString;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TYsonString CombineObjectIds(
    const std::vector<TObjectId>& objectIds,
    const std::vector<std::vector<TObjectId>>& remoteObjectIds)
{
    TString result;
    TStringOutput stringOutput(result);

    auto writer = CreateYsonWriter(
        &stringOutput,
        EYsonFormat::Binary,
        EYsonType::Node,
        /*enableRaw*/ false);

    BuildYsonFluently(writer.get())
        .BeginList()
        .DoFor(objectIds, [=] (TFluentList fluent, const auto objectId) {
            fluent.Item().Value(objectId);
        })
        .DoFor(remoteObjectIds, [=] (TFluentList fluent, const auto& objectIds) {
            fluent
            .DoFor(objectIds, [=] (TFluentList fluent, const auto objectId) {
                fluent.Item().Value(objectId);
            });
        })
        .EndList();

    writer->Flush();

    return TYsonString(result);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TTabletCellProxy
    : public TCellProxyBase
{
public:
    using TCellProxyBase::TCellProxyBase;

private:
    using TBase = TCellProxyBase;

    void ValidateRemoval() override
    {
        const auto* cell = GetThisImpl<TTabletCell>();

        if (cell->GossipStatistics().Cluster().TabletCount != 0) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it has active tablet(s)",
                cell->GetId());
        }

        TBase::ValidateRemoval();
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ActionIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCount)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::TotalStatistics);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellStatistics)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PeerCount)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Tablets)
            .SetOpaque(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* cell = GetThisImpl<TTabletCell>();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        switch (key) {
            case EInternedAttributeKey::TabletIds:
                if (multicellManager->IsSecondaryMaster()) {
                    BuildYsonFluently(consumer)
                        .DoListFor(cell->Tablets(), [] (TFluentList fluent, const TTabletBase* tablet) {
                            fluent
                                .Item().Value(tablet->GetId());
                        });
                    return true;
                }
                break;

            case EInternedAttributeKey::ActionIds:
                if (multicellManager->IsSecondaryMaster()) {
                    BuildYsonFluently(consumer)
                        .DoListFor(cell->Actions(), [] (TFluentList fluent, const TTabletAction* action) {
                            fluent
                                .Item().Value(action->GetId());
                        });
                    return true;
                }
                break;

            case EInternedAttributeKey::TabletCount:
                if (multicellManager->IsSecondaryMaster()) {
                    BuildYsonFluently(consumer)
                        .Value(cell->Tablets().size());
                    return true;
                }
                break;

            case EInternedAttributeKey::Tablets:
                if (multicellManager->IsSecondaryMaster()) {
                    consumer->OnBeginMap();
                    BuildTabletsMapFragment(consumer);
                    consumer->OnEndMap();
                    return true;
                }
                break;

            case EInternedAttributeKey::TotalStatistics:
                BuildYsonFluently(consumer)
                    .Value(New<TSerializableTabletCellStatistics>(
                        cell->GossipStatistics().Cluster(),
                        chunkManager));
                return true;

            case EInternedAttributeKey::MulticellStatistics:
                BuildYsonFluently(consumer)
                    .DoMapFor(cell->GossipStatistics().Multicell(), [&] (TFluentMap fluent, const auto& pair) {
                        auto serializableStatistics = New<TSerializableTabletCellStatistics>(
                            pair.second,
                            chunkManager);
                        fluent.Item(ToString(pair.first)).Value(serializableStatistics);
                    });
                return true;

            case EInternedAttributeKey::TabletCellBundle:
                if (!cell->CellBundle()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(cell->CellBundle()->GetName());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override
    {
        const auto* cell = GetThisImpl<TTabletCell>();

        switch (key) {
            case EInternedAttributeKey::TabletCount: {
                YT_VERIFY(IsPrimaryMaster());

                int tabletCount = cell->Tablets().size();
                return FetchFromSwarm<int>(key)
                    .Apply(BIND([tabletCount] (const std::vector<int>& tabletCounts) {
                        auto totalTabletCount = std::accumulate(
                            tabletCounts.begin(),
                            tabletCounts.end(),
                            tabletCount);
                        return ConvertToYsonString(totalTabletCount);
                    })
                    .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
            }

            case EInternedAttributeKey::TabletIds: {
                YT_VERIFY(IsPrimaryMaster());

                std::vector<TTabletId> tabletIds;
                for (const auto* tablet : cell->Tablets()) {
                    tabletIds.push_back(tablet->GetId());
                }

                return FetchFromSwarm<std::vector<TTabletId>>(key)
                    .Apply(BIND([tabletIds = std::move(tabletIds)] (const std::vector<std::vector<TTabletId>>& remoteTabletIds) {
                        return NDetail::CombineObjectIds(tabletIds, remoteTabletIds);
                    })
                    .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
            }

            case EInternedAttributeKey::ActionIds: {
                YT_VERIFY(IsPrimaryMaster());

                std::vector<TTabletActionId> actionIds;
                for (const auto* action : cell->Actions()) {
                    actionIds.push_back(action->GetId());
                }

                return FetchFromSwarm<std::vector<TTabletActionId>>(key)
                    .Apply(BIND([actionIds = std::move(actionIds)] (const std::vector<std::vector<TTabletActionId>>& remoteActionIds) {
                        return NDetail::CombineObjectIds(actionIds, remoteActionIds);
                    })
                    .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
            }

            case EInternedAttributeKey::Tablets: {
                YT_VERIFY(IsPrimaryMaster());

                auto* factory = GetEphemeralNodeFactory();
                auto builder = CreateBuilderFromFactory(factory);

                builder->OnBeginMap();
                BuildTabletsMapFragment(builder.get());
                builder->OnEndMap();
                auto node = builder->EndTree()->AsMap();

                return FetchFromSwarm<IMapNodePtr>(key)
                    .Apply(BIND([mapNode = std::move(node)] (
                        const std::vector<IMapNodePtr>& remoteTablets)
                    {
                        TStringStream output;
                        TYsonWriter writer(&output, EYsonFormat::Pretty, EYsonType::Node);

                        writer.OnBeginMap();

                        BuildYsonMapFragmentFluently(&writer)
                            .DoFor(mapNode->GetChildren(), [] (auto fluent, const auto& pair) {
                                fluent.Item(pair.first).Value(pair.second);
                        });

                        for (const auto& tabletsNode : remoteTablets) {
                            BuildYsonMapFragmentFluently(&writer)
                                .DoFor(tabletsNode->GetChildren(), [] (auto fluent, const auto& pair) {
                                    fluent.Item(pair.first).Value(pair.second);
                            });
                        }
                        writer.OnEndMap();
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

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* cell = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::PeerCount:
                if (cell->PeerCount()) {
                    THROW_ERROR_EXCEPTION("Peer count for cell %v is already set",
                        cell->GetId());
                }

                ValidateNoTransaction();
                Bootstrap_->GetTamedCellManager()->UpdatePeerCount(cell, ConvertTo<int>(value));
                return true;
            default:
                return TBase::SetBuiltinAttribute(key, value, force);
        }
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* cell = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::PeerCount:
                ValidateNoTransaction();
                Bootstrap_->GetTamedCellManager()->UpdatePeerCount(cell, std::nullopt);
                return true;
            default:
                return TBase::RemoveBuiltinAttribute(key);
        }
    }

    void BuildTabletsMapFragment(IYsonConsumer* consumer)
    {
        const auto* cell = GetThisImpl<TTabletCell>();
        BuildYsonMapFragmentFluently(consumer)
            .DoFor(cell->Tablets(), [] (TFluentMap fluent, const TTabletBase* tabletBase) {
                if (!IsObjectAlive(tabletBase) || tabletBase->GetType() != EObjectType::Tablet) {
                    return;
                }

                auto* tablet = tabletBase->As<TTablet>();
                auto* table = tablet->GetTable();
                fluent
                    .Item(ToString(tablet->GetId()))
                        .BeginMap()
                            .Item("table_id").Value(table->GetId())
                        .EndMap();
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateTabletCellProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTabletCell* cell)
{
    return New<TTabletCellProxy>(bootstrap, metadata, cell);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
