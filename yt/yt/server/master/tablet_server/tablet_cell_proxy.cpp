#include "tablet_cell_proxy.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/node_detail.h>

#include <yt/server/master/node_tracker_server/node.h>

#include <yt/server/master/cell_server/cell_proxy_base.h>
#include <yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/proto/ypath.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

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
using namespace NCypressServer;

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
        /* enableRaw */ false,
        /* booleanAsString */ false);

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
    TTabletCellProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTabletCell* cell)
        : TBase(bootstrap, metadata, cell)
    { }

private:
    typedef TCellProxyBase TBase;

    virtual void ValidateRemoval() override
    {
        const auto* cell = GetThisImpl<TTabletCell>();

        ValidatePermission(cell->GetCellBundle(), EPermission::Write);

        if (!cell->IsDecommissionCompleted()) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it is not decommissioned on node",
                cell->GetId());
        }

        if (!cell->GossipStatus().Cluster().Decommissioned) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it is not decommissioned on all masters",
                cell->GetId());
        }

        if (cell->GossipStatistics().Cluster().TabletCount != 0) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it has active tablet(s)",
                cell->GetId());
        }
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxChangelogId)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxSnapshotId)
            .SetOpaque(true));
    }

    int GetMaxHydraFileId(const TYPath& path) const
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto* node = cypressManager->ResolvePathToTrunkNode(path);
        if (node->GetType() != EObjectType::MapNode) {
            THROW_ERROR_EXCEPTION("Unexpected node type: expected %Qlv, got %Qlv",
                EObjectType::MapNode,
                node->GetType())
                << TErrorAttribute("path", path);
        }
        auto* mapNode = node->As<TMapNode>();

        int maxId = -1;
        for (const auto& [key, child] : mapNode->KeyToChild()) {
            int id;
            if (TryFromString<int>(key, id)) {
                maxId = std::max(maxId, id);
            }
        }

        return maxId;
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* cell = GetThisImpl<TTabletCell>();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        switch (key) {
            case EInternedAttributeKey::TabletIds:
                if (multicellManager->IsSecondaryMaster()) {
                    BuildYsonFluently(consumer)
                        .DoListFor(cell->Tablets(), [] (TFluentList fluent, const TTablet* tablet) {
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

            case EInternedAttributeKey::TabletCount: {
                if (multicellManager->IsSecondaryMaster()) {
                    BuildYsonFluently(consumer)
                        .Value(cell->Tablets().size());
                    return true;
                }
                break;
            }

            case EInternedAttributeKey::TotalStatistics: {
                // COMPAT(savrus)
                auto statistics = cell->GossipStatistics().Cluster();
                statistics.Health = cell->GossipStatus().Cluster().Health;
                BuildYsonFluently(consumer)
                    .Value(New<TSerializableTabletCellStatistics>(
                        statistics,
                        chunkManager));
                return true;
            }

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
                if (!cell->GetCellBundle()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(cell->GetCellBundle()->GetName());
                return true;

            case EInternedAttributeKey::MaxChangelogId: {
                auto changelogPath = Format("//sys/tablet_cells/%v/changelogs", cell->GetId());
                int maxId = GetMaxHydraFileId(changelogPath);
                BuildYsonFluently(consumer)
                    .Value(maxId);
                return true;
            }

            case EInternedAttributeKey::MaxSnapshotId: {
                auto snapshotPath = Format("//sys/tablet_cells/%v/snapshots", cell->GetId());
                int maxId = GetMaxHydraFileId(snapshotPath);
                BuildYsonFluently(consumer)
                    .Value(maxId);
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override
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

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
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
                return TBase::SetBuiltinAttribute(key, value);
        }
    }

    virtual bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
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
};

IObjectProxyPtr CreateTabletCellProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTabletCell* cell)
{
    return New<TTabletCellProxy>(bootstrap, metadata, cell);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
