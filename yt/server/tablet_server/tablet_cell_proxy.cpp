#include "tablet_cell_proxy.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/node_tracker_server/node.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellProxy
    : public TNonversionedObjectProxyBase<TTabletCell>
{
public:
    TTabletCellProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTabletCell* cell)
        : TBase(bootstrap, metadata, cell)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTabletCell> TBase;

    virtual void ValidateRemoval() override
    {
        const auto* cell = GetThisImpl();
        if (!cell->Tablets().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it has %v active tablet(s)",
                cell->GetId(),
                cell->Tablets().size());
        }
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* cell = GetThisImpl();

        descriptors->push_back(EInternedAttributeKey::LeadingPeerId);
        descriptors->push_back(EInternedAttributeKey::Health);
        descriptors->push_back(EInternedAttributeKey::Peers);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ActionIds)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::TabletCount);
        descriptors->push_back(EInternedAttributeKey::ConfigVersion);
        descriptors->push_back(EInternedAttributeKey::TotalStatistics);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrerequisiteTransactionId)
            .SetPresent(cell->GetPrerequisiteTransaction()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Decommissioned)
            .SetReplicated(true)
            .SetWritable(true));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* cell = GetThisImpl();

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        switch (key) {
            case EInternedAttributeKey::LeadingPeerId:
                BuildYsonFluently(consumer)
                    .Value(cell->GetLeadingPeerId());
                return true;

            case EInternedAttributeKey::Health:
                BuildYsonFluently(consumer)
                    .Value(cell->GetHealth());
                return true;

            case EInternedAttributeKey::Peers:
                BuildYsonFluently(consumer)
                    .DoListFor(cell->Peers(), [&] (TFluentList fluent, const TTabletCell::TPeer& peer) {
                        if (peer.Descriptor.IsNull()) {
                            fluent
                                .Item().BeginMap()
                                    .Item("state").Value(EPeerState::None)
                                .EndMap();
                        } else {
                            const auto* slot = peer.Node ? peer.Node->GetTabletSlot(cell) : nullptr;
                            auto state = slot ? slot->PeerState : EPeerState::None;
                            fluent
                                .Item().BeginMap()
                                    .Item("address").Value(peer.Descriptor.GetDefaultAddress())
                                    .Item("state").Value(state)
                                    .Item("last_seen_time").Value(peer.LastSeenTime)
                                .EndMap();
                        }
                    });
                return true;

            case EInternedAttributeKey::TabletIds:
                BuildYsonFluently(consumer)
                    .DoListFor(cell->Tablets(), [] (TFluentList fluent, const TTablet* tablet) {
                        fluent
                            .Item().Value(tablet->GetId());
                    });
                return true;

            case EInternedAttributeKey::ActionIds:
                BuildYsonFluently(consumer)
                    .DoListFor(cell->Actions(), [] (TFluentList fluent, const TTabletAction* action) {
                        fluent
                            .Item().Value(action->GetId());
                    });
                return true;

            case EInternedAttributeKey::TabletCount:
                BuildYsonFluently(consumer)
                    .Value(cell->Tablets().size());
                return true;

            case EInternedAttributeKey::ConfigVersion:
                BuildYsonFluently(consumer)
                    .Value(cell->GetConfigVersion());
                return true;

            case EInternedAttributeKey::TotalStatistics:
                BuildYsonFluently(consumer)
                    .Value(New<TSerializableTabletCellStatistics>(
                        cell->TotalStatistics(),
                        chunkManager));
                return true;

            case EInternedAttributeKey::PrerequisiteTransactionId:
                if (!cell->GetPrerequisiteTransaction()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(cell->GetPrerequisiteTransaction()->GetId());
                return true;

            case EInternedAttributeKey::TabletCellBundle:
                if (!cell->GetCellBundle()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(cell->GetCellBundle()->GetName());
                return true;

            case EInternedAttributeKey::Decommissioned:
                BuildYsonFluently(consumer)
                    .Value(cell->GetDecommissioned());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value)
    {
        auto* cell = GetThisImpl();
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        switch (key) {
            case EInternedAttributeKey::Decommissioned: {
                ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

                auto decommissioned = ConvertTo<bool>(value);

                if (decommissioned) {
                    tabletManager->DecomissionTabletCell(cell);
                } else if (cell->GetDecommissioned()) {
                    THROW_ERROR_EXCEPTION("Tablet cell cannot be undecommissioned");
                }

                return true;
            }
        }

        return TBase::SetBuiltinAttribute(key, value);
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

} // namespace NTabletServer
} // namespace NYT

