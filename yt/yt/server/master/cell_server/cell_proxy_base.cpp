#include "cell_proxy_base.h"
#include "private.h"
#include "cell_base.h"
#include "tamed_cell_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/node_tracker_server/node.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/proto/ypath.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NCellServer {

using namespace NConcurrency;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NTabletClient;

using NYT::ToProto;
using ::ToString;

////////////////////////////////////////////////////////////////////////////////

TCellProxyBase::TCellProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TCellBase* cell)
    : TBase(bootstrap, metadata, cell)
{ }

void TCellProxyBase::ValidateRemoval()
{
    const auto* cell = GetThisImpl();

    ValidatePermission(cell->GetCellBundle(), EPermission::Write);

    if (!cell->IsDecommissionCompleted()) {
        THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it is not decommissioned on node",
            cell->GetId());
    }
}

void TCellProxyBase::RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context)
{
    auto* cell = GetThisImpl();
    if (cell->IsDecommissionCompleted()) {
        TBase::RemoveSelf(request, response, context);
    } else {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            THROW_ERROR_EXCEPTION("Tablet cell is the primary world object and cannot be removed by a secondary master");
        }

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->RemoveCell(cell, request->force());

        context->Reply();
    }
}

void TCellProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    const auto* cell = GetThisImpl();

    descriptors->push_back(EInternedAttributeKey::LeadingPeerId);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Health)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::Peers);
    descriptors->push_back(EInternedAttributeKey::ConfigVersion);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrerequisiteTransactionId)
        .SetPresent(cell->GetPrerequisiteTransaction()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(EInternedAttributeKey::TabletCellLifeStage);
    descriptors->push_back(EInternedAttributeKey::Status);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellStatus)
        .SetOpaque(true));
}

bool TCellProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    const auto* cell = GetThisImpl();
    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    switch (key) {
        case EInternedAttributeKey::LeadingPeerId:
            BuildYsonFluently(consumer)
                .Value(cell->GetLeadingPeerId());
            return true;

        case EInternedAttributeKey::Health:
            if (multicellManager->IsMulticell()) {
                BuildYsonFluently(consumer)
                    .Value(cell->GetMulticellHealth());
            } else {
                BuildYsonFluently(consumer)
                    .Value(cell->GetHealth());
            }
            return true;

        case EInternedAttributeKey::Peers:
            BuildYsonFluently(consumer)
                .DoListFor(cell->Peers(), [&] (TFluentList fluent, const TCellBase::TPeer& peer) {
                    if (peer.Descriptor.IsNull()) {
                        fluent
                            .Item().BeginMap()
                                .Item("state").Value(EPeerState::None)
                            .EndMap();
                    } else {
                        const auto* slot = peer.Node ? peer.Node->GetCellSlot(cell) : nullptr;
                        auto state = slot ? slot->PeerState : EPeerState::None;
                        fluent
                            .Item().BeginMap()
                                .Item("address").Value(peer.Descriptor.GetDefaultAddress())
                                .Item("state").Value(state)
                                .Item("last_seen_time").Value(peer.LastSeenTime)
                                .Item("last_seen_state").Value(peer.LastSeenState)
                                .DoIf(!peer.LastRevocationReason.IsOK(), [&] (auto fluent) {
                                    fluent
                                        .Item("last_revocation_reason").Value(peer.LastRevocationReason);
                                })
                            .EndMap();
                    }
                });
            return true;

        case EInternedAttributeKey::ConfigVersion:
            BuildYsonFluently(consumer)
                .Value(cell->GetConfigVersion());
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

        case EInternedAttributeKey::TabletCellLifeStage:
            BuildYsonFluently(consumer)
                .Value(cell->GetCellLifeStage());
            return true;

        case EInternedAttributeKey::Status:
            BuildYsonFluently(consumer)
                .Value(cell->GossipStatus().Cluster());
            return true;

        case EInternedAttributeKey::MulticellStatus:
            BuildYsonFluently(consumer)
                .DoMapFor(cell->GossipStatus().Multicell(), [&] (TFluentMap fluent, const auto& pair) {
                    fluent.Item(ToString(pair.first)).Value(pair.second);
                });
            return true;


        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
