#include "cell_proxy_base.h"

#include "area.h"
#include "cell_base.h"
#include "private.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/cellar_agent/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NCellServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCellarAgent;
using namespace NCypressServer;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NTabletClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

void TCellProxyBase::ValidateRemoval()
{
    const auto* cell = GetThisImpl();

    ValidatePermission(cell->CellBundle().Get(), EPermission::Write);

    if (!cell->IsDecommissionCompleted()) {
        THROW_ERROR_EXCEPTION("Cannot remove tablet cell %v since it is not decommissioned on node",
            cell->GetId());
    }

    if (!cell->GossipStatus().Cluster().Decommissioned) {
        THROW_ERROR_EXCEPTION("Cannot remove chaos cell %v since it is not decommissioned on all masters",
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
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalHealth)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::Peers);
    descriptors->push_back(EInternedAttributeKey::ConfigVersion);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrerequisiteTransactionId)
        .SetPresent(cell->GetPrerequisiteTransaction()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellBundleId));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Area)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(EInternedAttributeKey::AreaId);
    descriptors->push_back(EInternedAttributeKey::TabletCellLifeStage);
    descriptors->push_back(EInternedAttributeKey::Status);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellStatus)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxChangelogId)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxSnapshotId)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::Suspended);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LeaseTransactionIds)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::RegisteredInCypress);
    descriptors->push_back(EInternedAttributeKey::PendingAclsUpdate);
}

bool TCellProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    const auto* cell = GetThisImpl();
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->CellHydraPersistenceSynchronizer;

    switch (key) {
        case EInternedAttributeKey::LeadingPeerId:
            BuildYsonFluently(consumer)
                .Value(cell->GetLeadingPeerId());
            return true;

        case EInternedAttributeKey::Health:
            // COMPAT(danilalexeev)
            if (config->UseHydraPersistenceDirectory &&
                multicellManager->IsPrimaryMaster() &&
                !cell->GetRegisteredInCypress())
            {
                 BuildYsonFluently(consumer)
                    .Value(ECellHealth::Initializing);
            } else if (multicellManager->IsMulticell()) {
                // COMPAT(akozhikhov).
                BuildYsonFluently(consumer)
                    .Value(cell->GetMulticellHealth());
            } else {
                BuildYsonFluently(consumer)
                    .Value(cell->GetHealth());
            }
            return true;

        case EInternedAttributeKey::LocalHealth:
            BuildYsonFluently(consumer)
                .Value(cell->GetHealth());
            return true;

        case EInternedAttributeKey::Peers:
            BuildYsonFluently(consumer)
                .DoListFor(cell->Peers(), [&] (TFluentList fluent, const TCellBase::TPeer& peer) {
                    if (peer.Descriptor.IsNull()) {
                        fluent
                            .Item().BeginMap()
                                .Item("state").Value(EPeerState::None)
                            .EndMap();
                    } else if (cell->IsAlienPeer(std::distance(cell->Peers().data(), &peer))) {
                        fluent
                            .Item().BeginMap()
                                .Item("address").Value(peer.Descriptor.GetDefaultAddress())
                                .Item("alien").Value(true)
                            .EndMap();
                    } else {
                        const auto* transaction = peer.PrerequisiteTransaction;
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
                                .DoIf(transaction, [&] (TFluentMap fluent) {
                                    fluent
                                        .Item("prerequisite_transaction").Value(transaction->GetId());
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
            if (!cell->CellBundle()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(cell->CellBundle()->GetName());
            return true;

        case EInternedAttributeKey::CellBundleId:
            if (!cell->CellBundle()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(cell->CellBundle()->GetId());
            return true;

        case EInternedAttributeKey::Area:
            if (!cell->GetArea()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(cell->GetArea()->GetName());
            return true;

        case EInternedAttributeKey::AreaId:
            if (!cell->GetArea()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(cell->GetArea()->GetId());
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

        case EInternedAttributeKey::MaxChangelogId: {
            BuildYsonFluently(consumer)
                .Value(cell->GetMaxChangelogId());
            return true;
        }

        case EInternedAttributeKey::MaxSnapshotId: {
            BuildYsonFluently(consumer)
                .Value(cell->GetMaxSnapshotId());
            return true;
        }

        case EInternedAttributeKey::Suspended:
            BuildYsonFluently(consumer)
                .Value(cell->GetSuspended());
            return true;

        case EInternedAttributeKey::LeaseTransactionIds:
            BuildYsonFluently(consumer)
                .Value(cell->LeaseTransactionIds());
            return true;

        case EInternedAttributeKey::RegisteredInCypress:
            BuildYsonFluently(consumer)
                .Value(cell->GetRegisteredInCypress());
            return true;

        case EInternedAttributeKey::PendingAclsUpdate:
            BuildYsonFluently(consumer)
                .Value(cell->GetPendingAclsUpdate());
            return true;

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TCellProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    const auto& cellManager = Bootstrap_->GetTamedCellManager();

    auto* cell = GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::Area: {
            auto areaName = ConvertTo<TString>(value);
            auto* area = cellManager->GetAreaByNameOrThrow(cell->CellBundle().Get(), areaName);
            cellManager->UpdateCellArea(cell, area);
            return true;
        }

        default:
            break;
    }

    return TBase::SetBuiltinAttribute(key, value, force);
}

TCellProxyBase::TResolveResult TCellProxyBase::Resolve(const TYPath& path, const IYPathServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();

    if (tokenizer.GetType() == NYPath::ETokenType::Ampersand) {
        return TBase::ResolveSelf(TYPath(tokenizer.GetSuffix()), context);
    }

    if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
        return ResolveSelf(TYPath(tokenizer.GetSuffix()), context);
    }

    tokenizer.Expect(NYPath::ETokenType::Slash);

    if (tokenizer.Advance() == NYPath::ETokenType::At) {
        return ResolveAttributes(TYPath(tokenizer.GetSuffix()), context);
    } else {
        return PropagateToHydraPersistenceStorage("/" + TYPath(tokenizer.GetInput()));
    }
}

TCellProxyBase::TResolveResult TCellProxyBase::ResolveSelf(const TYPath& path, const IYPathServiceContextPtr& context)
{
    const auto& method = context->GetMethod();
    if (method == "Remove" ||
        method == "Get" ||
        method == "Set" ||
        method == "Create" ||
        method == "Copy" ||
        method == "EndCopy")
    {
        return TResolveResultHere{path};
    } else {
        return PropagateToHydraPersistenceStorage(path);
    }
}

TCellProxyBase::TResolveResult TCellProxyBase::PropagateToHydraPersistenceStorage(const TYPath& pathSuffix) const
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto* cell = GetThisImpl();
    auto combinedPath = GetCellHydraPersistencePath(cell->GetId()) + pathSuffix;
    return TResolveResultThere{objectManager->GetRootService(), std::move(combinedPath)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
