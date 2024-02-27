#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "cell_bundle_proxy.h"
#include "tamed_cell_manager.h"
#include "area.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/security_server/config.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

namespace NYT::NCellServer {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NTableServer;
using namespace NObjectServer;
using namespace NNodeTrackerServer;
using namespace NSecurityServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

void TCellBundleProxy::ValidateRemoval()
{
    const auto* cellBundle = GetThisImpl();
    if (!cellBundle->Cells().empty()) {
        THROW_ERROR_EXCEPTION("Cannot remove cell bundle %Qv since it has %v active cell(s)",
            cellBundle->GetName(),
            cellBundle->Cells().size());
    }

    if (!cellBundle->Areas().empty()) {
        THROW_ERROR_EXCEPTION("Cannot remove cell bundle %Qv since it has %v areas",
            cellBundle->GetName(),
            cellBundle->Areas().size());
    }
}

void TCellBundleProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes)
{
    const auto* cellBundle = GetThisImpl();

    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Options)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true));
    attributes->push_back(EInternedAttributeKey::ConfigVersion);
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::DynamicOptions)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true));
    attributes->push_back(EInternedAttributeKey::DynamicConfigVersion);
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::NodeTagFilter)
        .SetWritable(true)
        .SetReplicated(true)
        .SetPresent(cellBundle->Areas().size() == 1 && !cellBundle->Areas().begin()->second->NodeTagFilter().IsEmpty()));
    attributes->push_back(EInternedAttributeKey::TabletCellCount);
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellIds)
        .SetOpaque(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::CellBalancerConfig)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true)
        .SetWritePermission(EPermission::Use));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Nodes)
        .SetPresent(cellBundle->Areas().size() == 1)
        .SetOpaque(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Health)
        .SetReplicated(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Areas)
        .SetOpaque(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::AreaNodes)
        .SetOpaque(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::ChangelogAccountViolatedResourceLimits)
        .SetOpaque(true));
    attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::SnapshotAccountViolatedResourceLimits)
        .SetOpaque(true));

    TBase::ListSystemAttributes(attributes);
}

bool TCellBundleProxy::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* cellBundle = GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::Name:
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetName());
            return true;

        case EInternedAttributeKey::Options:
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetOptions());
            return true;

        case EInternedAttributeKey::ConfigVersion:
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetConfigVersion());
            return true;

        case EInternedAttributeKey::DynamicOptions:
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetDynamicOptions());
            return true;

        case EInternedAttributeKey::DynamicConfigVersion:
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetDynamicConfigVersion());
            return true;

        case EInternedAttributeKey::NodeTagFilter: {
            if (cellBundle->Areas().size() != 1) {
                break;
            }
            auto* area = cellBundle->Areas().begin()->second;
            if (area->NodeTagFilter().IsEmpty()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(area->NodeTagFilter().GetFormula());
            return true;
        }

        case EInternedAttributeKey::TabletCellIds:
            BuildYsonFluently(consumer)
                .DoListFor(cellBundle->Cells(), [] (TFluentList fluent, const TCellBase* cell) {
                    fluent
                        .Item().Value(cell->GetId());
                });
            return true;

        case EInternedAttributeKey::TabletCellCount:
            BuildYsonFluently(consumer)
                .Value(cellBundle->Cells().size());
            return true;

        case EInternedAttributeKey::CellBalancerConfig:
            BuildYsonFluently(consumer)
                .Value(cellBundle->CellBalancerConfig());
            return true;

        case EInternedAttributeKey::Nodes: {
            if (cellBundle->Areas().size() != 1) {
                return false;
            }
            const auto& bundleTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
            BuildYsonFluently(consumer)
                .DoListFor(bundleTracker->GetAreaNodes(cellBundle->Areas().begin()->second), [] (TFluentList fluent, const TNode* node) {
                    fluent
                        .Item().Value(node->GetDefaultAddress());
                });
            return true;
        }
        case EInternedAttributeKey::Health:
            BuildYsonFluently(consumer)
                .Value(cellBundle->Health());
            return true;

        case EInternedAttributeKey::Areas: {
            BuildYsonFluently(consumer)
                .DoMapFor(cellBundle->Areas(), [] (TFluentMap fluent, const auto& pair) {
                    const auto* area = pair.second;
                    fluent
                        .Item(ToString(area->GetName()))
                        .BeginMap()
                            .Item("id").Value(area->GetId())
                            .Item("cell_count").Value(std::ssize(area->Cells()))
                            .Item("node_tag_filter").Value(area->NodeTagFilter().GetFormula())
                        .EndMap();
                });
            return true;
        }

        case EInternedAttributeKey::AreaNodes: {
            const auto& bundleTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
            BuildYsonFluently(consumer)
                .DoMapFor(cellBundle->Areas(), [&] (TFluentMap fluent, const auto& pair) {
                    const auto* area = pair.second;
                    fluent
                        .Item(area->GetName())
                        .DoListFor(bundleTracker->GetAreaNodes(area), [] (TFluentList fluent, const TNode* node) {
                            fluent
                                .Item().Value(node->GetDefaultAddress());
                        });
                });
            return true;
        }

        case EInternedAttributeKey::ChangelogAccountViolatedResourceLimits: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            const auto& securityManager = Bootstrap_->GetSecurityManager();

            auto bundleOptions = cellBundle->GetOptions();
            auto* account = securityManager->GetAccountByNameOrThrow(
                bundleOptions->ChangelogAccount,
                /*activeLifeStageOnly*/ true);
            auto* medium = chunkManager->GetMediumByNameOrThrow(bundleOptions->ChangelogPrimaryMedium);

            DoSerializeAccountViolatedResourceLimits(account, medium, consumer);

            return true;
        }

        case EInternedAttributeKey::SnapshotAccountViolatedResourceLimits: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            const auto& securityManager = Bootstrap_->GetSecurityManager();

            auto bundleOptions = cellBundle->GetOptions();
            auto* account = securityManager->GetAccountByNameOrThrow(
                bundleOptions->SnapshotAccount,
                /*activeLifeStageOnly*/ true);
            auto* medium = chunkManager->GetMediumByNameOrThrow(bundleOptions->SnapshotPrimaryMedium);

            DoSerializeAccountViolatedResourceLimits(account, medium, consumer);

            return true;
        }

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TCellBundleProxy::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    const auto& cellManager = Bootstrap_->GetTamedCellManager();

    auto* cellBundle = GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::Name: {
            auto newName = ConvertTo<TString>(value);
            cellManager->RenameCellBundle(cellBundle, newName);
            return true;
        }

        case EInternedAttributeKey::Options: {
            auto options = ConvertTo<TTabletCellOptionsPtr>(value);
            cellManager->SetCellBundleOptions(cellBundle, options);
            return true;
        }

        case EInternedAttributeKey::DynamicOptions: {
            auto options = ConvertTo<TDynamicTabletCellOptionsPtr>(value);
            cellBundle->SetDynamicOptions(options);
            return true;
        }

        case EInternedAttributeKey::NodeTagFilter: {
            if (cellBundle->Areas().size() != 1) {
                THROW_ERROR_EXCEPTION("Unable to identify unique area for bundle %Qv",
                    cellBundle->GetName());
            }
            auto* area = cellBundle->Areas().begin()->second;
            auto formula = ConvertTo<TString>(value);
            cellManager->SetAreaNodeTagFilter(area, ConvertTo<TString>(value));
            return true;
        }

        case EInternedAttributeKey::CellBalancerConfig:
            cellBundle->CellBalancerConfig() = ConvertTo<TCellBalancerConfigPtr>(value);
            return true;

        default:
            break;
    }

    return TBase::SetBuiltinAttribute(key, value, force);
}

void TCellBundleProxy::DoSerializeAccountViolatedResourceLimits(
    TAccount* account,
    TMedium* medium,
    IYsonConsumer* consumer) const
{
    auto enableTabletResourceValidation =
        Bootstrap_->GetConfigManager()->GetConfig()->SecurityManager->EnableTabletResourceValidation;
    auto violatedResourceLimits = account->GetViolatedResourceLimits(
        Bootstrap_,
        enableTabletResourceValidation);

    // NB: Filter out master memory and irrelevant media violations.
    violatedResourceLimits.SetMasterMemory({});

    SerializeViolatedClusterResourceLimitsInBooleanFormat(
        violatedResourceLimits,
        consumer,
        Bootstrap_,
        /*serializeDiskSpace*/ false,
        /*relevantMediumIndex*/ medium->GetIndex());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
