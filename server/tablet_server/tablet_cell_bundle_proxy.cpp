#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_manager.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/object_server/interned_attributes.h>
#include <yt/server/object_server/object_detail.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/node_tracker_server/node.h>

#include <yt/ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleProxy
    : public TNonversionedObjectProxyBase<TTabletCellBundle>
{
public:
    TTabletCellBundleProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTabletCellBundle* cellBundle)
        : TBase(bootstrap, metadata, cellBundle)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTabletCellBundle> TBase;

    virtual void ValidateRemoval() override
    {
        const auto* cellBundle = GetThisImpl();
        if (!cellBundle->TabletCells().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell bundle %Qv since it has %v active tablet cell(s)",
                cellBundle->GetName(),
                cellBundle->TabletCells().size());
        }
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
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
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::NodeTagFilter)
            .SetWritable(true)
            .SetReplicated(true)
            .SetPresent(!cellBundle->NodeTagFilter().IsEmpty()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletBalancerConfig)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        attributes->push_back(EInternedAttributeKey::TabletCellCount);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellIds)
            .SetOpaque(true));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Nodes)
            .SetOpaque(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
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

            case EInternedAttributeKey::NodeTagFilter:
                if (cellBundle->NodeTagFilter().IsEmpty()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(cellBundle->NodeTagFilter().GetFormula());
                return true;

            case EInternedAttributeKey::TabletCellIds:
                BuildYsonFluently(consumer)
                    .DoListFor(cellBundle->TabletCells(), [] (TFluentList fluent, const TTabletCell* cell) {
                        fluent
                            .Item().Value(cell->GetId());
                    });
                return true;

            case EInternedAttributeKey::TabletCellCount:
                BuildYsonFluently(consumer)
                    .Value(cellBundle->TabletCells().size());
                return true;

            case EInternedAttributeKey::TabletBalancerConfig:
                BuildYsonFluently(consumer)
                    .Value(cellBundle->TabletBalancerConfig());
                return true;

            case EInternedAttributeKey::Nodes: {
                const auto& bundleTracker = Bootstrap_->GetTabletManager()->GetBundleNodeTracker();
                BuildYsonFluently(consumer)
                    .DoListFor(bundleTracker->GetBundleNodes(cellBundle), [] (TFluentList fluent, const TNode* node) {
                        fluent
                            .Item().Value(node->GetDefaultAddress());
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
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto* cellBundle = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name: {
                auto newName = ConvertTo<TString>(value);
                tabletManager->RenameTabletCellBundle(cellBundle, newName);
                return true;
            }

            case EInternedAttributeKey::Options: {
                auto options = ConvertTo<TTabletCellOptionsPtr>(value);
                if (!cellBundle->TabletCells().empty()) {
                    THROW_ERROR_EXCEPTION("Cannot change options since tablet cell bundle has %v tablet cell(s)",
                        cellBundle->TabletCells().size());
                }
                cellBundle->SetOptions(options);
                return true;
            }

            case EInternedAttributeKey::NodeTagFilter: {
                auto formula = ConvertTo<TString>(value);
                tabletManager->SetTabletCellBundleNodeTagFilter(cellBundle, ConvertTo<TString>(value));
                return true;
            }

            case EInternedAttributeKey::TabletBalancerConfig:
                cellBundle->TabletBalancerConfig() = ConvertTo<TTabletBalancerConfigPtr>(value);
                return true;

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

IObjectProxyPtr CreateTabletCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTabletCellBundle* cellBundle)
{
    return New<TTabletCellBundleProxy>(bootstrap, metadata, cellBundle);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

