#include "config.h"
#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_manager.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

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

        attributes->push_back(TAttributeDescriptor("name")
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        attributes->push_back(TAttributeDescriptor("options")
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        attributes->push_back(TAttributeDescriptor("node_tag_filter")
            .SetWritable(true)
            .SetReplicated(true)
            .SetPresent(!cellBundle->NodeTagFilter().IsEmpty()));
        attributes->push_back(TAttributeDescriptor("tablet_balancer_config")
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        attributes->push_back("tablet_cell_count");
        attributes->push_back(TAttributeDescriptor("tablet_cell_ids")
            .SetOpaque(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto* cellBundle = GetThisImpl();

        if (key == "name") {
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetName());
            return true;
        }

        if (key == "options") {
            BuildYsonFluently(consumer)
                .Value(cellBundle->GetOptions());
            return true;
        }

        if (key == "node_tag_filter" && !cellBundle->NodeTagFilter().IsEmpty()) {
            BuildYsonFluently(consumer)
                .Value(cellBundle->NodeTagFilter().GetFormula());
            return true;
        }

        if (key == "tablet_cell_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(cellBundle->TabletCells(), [] (TFluentList fluent, const TTabletCell* cell) {
                    fluent
                        .Item().Value(cell->GetId());
                });
            return true;
        }

        if (key == "tablet_cell_count") {
            BuildYsonFluently(consumer)
                .Value(cellBundle->TabletCells().size());
            return true;
        }

        if (key == "tablet_balancer_config") {
            BuildYsonFluently(consumer)
                .Value(cellBundle->TabletBalancerConfig());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto* cellBundle = GetThisImpl();

        if (key == "name") {
            auto newName = ConvertTo<TString>(value);
            tabletManager->RenameTabletCellBundle(cellBundle, newName);
            return true;
        }

        if (key == "options") {
            auto options = ConvertTo<TTabletCellOptionsPtr>(value);
            if (!cellBundle->TabletCells().empty()) {
                THROW_ERROR_EXCEPTION("Cannot change options since tablet cell bundle has %v tablet cell(s)",
                    cellBundle->TabletCells().size());
            }
            cellBundle->SetOptions(options);
            return true;
        }

        if (key == "node_tag_filter") {
            auto formula = ConvertTo<TString>(value);
            cellBundle->NodeTagFilter() = MakeBooleanFormula(formula);
            return true;
        }

        if (key == "tablet_balancer_config") {
            cellBundle->TabletBalancerConfig() = ConvertTo<TTabletBalancerConfigPtr>(value);
            return true;
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

