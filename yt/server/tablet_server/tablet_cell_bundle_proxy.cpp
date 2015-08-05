#include "stdafx.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleProxy
    : public TNonversionedObjectProxyBase<TTabletCellBundle>
{
public:
    TTabletCellBundleProxy(NCellMaster::TBootstrap* bootstrap, TTabletCellBundle* bundle)
        : TBase(bootstrap, bundle)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTabletCellBundle> TBase;

    virtual void ValidateRemoval() override
    {
        const auto* bundle = GetThisTypedImpl();
        if (!bundle->TabletCells().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove tablet cell bundle %Qv since it has %v active tablet cell(s)",
                bundle->GetName(),
                bundle->TabletCells().size());
        }
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        attributes->push_back("options");
        attributes->push_back("tablet_cell_count");
        attributes->push_back(TAttributeDescriptor("tablet_cell_ids")
            .SetPresent(true)
            .SetOpaque(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        const auto* bundle = GetThisTypedImpl();

        if (key == "options") {
            BuildYsonFluently(consumer)
              .Value(bundle->GetOptions());
            return true;
        }

        if (key == "tablet_cell_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(bundle->TabletCells(), [] (TFluentList fluent, const TTabletCell* cell) {
                    fluent
                        .Item().Value(cell->GetId());
                });
            return true;
        }

        if (key == "tablet_cell_count") {
            BuildYsonFluently(consumer)
                .Value(bundle->TabletCells().size());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

IObjectProxyPtr CreateTabletCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    TTabletCellBundle* bundle)
{
    return New<TTabletCellBundleProxy>(bootstrap, bundle);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

