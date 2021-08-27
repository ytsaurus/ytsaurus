#include "area_proxy.h"
#include "cell_bundle.h"
#include "area.h"
#include "bundle_node_tracker.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/object_server/object_detail.h>
#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/type_handler.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TAreaProxy
    : public TNonversionedObjectProxyBase<TArea>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TArea>;

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);

        const auto* area = GetThisImpl();
        if (!area->Cells().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove area %Qv since it has %v cell(s)",
                area->GetName(),
                area->Cells().size());
        }
    }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);
        const auto* area = GetThisImpl();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::NodeTagFilter)
            .SetWritable(true)
            .SetReplicated(true)
            .SetPresent(!area->NodeTagFilter().IsEmpty()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellBundle)
            .SetPresent(area->GetCellBundle()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellBundleId)
            .SetReplicated(true)
            .SetMandatory(true)
            .SetPresent(area->GetCellBundle()));
        descriptors->push_back(EInternedAttributeKey::CellIds);
        descriptors->push_back(EInternedAttributeKey::Nodes);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* area = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(area->GetName());
                return true;

            case EInternedAttributeKey::NodeTagFilter:
                if (area->NodeTagFilter().IsEmpty()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(area->NodeTagFilter().GetFormula());
                return true;

            case EInternedAttributeKey::CellBundle:
                if (!area->GetCellBundle()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(area->GetCellBundle()->GetName());
                return true;

            case EInternedAttributeKey::CellBundleId:
                if (!area->GetCellBundle()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(area->GetCellBundle()->GetId());
                return true;

            case EInternedAttributeKey::CellIds:
                BuildYsonFluently(consumer)
                    .DoListFor(area->Cells(), [] (TFluentList fluent, const TCellBase* cell) {
                        fluent
                            .Item().Value(cell->GetId());
                    });
                return true;

            case EInternedAttributeKey::Nodes: {
                const auto& bundleTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
                BuildYsonFluently(consumer)
                    .DoListFor(bundleTracker->GetAreaNodes(area), [] (TFluentList fluent, const TNode* node) {
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

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* area = GetThisImpl();
        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        switch (key) {
            case EInternedAttributeKey::Name: {
                auto newName = ConvertTo<TString>(value);
                cellManager->RenameArea(area, newName);
                return true;
            }

            case EInternedAttributeKey::NodeTagFilter: {
                auto formula = ConvertTo<TString>(value);
                cellManager->SetAreaNodeTagFilter(area, ConvertTo<TString>(value));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

IObjectProxyPtr CreateAreaProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TArea* area)
{
    return New<TAreaProxy>(bootstrap, metadata, area);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer

