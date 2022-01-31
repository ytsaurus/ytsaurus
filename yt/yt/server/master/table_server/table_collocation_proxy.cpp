#include "table_collocation_proxy.h"
#include "table_collocation.h"
#include "table_manager.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/table_server/replicated_table_node.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TTableCollocationProxy
    : public TNonversionedObjectProxyBase<TTableCollocation>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TTableCollocation>;

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        attributes->push_back(EInternedAttributeKey::ExternalCellTag);
        attributes->push_back(EInternedAttributeKey::TableIds);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TablePaths)
            .SetOpaque(true));
        attributes->push_back(EInternedAttributeKey::CollocationType);

        TBase::ListSystemAttributes(attributes);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* collocation = GetThisImpl();

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        switch (key) {
            case EInternedAttributeKey::ExternalCellTag:
                BuildYsonFluently(consumer)
                    .Value(collocation->GetExternalCellTag());
                return true;

            case EInternedAttributeKey::TableIds:
                BuildYsonFluently(consumer)
                    .DoListFor(collocation->Tables(), [] (TFluentList fluent, TTableNode* table) {
                        if (!IsObjectAlive(table)) {
                            return;
                        }
                        fluent
                            .Item().Value(table->GetId());
                    });
                return true;

            case EInternedAttributeKey::TablePaths:
                BuildYsonFluently(consumer)
                    .DoListFor(collocation->Tables(), [&] (TFluentList fluent, TTableNode* table) {
                        if (!IsObjectAlive(table)) {
                            return;
                        }
                        fluent
                            .Item().Value(cypressManager->GetNodePath(table, nullptr));
                    });
                return true;

            case EInternedAttributeKey::CollocationType:
                BuildYsonFluently(consumer)
                    .Value(collocation->GetType());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateTableCollocationProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTableCollocation* collocation)
{
    return New<TTableCollocationProxy>(bootstrap, metadata, collocation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
