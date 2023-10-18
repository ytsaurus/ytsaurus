#include "secondary_index_proxy.h"
#include "secondary_index.h"
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

class TSecondaryIndexProxy
    : public TNonversionedObjectProxyBase<TSecondaryIndex>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TSecondaryIndex>;

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        attributes->push_back(EInternedAttributeKey::TableId);
        attributes->push_back(EInternedAttributeKey::TablePath);
        attributes->push_back(EInternedAttributeKey::IndexTableId);
        attributes->push_back(EInternedAttributeKey::IndexTablePath);
        attributes->push_back(EInternedAttributeKey::Kind);

        TBase::ListSystemAttributes(attributes);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* secondaryIndex = GetThisImpl();

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        switch (key) {
            case EInternedAttributeKey::TableId:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetTable()->GetId());
                return true;

            case EInternedAttributeKey::TablePath:
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(secondaryIndex->GetTable(), /*transaction*/ nullptr));
                return true;

            case EInternedAttributeKey::IndexTableId:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetIndexTable()->GetId());
                return true;

            case EInternedAttributeKey::IndexTablePath:
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(secondaryIndex->GetIndexTable(), /*transaction*/ nullptr));
                return true;

            case EInternedAttributeKey::Kind:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetKind());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateSecondaryIndexProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TSecondaryIndex* secondaryIndex)
{
    return New<TSecondaryIndexProxy>(bootstrap, metadata, secondaryIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
