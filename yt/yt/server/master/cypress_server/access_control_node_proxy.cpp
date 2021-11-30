#include "access_control_node_proxy.h"

#include "access_control_node.h"
#include "node_proxy_detail.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>
#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TAccessControlNodeProxy
    : public TCypressNodeProxyBase<
        TNontemplateCypressNodeProxyBase, 
        IEntityNode, 
        TAccessControlNode>
{
    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(Entity);

public:
    TAccessControlNodeProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TAccessControlNode* trunkNode)
        : TBase(bootstrap, metadata, transaction, trunkNode)
    { }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TAccessControlNode>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Namespace)
            .SetMandatory(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        switch (key) {
            case EInternedAttributeKey::Namespace: {
                const auto* impl = GetThisImpl();
                BuildYsonFluently(consumer)
                    .Value(impl->GetNamespace());
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateAccessControlNodeProxy(
    TBootstrap *bootstrap, 
    TObjectTypeMetadata *metadata, 
    TTransaction *transaction, 
    TAccessControlNode *trunkNode)
{
    return New<TAccessControlNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
