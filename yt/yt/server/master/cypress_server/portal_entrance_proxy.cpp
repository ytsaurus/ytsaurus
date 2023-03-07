#include "portal_entrance_proxy.h"
#include "portal_entrance_node.h"
#include "node_proxy_detail.h"

#include <yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TPortalEntranceProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TPortalEntranceNode>
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    TPortalEntranceProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TPortalEntranceNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TPortalEntranceNode>;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::RemovalStarted);
        descriptors->push_back(EInternedAttributeKey::ExitCellTag);
        descriptors->push_back(EInternedAttributeKey::ExitNodeId);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        switch (key) {
            case EInternedAttributeKey::RemovalStarted:
                BuildYsonFluently(consumer)
                    .Value(node->GetRemovalStarted());
                return true;

            case EInternedAttributeKey::ExitCellTag:
                BuildYsonFluently(consumer)
                    .Value(node->GetExitCellTag());
                return true;

            case EInternedAttributeKey::ExitNodeId:
                BuildYsonFluently(consumer)
                    .Value(MakePortalExitNodeId(node->GetId(), node->GetExitCellTag()));
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreatePortalEntranceProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TPortalEntranceNode* trunkNode)
{
    return New<TPortalEntranceProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
