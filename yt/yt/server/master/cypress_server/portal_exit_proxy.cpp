#include "portal_exit_proxy.h"
#include "portal_exit_node.h"
#include "node_proxy_detail.h"
#include "helpers.h"

#include <yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NYson;
using namespace NYTree;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TPortalExitProxy
    : public TMapNodeProxy
{
public:
    TPortalExitProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TPortalExitNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    using TBase = TMapNodeProxy;

    TPortalExitNode* GetThisImpl()
    {
        return TNontemplateCypressNodeProxyBase::GetThisImpl<TPortalExitNode>();
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::RemovalStarted);
        descriptors->push_back(EInternedAttributeKey::EntranceCellTag);
        descriptors->push_back(EInternedAttributeKey::EntranceNodeId);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        switch (key) {
            case EInternedAttributeKey::RemovalStarted:
                BuildYsonFluently(consumer)
                    .Value(node->GetRemovalStarted());
                return true;

            case EInternedAttributeKey::EntranceCellTag:
                BuildYsonFluently(consumer)
                    .Value(node->GetEntranceCellTag());
                return true;

            case EInternedAttributeKey::EntranceNodeId:
                BuildYsonFluently(consumer)
                    .Value(MakePortalEntranceNodeId(node->GetId(), node->GetEntranceCellTag()));
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreatePortalExitProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TPortalExitNode* trunkNode)
{
    return New<TPortalExitProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
