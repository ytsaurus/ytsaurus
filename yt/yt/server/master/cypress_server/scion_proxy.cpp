#include "scion_proxy.h"

#include "node_proxy_detail.h"
#include "scion_node.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TScionProxy
    : public TMapNodeProxy
{
public:
    using TMapNodeProxy::TMapNodeProxy;

private:
    using TBase = TMapNodeProxy;

    TScionNode* GetThisImpl()
    {
        return TNontemplateCypressNodeProxyBase::GetThisImpl<TScionNode>();
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::RootstockId);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        switch (key) {
            case EInternedAttributeKey::RootstockId:
                BuildYsonFluently(consumer)
                    .Value(node->GetRootstockId());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateScionProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TScionNode* trunkNode)
{
    return New<TScionProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
