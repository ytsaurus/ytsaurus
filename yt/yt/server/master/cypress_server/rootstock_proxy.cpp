#include "rootstock_proxy.h"

#include "node_proxy_detail.h"
#include "rootstock_node.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TRootstockProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TRootstockNode>
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TRootstockNode>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::ScionId);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        switch (key) {
            case EInternedAttributeKey::ScionId:
                BuildYsonFluently(consumer)
                    .Value(node->GetScionId());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        switch (key) {
            case EInternedAttributeKey::Opaque: {
                auto opaque = ConvertTo<bool>(value);
                if (!opaque) {
                    THROW_ERROR_EXCEPTION("Rootstocks cannot be made non-opaque");
                }
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateRootstockProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TRootstockNode* trunkNode)
{
    return New<TRootstockProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
