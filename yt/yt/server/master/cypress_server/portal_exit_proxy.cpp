#include "portal_exit_proxy.h"

#include "node_proxy_detail.h"
#include "portal_exit_node.h"
#include "public.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TPortalExitProxy
    : public TMapNodeProxy
{
public:
    using TMapNodeProxy::TMapNodeProxy;

private:
    using TBase = TMapNodeProxy;

    TPortalExitNode* GetThisImpl()
    {
        return TNontemplateCypressNodeProxyBase::GetThisImpl<TPortalExitNode>();
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::RemovalStarted);
        descriptors->push_back(EInternedAttributeKey::EntranceCellTag);
        descriptors->push_back(EInternedAttributeKey::EntranceNodeId);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
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

            case EInternedAttributeKey::Acl:
                BuildYsonFluently(consumer)
                    .Value(node->DirectAcd().Acl());
                return true;

            case EInternedAttributeKey::AnnotationPath:
                BuildYsonFluently(consumer)
                    .Value(node->EffectiveAnnotationPath());
                return true;
            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        switch (key) {
            case EInternedAttributeKey::Acl:
            case EInternedAttributeKey::Annotation:
            case EInternedAttributeKey::InheritAcl:
            case EInternedAttributeKey::Owner:
                THROW_ERROR_EXCEPTION("Setting %Qv to portal exit is not allowed", key.Unintern());

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
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
