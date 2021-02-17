#include "link_node_proxy.h"
#include "link_node.h"
#include "node_proxy_detail.h"

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/core/ypath/tokenizer.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

using NYPath::TTokenizer;
using NYPath::ETokenType;

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TLinkNode>
{
    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(Entity)

public:
    TLinkNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TLinkNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        auto propagate = [&] () {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto* linkNode = GetThisImpl();
            auto combinedPath = linkNode->ComputeEffectiveTargetPath() + path;
            return TResolveResultThere{objectManager->GetRootService(), std::move(combinedPath)};
        };

        const auto& method = context->GetMethod();
        TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case ETokenType::Ampersand:
                return TBase::Resolve(TYPath(tokenizer.GetSuffix()), context);

            case ETokenType::EndOfStream: {
                // NB: Always handle mutating Cypress verbs locally.
                if (method == "Remove" ||
                    method == "Set" ||
                    method == "Create" ||
                    method == "Copy" ||
                    method == "EndCopy")
                {
                    return TResolveResultHere{path};
                } else if (method == "BeginCopy") {
                    THROW_ERROR_EXCEPTION("A link node cannot be externalized; consider externalizing its target instead");
                } else {
                    return propagate();
                }
            }

            default:
                return propagate();
        }
    }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TLinkNode>;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::TargetPath);
        descriptors->push_back(EInternedAttributeKey::Broken);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        switch (key) {
            case EInternedAttributeKey::TargetPath: {
                const auto* impl = GetThisImpl();
                BuildYsonFluently(consumer)
                    .Value(impl->GetTargetPath());
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        switch (key) {
            case EInternedAttributeKey::Broken:
                return IsBroken().Apply(BIND([] (bool result) {
                    return ConvertToYsonString(result);
                }));

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    TFuture<bool> IsBroken() const
    {
        try {
            const auto* linkNode = GetThisImpl();
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto req = TYPathProxy::Exists(linkNode->ComputeEffectiveTargetPath());
            auto rsp = ExecuteVerb(objectManager->GetRootService(), req);
            return rsp.Apply(BIND([] (const TYPathProxy::TErrorOrRspExistsPtr& rspOrError) {
                if (!rspOrError.IsOK()) {
                    return TrueFuture;
                }
                return MakeFuture(!rspOrError.Value()->value());
            }));
        } catch (const std::exception& ex) {
            return MakeFuture<bool>(ex);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateLinkNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TLinkNode* trunkNode)
{
    return New<TLinkNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
