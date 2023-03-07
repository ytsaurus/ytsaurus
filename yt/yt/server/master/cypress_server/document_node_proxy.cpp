#include "document_node_proxy.h"
#include "document_node.h"
#include "node_proxy_detail.h"

#include <yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TServerRequest, class TServerResponse, class TContext>
bool DelegateInvocation(
    IYPathServicePtr service,
    TServerRequest* serverRequest,
    TServerResponse* serverResponse,
    TIntrusivePtr<TContext> context)
{
    typedef typename TServerRequest::TMessage  TRequestMessage;
    typedef typename TServerResponse::TMessage TResponseMessage;

    typedef TTypedYPathRequest<TRequestMessage, TResponseMessage>  TClientRequest;

    auto clientRequest = New<TClientRequest>(context->RequestHeader());
    clientRequest->MergeFrom(*serverRequest);

    auto clientResponseOrError = ExecuteVerb(service, clientRequest).Get();

    if (clientResponseOrError.IsOK()) {
        const auto& clientResponse = clientResponseOrError.Value();
        serverResponse->MergeFrom(*clientResponse);
        context->Reply();
        return true;
    } else {
        context->Reply(clientResponseOrError);
        return false;
    }
}

} // namespace

class TDocumentNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TDocumentNode>
{
public:
    TDocumentNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TDocumentNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

    virtual ENodeType GetType() const override
    {
        return ENodeType::Entity;
    }

    virtual TIntrusivePtr<const IEntityNode> AsEntity() const override
    {
        return this;
    }

    virtual TIntrusivePtr<IEntityNode> AsEntity() override
    {
        return this;
    }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TDocumentNode>;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const NRpc::IServiceContextPtr& context) override
    {
        return TResolveResultHere{"/" + path};
    }

    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
        const auto* impl = GetThisImpl();
        DelegateInvocation(impl->GetValue(), request, response, context);
    }

    virtual void GetRecursive(const TYPath& path, TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
        const auto* impl = GetThisImpl();
        DelegateInvocation(impl->GetValue(), request, response, context);
    }


    virtual void SetSelf(TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        SetImplValue(TYsonString(request->value()));
        context->Reply();
    }

    virtual void SetRecursive(const TYPath& path, TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override
    {
        context->SetRequestInfo();
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        auto* impl = LockThisImpl();
        if (DelegateInvocation(impl->GetValue(), request, response, context)) {
            SetModified();
        }
    }


    virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
        const auto* impl = GetThisImpl();
        DelegateInvocation(impl->GetValue(), request, response, context);
    }

    virtual void ListRecursive(const TYPath& path, TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
        const auto* impl = GetThisImpl();
        DelegateInvocation(impl->GetValue(), request, response, context);
    }


    virtual void RemoveRecursive(const TYPath& path, TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        auto* impl = LockThisImpl();
        if (DelegateInvocation(impl->GetValue(), request, response, context)) {
            SetModified();
        }
    }


    virtual void ExistsRecursive(const TYPath& path, TReqExists* request, TRspExists* response, const TCtxExistsPtr& context) override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
        const auto* impl = GetThisImpl();
        DelegateInvocation(impl->GetValue(), request, response, context);
    }


    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Value)
            .SetWritable(true)
            .SetOpaque(true)
            .SetReplicated(true));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* impl = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Value:
                BuildYsonFluently(consumer)
                    .Value(impl->GetValue());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        switch (key) {
            case EInternedAttributeKey::Value:
                SetImplValue(value);
                return true;

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }


    void SetImplValue(const TYsonString& value)
    {
        auto* impl = LockThisImpl();
        impl->SetValue(ConvertToNode(value));
        SetModified();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateDocumentNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TDocumentNode* trunkNode)
{
    return New<TDocumentNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
