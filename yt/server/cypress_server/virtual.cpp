#include "stdafx.h"
#include "virtual.h"

#include <core/misc/singleton.h>

#include <core/ypath/tokenizer.h>

#include <server/hydra/hydra_manager.h>

#include <server/cypress_server/node_detail.h>
#include <server/cypress_server/node_proxy_detail.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

namespace NYT {
namespace NCypressServer {

using namespace NRpc;
using namespace NYTree;
using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
public:
    explicit TVirtualNode(const TVersionedNodeId& id)
        : TCypressNodeBase(id)
    { }

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TVirtualNode>
{
public:
    TVirtualNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TVirtualNode* trunkNode,
        IYPathServicePtr service,
        EVirtualNodeOptions options)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
        , Service(service)
        , Options(options)
    { }

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TVirtualNode> TBase;

    const IYPathServicePtr Service;
    const EVirtualNodeOptions Options;


    virtual TResolveResult ResolveSelf(const TYPath& path, IServiceContextPtr context) override
    {
        const auto& method = context->GetMethod();
        if ((Options & EVirtualNodeOptions::RedirectSelf) != EVirtualNodeOptions::None &&
            method != "Remove")
        {
            return TResolveResult::There(Service, path);
        } else {
            return TBase::ResolveSelf(path, context);
        }
    }

    virtual TResolveResult ResolveRecursive(const TYPath& path, IServiceContextPtr context) override
    {
        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
            case NYPath::ETokenType::Slash:
                return TResolveResult::There(Service, path);
            default:
                return TResolveResult::There(Service, "/" + path);
        }
    }


    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider) {
            provider->ListSystemAttributes(attributes);
        }

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider && provider->GetBuiltinAttribute(key, consumer)) {
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider && provider->SetBuiltinAttribute(key, value)) {
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual bool IsLeaderReadRequired() const override
    {
        return Any(Options & EVirtualNodeOptions::RequireLeader);
    }

    ISystemAttributeProvider* GetTargetBuiltinAttributeProvider()
    {
        return dynamic_cast<ISystemAttributeProvider*>(Service.Get());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TVirtualNode>
{
public:
    TVirtualNodeTypeHandler(
        TBootstrap* bootstrap,
        TYPathServiceProducer producer,
        EObjectType objectType,
        EVirtualNodeOptions options)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(bootstrap)
        , Producer(producer)
        , ObjectType(objectType)
        , Options(options)
    { }

    virtual EObjectType GetObjectType() override
    {
        return ObjectType;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

private:
    TYPathServiceProducer Producer;
    EObjectType ObjectType;
    EVirtualNodeOptions Options;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TVirtualNode* trunkNode,
        TTransaction* transaction) override
    {
        auto service = Producer.Run(trunkNode, transaction);
        return New<TVirtualNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode,
            service,
            Options);
    }

};

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer,
    EVirtualNodeOptions options)
{
    return New<TVirtualNodeTypeHandler>(
        bootstrap,
        producer,
        objectType,
        options);
}

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    IYPathServicePtr service,
    EVirtualNodeOptions options)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (TCypressNodeBase* trunkNode, TTransaction* transaction) -> IYPathServicePtr {
            UNUSED(trunkNode);
            UNUSED(transaction);
            return service;
        }),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
