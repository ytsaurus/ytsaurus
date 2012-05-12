#include "stdafx.h"
#include "virtual.h"

#include <ytlib/cypress/node_detail.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
public:
    explicit TVirtualNode(const TVersionedNodeId& id)
        : TCypressNodeBase(id)
    { }

    TVirtualNode(const TVersionedNodeId& id, const TVirtualNode& other)
        : TCypressNodeBase(id, other)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TVirtualNode(Id, *this);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeProxy
    : public TCypressNodeProxyBase<IEntityNode, TVirtualNode>
{
public:
    typedef TCypressNodeProxyBase<IEntityNode, TVirtualNode> TBase;

    TVirtualNodeProxy(
        INodeTypeHandler* typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        const TNodeId& nodeId,
        IYPathServicePtr service)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
        , Service(service)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        TTokenizer tokenizer(path);
        tokenizer.ParseNext();
        if (tokenizer.GetCurrentType() == ETokenType::Ampersand) {
            return TBase::Resolve(TYPath(tokenizer.GetCurrentSuffix()), verb);
        }
        return TResolveResult::There(~Service, path);
    }

private:
    IYPathServicePtr Service;

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TVirtualNode>
{
public:
    typedef TVirtualNodeTypeHandler TThis;

    TVirtualNodeTypeHandler(
        TBootstrap* bootstrap,
        TYPathServiceProducer producer,
        EObjectType objectType)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(bootstrap)
        , Producer(producer)
        , ObjectType(objectType)
    { }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const TNodeId& id,
        TTransaction* transaction)
    {
        auto service = Producer.Run(id);
        return New<TVirtualNodeProxy>(
            this,
            Bootstrap,
            transaction,
            id,
            service);
    }

    virtual EObjectType GetObjectType()
    {
        return ObjectType;
    }

    virtual ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    virtual TAutoPtr<ICypressNode> Create(const TVersionedNodeId& id)
    {
        return new TVirtualNode(id);
    }

private:
    TYPathServiceProducer Producer;
    EObjectType ObjectType;

};

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer)
{
    return New<TVirtualNodeTypeHandler>(
        bootstrap,
        producer,
        objectType);
}

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    IYPathServicePtr service)
{
    IYPathServicePtr service_ = service;
    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (const TNodeId& id) -> IYPathServicePtr {
            UNUSED(id);
            return service_;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
