#include "stdafx.h"
#include "virtual.h"

#include <ytlib/cypress/node_detail.h>
#include <ytlib/cypress/node_proxy_detail.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;

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
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        IYPathService* service)
        : TBase(
            typeHandler,
            cypressManager,
            transactionId,
            nodeId)
        , Service(service)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        if (IsLocalYPath(path)) {
            return TBase::Resolve(path, verb);
        }
        auto redirectedPath = ChopYPathRedirectMarker(path);
        return TResolveResult::There(~Service, redirectedPath);
    }

private:
    TYPathServicePtr Service;

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TVirtualNode>
{
public:
    typedef TVirtualNodeTypeHandler TThis;

    TVirtualNodeTypeHandler(
        TCypressManager* cypressManager,
        TYPathServiceProducer* producer,
        EObjectType objectType)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(cypressManager)
        , Producer(producer)
        , ObjectType(objectType)
    { }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        auto service = Producer->Do(id);
        return New<TVirtualNodeProxy>(
            this,
            ~CypressManager,
            id.TransactionId,
            id.ObjectId,
            ~service);
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
    TYPathServiceProducer::TPtr Producer;
    EObjectType ObjectType;

};

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    EObjectType objectType,
    TYPathServiceProducer* producer)
{
    return New<TVirtualNodeTypeHandler>(
        cypressManager,
        producer,
        objectType);
}

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    EObjectType objectType,
    IYPathService* service)
{
    TYPathServicePtr service_ = service;
    return CreateVirtualTypeHandler(
        cypressManager,
        objectType,
        ~FromFunctor([=] (const TVersionedNodeId& id) -> TYPathServicePtr
            {
                UNUSED(id);
                return service_;
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
