#include "stdafx.h"
#include "virtual.h"

#include <ytlib/ytree/ypath_format.h>
#include <ytlib/cypress_server/node_detail.h>
#include <ytlib/cypress_server/node_proxy_detail.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
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
    : public TCypressNodeProxyBase<IEntityNode, TVirtualNode>
{
public:
    typedef TCypressNodeProxyBase<IEntityNode, TVirtualNode> TBase;

    TVirtualNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        const TNodeId& nodeId,
        IYPathServicePtr service,
        bool requiredLeaderStatus)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
        , Service(service)
        , RequireLeaderStatus(requiredLeaderStatus)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        if (RequireLeaderStatus) {
            ValidateLeaderStatus();
        }

        TTokenizer tokenizer(path);
        tokenizer.ParseNext();
        if (tokenizer.GetCurrentType() == SuppressRedirectToken) {
            return TBase::Resolve(TYPath(tokenizer.GetCurrentSuffix()), verb);
        }

        return TResolveResult::There(Service, path);
    }

private:
    IYPathServicePtr Service;
    bool RequireLeaderStatus;

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
        EObjectType objectType,
        bool requireLeaderStatus)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(bootstrap)
        , Producer(producer)
        , ObjectType(objectType)
        , RequireLeaderStatus(requireLeaderStatus)
    { }

    virtual ICypressNodeProxyPtr GetProxy(
        const TNodeId& id,
        TTransaction* transaction)
    {
        auto service = Producer.Run(id);
        return New<TVirtualNodeProxy>(
            this,
            Bootstrap,
            transaction,
            id,
            service,
            RequireLeaderStatus);
    }

    virtual EObjectType GetObjectType()
    {
        return ObjectType;
    }

    virtual ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

private:
    TYPathServiceProducer Producer;
    EObjectType ObjectType;
    bool RequireLeaderStatus;

};

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer,
    bool requireLeaderStatus)
{
    return New<TVirtualNodeTypeHandler>(
        bootstrap,
        producer,
        objectType,
        requireLeaderStatus);
}

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    IYPathServicePtr service,
    bool requireLeaderStatus)
{
    IYPathServicePtr service_ = service;
    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (const TNodeId& id) -> IYPathServicePtr {
            UNUSED(id);
            return service_;
        }),
        requireLeaderStatus);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
