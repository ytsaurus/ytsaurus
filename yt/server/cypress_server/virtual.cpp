#include "stdafx.h"
#include "virtual.h"

#include <ytlib/ytree/ypath_format.h>
#include <server/cypress_server/node_detail.h>
#include <server/cypress_server/node_proxy_detail.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

using namespace NRpc;
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
        ICypressNode* trunkNode,
        IYPathServicePtr service,
        bool requiredLeaderStatus)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
        , Service(service)
        , RequireLeaderStatus(requiredLeaderStatus)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        if (RequireLeaderStatus) {
            ValidateLeaderStatus();
        }

        TTokenizer tokenizer(path);
        tokenizer.ParseNext();
        if (tokenizer.GetCurrentType() == SuppressRedirectToken) {
            return TBase::Resolve(TYPath(tokenizer.GetCurrentSuffix()), context);
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
        ICypressNode* trunkNode,
        TTransaction* transaction) override
    {
        auto service = Producer.Run(trunkNode, transaction);
        return New<TVirtualNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode,
            service,
            RequireLeaderStatus);
    }

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
    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (ICypressNode* trunkNode, TTransaction* transaction) -> IYPathServicePtr {
            UNUSED(trunkNode);
            UNUSED(transaction);
            return service;
        }),
        requireLeaderStatus);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
