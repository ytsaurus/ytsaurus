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

class TLeaderValidatorWrapper
    : public IYPathService
{
public:
    TLeaderValidatorWrapper(
        TBootstrap* bootstrap,
        IYPathServicePtr underlyingService)
        : Bootstrap(bootstrap)
        , UnderlyingService(underlyingService)
    { }

    virtual TResolveResult Resolve(const TYPath& path, IServiceContextPtr context) override
    {
        auto result = UnderlyingService->Resolve(path, context);
        return result.IsHere()
            ? result
            : TResolveResult::There(
                CreateLeaderValidatorWrapper(Bootstrap, result.GetService()),
                result.GetPath());
    }

    virtual void Invoke(IServiceContextPtr context) override
    {
        Bootstrap->GetMetaStateFacade()->ValidateLeaderStatus();
        UnderlyingService->Invoke(context);
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const override
    {
        return UnderlyingService->IsWriteRequest(context);
    }

private:
    TBootstrap* Bootstrap;
    IYPathServicePtr UnderlyingService;

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
        IYPathServicePtr service)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
        , Service(service)
    { }

    virtual TResolveResult Resolve(const TYPath& path, IServiceContextPtr context) override
    {
        TTokenizer tokenizer(path);
        tokenizer.ParseNext();
        if (tokenizer.GetCurrentType() == SuppressRedirectToken) {
            return TBase::Resolve(TYPath(tokenizer.GetCurrentSuffix()), context);
        }

        return TResolveResult::There(Service, path);
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
            service);
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

};

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer)
{
    return New<TVirtualNodeTypeHandler>(
        bootstrap,
        producer,
        objectType);
}

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    IYPathServicePtr service)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (ICypressNode* trunkNode, TTransaction* transaction) -> IYPathServicePtr {
            UNUSED(trunkNode);
            UNUSED(transaction);
            return service;
        }));
}

IYPathServicePtr CreateLeaderValidatorWrapper(
    TBootstrap* bootstrap,
    IYPathServicePtr service)
{
    return New<TLeaderValidatorWrapper>(bootstrap, service);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
