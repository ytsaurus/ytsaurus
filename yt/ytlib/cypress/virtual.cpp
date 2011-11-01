#include "stdafx.h"
#include "virtual.h"

#include "../cypress/node_detail.h"
#include "../cypress/node_proxy_detail.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
public:
    explicit TVirtualNode(
        const TBranchedNodeId& id,
        ERuntimeNodeType runtimeType = ERuntimeNodeType::Invalid)
        : TCypressNodeBase(id)
        , RuntimeType(runtimeType)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TVirtualNode(Id, RuntimeType);
    }

    virtual ERuntimeNodeType GetRuntimeType() const
    {
        return RuntimeType;
    }

    virtual void Save(TOutputStream* output) const
    {
        TCypressNodeBase::Save(output);
        // TODO: enum serialization
        ::Save(output, static_cast<i32>(RuntimeType));
    }

    virtual void Load(TInputStream* input)
    {
        TCypressNodeBase::Load(input);
        // TODO: enum serialization
        i32 type;
        ::Load(input, type);
        RuntimeType = ERuntimeNodeType(type);
    }

private:
    ERuntimeNodeType RuntimeType;

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeProxy
    : public TCypressNodeProxyBase<IEntityNode, TVirtualNode>
{
public:
    TVirtualNodeProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        IYPathService* service)
        : TCypressNodeProxyBase<IEntityNode, TVirtualNode>(
            typeHandler,
            cypressManager,
            transactionId,
            nodeId)
        , Service(service)
    { }

private:
    IYPathService::TPtr Service;

    virtual TNavigateResult Navigate(TYPath path)
    {
        return Service->Navigate(path);
    }

    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer)
    {
        return Service->Get(path, consumer);
    }

    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer)
    {
        return Service->Set(path, producer);
    }

    virtual TRemoveResult Remove(TYPath path)
    {
        return Service->Remove(path);
    }

    virtual TLockResult Lock(TYPath path)
    {
        return Service->Lock(path);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public INodeTypeHandler
{
public:
    TVirtualNodeTypeHandler(
        TCypressManager* cypressManager,
        TYPathServiceBuilder* serviceBuilder,
        ERuntimeNodeType runtimeType,
        const Stroka& typeName)
        : CypressManager(cypressManager)
        , ServiceBuilder(serviceBuilder)
        , RuntimeType(runtimeType)
        , TypeName(typeName)
    { }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        TCreateServiceParam param;
        param.Node = &node;
        param.TransactionId = transactionId;
        auto service = ServiceBuilder->Do(param);

        return New<TVirtualNodeProxy>(
            this,
            ~CypressManager,
            transactionId,
            node.GetId().NodeId,
            ~service);
    }

    virtual ERuntimeNodeType GetRuntimeType()
    {
        return RuntimeType;
    }

    virtual ENodeType GetNodeType()
    {
        // TODO: is this always right?
        return ENodeType::Entity;
    }

    virtual Stroka GetTypeName()
    {
        return TypeName;
    }
    
    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode::TPtr manifest)
    {
        // TODO: only system transaction may do this
        UNUSED(transactionId);
        UNUSED(manifest);
        return new TVirtualNode(
            TBranchedNodeId(nodeId, NullTransactionId),
            RuntimeType);
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TBranchedNodeId& id)
    {
        return new TVirtualNode(id);
    }

    virtual void Destroy(ICypressNode& node)
    {
        UNUSED(node);
    }

    virtual TAutoPtr<ICypressNode> Branch(
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        UNUSED(node);
        UNUSED(transactionId);
        YUNREACHABLE();
    }

    virtual void Merge(
        ICypressNode& committedNode,
        ICypressNode& branchedNode)
    {
        UNUSED(committedNode);
        UNUSED(branchedNode);
        YUNREACHABLE();
    }

    virtual void GetAttributeNames(
        const ICypressNode& node,
        yvector<Stroka>* names)
    {
        UNUSED(node);
        UNUSED(names);
        YUNREACHABLE();
    }

    virtual bool GetAttribute(
        const ICypressNode& node,
        const Stroka& name,
        NYTree::IYsonConsumer* consumer)
    {
        UNUSED(node);
        UNUSED(name);
        UNUSED(consumer);
        YUNREACHABLE();
    }

private:
    TCypressManager::TPtr CypressManager;
    TYPathServiceBuilder::TPtr ServiceBuilder;
    ERuntimeNodeType RuntimeType;
    Stroka TypeName;

};

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    ERuntimeNodeType runtypeType,
    const Stroka& typeName,
    TYPathServiceBuilder* serviceBuilder)
{
    return New<TVirtualNodeTypeHandler>(
        cypressManager,
        serviceBuilder,
        runtypeType,
        typeName);
}

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    ERuntimeNodeType runtypeType,
    const Stroka& typeName,
    IYPathService* service)
{
    IYPathService::TPtr servicePtr = service;
    return New<TVirtualNodeTypeHandler>(
        cypressManager,
        ~FromFunctor([=] (const TCreateServiceParam& param) -> IYPathService::TPtr
            {
                UNUSED(param);
                return servicePtr;
            }),
        runtypeType,
        typeName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
