#include "stdafx.h"
#include "virtual.h"

#include "../cypress/node_detail.h"
#include "../cypress/node_proxy_detail.h"

#include "../ytree/yson_writer.h"
#include "../ytree/tree_visitor.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
    DECLARE_BYVAL_RO_PROPERTY(RuntimeType, ERuntimeNodeType);
    DECLARE_BYVAL_RO_PROPERTY(Manifest, Stroka);

public:
    explicit TVirtualNode(
        const TBranchedNodeId& id,
        ERuntimeNodeType runtimeType = ERuntimeNodeType::Invalid,
        Stroka manifest = "")
        : TCypressNodeBase(id)
        , RuntimeType_(runtimeType)
        , Manifest_(manifest)
    { }

    explicit TVirtualNode(
        const TBranchedNodeId& id,
        const TVirtualNode& other)
        : TCypressNodeBase(id)
        , RuntimeType_(other.RuntimeType_)
        , Manifest_(other.Manifest_)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TVirtualNode(Id, RuntimeType_);
    }

    virtual void Save(TOutputStream* output) const
    {
        TCypressNodeBase::Save(output);
        ::Save(output, RuntimeType_);
        ::Save(output, Manifest_);
    }

    virtual void Load(TInputStream* input)
    {
        TCypressNodeBase::Load(input);
        ::Load(input, RuntimeType_);
        ::Load(input, Manifest_);
    }

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
    typedef TCypressNodeProxyBase<IEntityNode, TVirtualNode> TBase;

    IYPathService::TPtr Service;

    //virtual TNavigateResult Navigate(TYPath path)
    //{
    //    if (~Service == NULL) {
    //        return TBase::Navigate(path);
    //    } else {
    //        return Service->Navigate(path);
    //    }
    //}

    //virtual TGetResult Get(TYPath path, IYsonConsumer* consumer)
    //{
    //    if (~Service == NULL) {
    //        return TBase::Get(path, consumer);
    //    } else {
    //        return Service->Get(path, consumer);
    //    }
    //}

    //virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer)
    //{
    //    if (~Service == NULL) {
    //        return TBase::Set(path, producer);
    //    } else {
    //        return Service->Set(path, producer);
    //    }
    //}

    //virtual TRemoveResult Remove(TYPath path)
    //{
    //    if (~Service == NULL) {
    //        return TBase::Remove(path);
    //    } else {
    //        return Service->Remove(path);
    //    }
    //}

    //virtual TLockResult Lock(TYPath path)
    //{
    //    if (~Service == NULL) {
    //        return TBase::Lock(path);
    //    } else {
    //        return Service->Lock(path);
    //    }
    //}
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TVirtualNode>
{
public:
    TVirtualNodeTypeHandler(
        TCypressManager* cypressManager,
        TYPathServiceBuilder* serviceBuilder,
        ERuntimeNodeType runtimeType,
        const Stroka& typeName)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(cypressManager)
        , ServiceBuilder(serviceBuilder)
        , RuntimeType(runtimeType)
        , TypeName(typeName)
    { }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        auto typedNode = dynamic_cast<const TVirtualNode&>(node);

        TVirtualYPathContext context;
        context.NodeId = node.GetId().NodeId;
        context.TransactionId = transactionId;
        context.Manifest = typedNode.GetManifest();
        context.Fallback = New<TVirtualNodeProxy>(
            this,
            ~CypressManager,
            transactionId,
            node.GetId().NodeId,
            static_cast<IYPathService*>(NULL));

        auto service = ServiceBuilder->Do(context);

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
        return ENodeType::Entity;
    }

    virtual Stroka GetTypeName()
    {
        return TypeName;
    }
    
    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode::TPtr manifest)
    {
        UNUSED(transactionId);

        TStringStream manifestStream;
        TYsonWriter writer(&manifestStream, TYsonWriter::EFormat::Binary);
        TTreeVisitor visitor(&writer);
        visitor.Visit(~manifest);

        return new TVirtualNode(
            TBranchedNodeId(nodeId, NullTransactionId),
            RuntimeType,
            manifestStream.Str());
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TBranchedNodeId& id)
    {
        return new TVirtualNode(id);
    }

private:
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
        ~FromFunctor([=] (const TVirtualYPathContext& context) -> IYPathService::TPtr
            {
                UNUSED(context);
                return servicePtr;
            }),
        runtypeType,
        typeName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
