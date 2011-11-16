#include "stdafx.h"
#include "virtual.h"

#include "../cypress/node_detail.h"
#include "../cypress/node_proxy_detail.h"

#include "../ytree/yson_writer.h"
#include "../ytree/tree_visitor.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
    DECLARE_BYVAL_RO_PROPERTY(RuntimeType, ERuntimeNodeType);
    DECLARE_BYVAL_RO_PROPERTY(Manifest, TYson);

public:
    explicit TVirtualNode(
        const TBranchedNodeId& id,
        ERuntimeNodeType runtimeType = ERuntimeNodeType::Invalid,
        TYson manifest = "")
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

    bool IsOperationLogged(TYPath path, const Stroka& verb) const
    {
        // Don't log anything for virtual nodes expect when the path is
        // empty and thus refers to the node itself.
        if (IsEmptyYPath(path)) {
            return TBase::IsOperationLogged(path, verb);
        } else {
            return false;
        }
    }

private:
    typedef TCypressNodeProxyBase<IEntityNode, TVirtualNode> TBase;

    IYPathService::TPtr Service;

    virtual TResolveResult Resolve(TYPath path, const Stroka& verb)
    {
        if (~Service == NULL) {
            return TBase::Resolve(path, verb);
        } else {
            return Service->Resolve(path, verb);
        }
    }

    virtual void Invoke(IServiceContext* context)
    {
        if (~Service == NULL) {
            TBase::Invoke(context);
        } else {
            Service->Invoke(context);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TVirtualNode>
{
public:
    TVirtualNodeTypeHandler(
        TCypressManager* cypressManager,
        TYPathServiceProducer* producer,
        ERuntimeNodeType runtimeType,
        const Stroka& typeName)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(cypressManager)
        , Producer(producer)
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

        auto service = Producer->Do(context);

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
    TYPathServiceProducer::TPtr Producer;
    ERuntimeNodeType RuntimeType;
    Stroka TypeName;

};

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    ERuntimeNodeType runtypeType,
    const Stroka& typeName,
    TYPathServiceProducer* producer)
{
    return New<TVirtualNodeTypeHandler>(
        cypressManager,
        producer,
        runtypeType,
        typeName);
}

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    ERuntimeNodeType runtypeType,
    const Stroka& typeName,
    IYPathService* service)
{
    IYPathService::TPtr service_ = service;
    return New<TVirtualNodeTypeHandler>(
        cypressManager,
        ~FromFunctor([=] (const TVirtualYPathContext& context) -> IYPathService::TPtr
            {
                UNUSED(context);
                return service_;
            }),
        runtypeType,
        typeName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
