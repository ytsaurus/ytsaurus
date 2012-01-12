#include "stdafx.h"
#include "virtual.h"

#include <ytlib/cypress/node_detail.h>
#include <ytlib/cypress/node_proxy_detail.h>

#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/tree_visitor.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
    DEFINE_BYVAL_RO_PROPERTY(TYson, Manifest);

public:
    TVirtualNode(
        const TVersionedNodeId& id,
        EObjectType objectType,
        const TYson& manifest = "")
        : TCypressNodeBase(id, objectType)
        , Manifest_(manifest)
    { }

    TVirtualNode(
        const TVersionedNodeId& id,
        const TVirtualNode& other)
        : TCypressNodeBase(id, other)
        , Manifest_(other.Manifest_)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TVirtualNode(Id, *this);
    }

    virtual void Save(TOutputStream* output) const
    {
        TCypressNodeBase::Save(output);
        ::Save(output, Manifest_);
    }

    virtual void Load(TInputStream* input)
    {
        TCypressNodeBase::Load(input);
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

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        if (IsLocalYPath(path)) {
            return TBase::Resolve(path, verb);
        } else {
            return TResolveResult::There(~Service, path);
        }
    }

private:
    typedef TCypressNodeProxyBase<IEntityNode, TVirtualNode> TBase;

    IYPathService::TPtr Service;

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
        EObjectType objectType,
        const Stroka& typeName)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(cypressManager)
        , Producer(producer)
        , ObjectType(objectType)
        , TypeName(typeName)
    {
        RegisterGetter("manifest", FromMethod(&TThis::GetManifest));
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        auto typedNode = dynamic_cast<const TVirtualNode&>(node);

        TVirtualYPathContext context;
        context.NodeId = node.GetId().NodeId;
        context.TransactionId = transactionId;
        context.Manifest = typedNode.GetManifest();

        auto service = Producer->Do(context);

        return New<TVirtualNodeProxy>(
            this,
            ~CypressManager,
            transactionId,
            node.GetId().NodeId,
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

    virtual Stroka GetTypeName()
    {
        return TypeName;
    }
    
    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode* manifest)
    {
        UNUSED(transactionId);

        return new TVirtualNode(
            nodeId,
            ObjectType,
            SerializeToYson(manifest));
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TVersionedNodeId& id)
    {
        return new TVirtualNode(id, ObjectType);
    }

private:
    TYPathServiceProducer::TPtr Producer;
    EObjectType ObjectType;
    Stroka TypeName;

    static void GetManifest(const TGetAttributeParam& param)
    {
        TStringInput input(param.Node->GetManifest());
        TYsonReader reader(param.Consumer, &input);
        reader.Read();
    }
};

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    EObjectType runtypeType,
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
    EObjectType runtypeType,
    const Stroka& typeName,
    IYPathService* service)
{
    IYPathService::TPtr service_ = service;
    return CreateVirtualTypeHandler(
        cypressManager,
        runtypeType,
        typeName,
        ~FromFunctor([=] (const TVirtualYPathContext& context) -> IYPathService::TPtr
            {
                UNUSED(context);
                return service_;
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
