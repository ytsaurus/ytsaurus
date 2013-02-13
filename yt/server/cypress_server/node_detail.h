#pragma once

#include "node.h"
#include "cypress_manager.h"
#include "helpers.h"

#include <ytlib/misc/serialize.h>

#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ypath.pb.h>

#include <ytlib/meta_state/meta_state_manager.h>

#include <server/object_server/object_detail.h>
#include <server/object_server/attribute_set.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialization_context.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl, class TProxy>
class TNodeBehaviorBase
    : public INodeBehavior
{
public:
    TNodeBehaviorBase(
        NCellMaster::TBootstrap* bootstrap,
        TImpl* trunkNode)
        : Bootstrap(bootstrap)
        , TrunkNode(trunkNode)
    {
        YASSERT(trunkNode->IsTrunk());
    }

    virtual void Destroy() override
    { }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    TImpl* TrunkNode;

    TImpl* GetImpl() const
    {
        return TrunkNode;
    }

    TIntrusivePtr<TProxy> GetProxy() const
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto proxy = cypressManager->GetVersionedNodeProxy(TrunkNode);
        auto* typedProxy = dynamic_cast<TProxy*>(~proxy);
        YCHECK(typedProxy);
        return typedProxy;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    explicit TNontemplateCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

protected:
    NCellMaster::TBootstrap* Bootstrap;

    bool IsRecovery() const;

    void DestroyCore(TCypressNodeBase* node);

    void BranchCore(
        const TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);

    void MergeCore(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode);

    TAutoPtr<TCypressNodeBase> CloneCore(
        TCypressNodeBase* sourceNode,
        NTransactionServer::TTransaction* transaction);

};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public TNontemplateCypressNodeTypeHandlerBase
{
public:
    explicit TCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : TNontemplateCypressNodeTypeHandlerBase(bootstrap)
    { }

    virtual ICypressNodeProxyPtr GetProxy(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(dynamic_cast<TImpl*>(trunkNode), transaction);
    }

    virtual TAutoPtr<TCypressNodeBase> Instantiate(const TVersionedNodeId& id) override
    {
        return new TImpl(id);
    }

    virtual TAutoPtr<TCypressNodeBase> Create(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        // TODO(babenko): Release is needed due to cast to ICypressNode.
        return DoCreate(
            transaction,
            request,
            response).Release();
    }

    virtual void SetDefaultAttributes(NYTree::IAttributeDictionary* attributes) override
    {
        UNUSED(attributes);
    }

    virtual void Destroy(TCypressNodeBase* node) override
    {
        // Run core stuff.
        DestroyCore(node);

        // Run custom stuff.
        DoDestroy(dynamic_cast<TImpl*>(node));
    }

    virtual TAutoPtr<TCypressNodeBase> Branch(
        const TCypressNodeBase* originatingNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode) override
    {
        // Instantiate a branched copy.
        auto originatingId = originatingNode->GetVersionedId();
        auto branchedId = TVersionedNodeId(originatingId.ObjectId, GetObjectId(transaction));
        TAutoPtr<TImpl> branchedNode(new TImpl(branchedId));

        // Run core stuff.
        BranchCore(
            originatingNode,
            ~branchedNode,
            transaction,
            mode);

        // Run custom stuff.
        DoBranch(
            dynamic_cast<const TImpl*>(originatingNode),
            ~branchedNode);

        return branchedNode.Release();
    }

    virtual void Merge(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) override
    {
        // Run core stuff.
        MergeCore(
            originatingNode,
            branchedNode);

        // Run custom stuff.
        DoMerge(
            dynamic_cast<TImpl*>(originatingNode),
            dynamic_cast<TImpl*>(branchedNode));
    }

    virtual INodeBehaviorPtr CreateBehavior(TCypressNodeBase* trunkNode) override
    {
        return DoCreateBehavior(dynamic_cast<TImpl*>(trunkNode));
    }

    virtual TAutoPtr<TCypressNodeBase> Clone(
        TCypressNodeBase* sourceNode,
        NTransactionServer::TTransaction* transaction) override
    {
        // Run core stuff.
        auto clonedNode = CloneCore(
            sourceNode,
            transaction);

        // Run custom stuff.
        DoClone(
            dynamic_cast<TImpl*>(sourceNode),
            dynamic_cast<TImpl*>(~clonedNode),
            transaction);

        return clonedNode;
    }


protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual TAutoPtr<TImpl> DoCreate(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response)
    {
        UNUSED(transaction);
        UNUSED(request);
        UNUSED(response);

        auto objectManager = Bootstrap->GetObjectManager();

        auto nodeId = TVersionedNodeId(objectManager->GenerateId(GetObjectType()));

        TAutoPtr<TImpl> node(new TImpl(nodeId));
        node->SetTrunkNode(~node);

        return node;
    }

    virtual void DoDestroy(TImpl* node)
    {
        UNUSED(node);
    }

    virtual void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode)
    {
        UNUSED(originatingNode);
        UNUSED(branchedNode);
    }

    virtual void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode)
    {
        UNUSED(originatingNode);
        UNUSED(branchedNode);
    }

    virtual INodeBehaviorPtr DoCreateBehavior(TImpl* trunkNode)
    {
        UNUSED(trunkNode);
        return nullptr;
    }

    virtual void DoClone(
        TImpl* sourceNode,
        TImpl* clonedNode,
        NTransactionServer::TTransaction* transaction)
    {
        UNUSED(sourceNode);
        UNUSED(clonedNode);
        UNUSED(transaction);
    }

private:
    typedef TCypressNodeTypeHandlerBase<TImpl> TThis;

};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TValue>
struct TCypressScalarTypeTraits
{ };

template <>
struct TCypressScalarTypeTraits<Stroka>
    : NYTree::NDetail::TScalarTypeTraits<Stroka>
{
    static const NObjectClient::EObjectType::EDomain ObjectType;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : NYTree::NDetail::TScalarTypeTraits<i64>
{
    static const NObjectClient::EObjectType::EDomain ObjectType;
};

template <>
struct TCypressScalarTypeTraits<double>
    : NYTree::NDetail::TScalarTypeTraits<double>
{
    static const NObjectClient::EObjectType::EDomain ObjectType;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNode
    : public TCypressNodeBase
{
    typedef TScalarNode<TValue> TThis;

    DEFINE_BYREF_RW_PROPERTY(TValue, Value)

public:
    explicit TScalarNode(const TVersionedNodeId& id)
        : TCypressNodeBase(id)
        , Value_()
    { }

    virtual void Save(const NCellMaster::TSaveContext& context) const override
    {
        TCypressNodeBase::Save(context);
        auto* output = context.GetOutput();
        ::Save(output, Value_);
    }

    virtual void Load(const NCellMaster::TLoadContext& context) override
    {
        TCypressNodeBase::Load(context);
        auto* input = context.GetInput();
        ::Load(input, Value_);
    }
};

typedef TScalarNode<Stroka> TStringNode;
typedef TScalarNode<i64>    TIntegerNode;
typedef TScalarNode<double> TDoubleNode;

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNodeTypeHandler
    : public TCypressNodeTypeHandlerBase< TScalarNode<TValue> >
{
public:
    explicit TScalarNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual NObjectClient::EObjectType GetObjectType() override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    virtual NYTree::ENodeType GetNodeType() override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

protected:
    typedef TCypressNodeTypeHandlerBase< TScalarNode<TValue> > TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TScalarNode<TValue>* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBranch(
        const TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);

        branchedNode->Value() = originatingNode->Value();
    }

    virtual void DoMerge(
        TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->Value() = branchedNode->Value();
    }

    virtual void DoClone(
        TScalarNode<TValue>* sourceNode,
        TScalarNode<TValue>* clonedNode,
        NTransactionServer::TTransaction* transaction) override
    {
        TBase::DoClone(sourceNode, clonedNode, transaction);

        clonedNode->Value() = sourceNode->Value();
    }

};

typedef TScalarNodeTypeHandler<Stroka> TStringNodeTypeHandler;
typedef TScalarNodeTypeHandler<i64>    TIntegerNodeTypeHandler;
typedef TScalarNodeTypeHandler<double> TDoubleNodeTypeHandler;

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TCypressNodeBase*> TKeyToChild;
    typedef yhash_map<TCypressNodeBase*, Stroka> TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);
    DEFINE_BYREF_RW_PROPERTY(int, ChildCountDelta);

public:
    explicit TMapNode(const TVersionedNodeId& id);

    virtual void Save(const NCellMaster::TSaveContext& context) const override;
    virtual void Load(const NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TMapNode>
{
public:
    explicit TMapNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

private:
    typedef TMapNodeTypeHandler TThis;
    typedef TCypressNodeTypeHandlerBase<TMapNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TMapNode* node) override;

    virtual void DoBranch(
        const TMapNode* originatingNode,
        TMapNode* branchedNode);

    virtual void DoMerge(
        TMapNode* originatingNode,
        TMapNode* branchedNode) override;

    virtual void DoClone(
        TMapNode* sourceNode,
        TMapNode* clonedNode,
        NTransactionServer::TTransaction* transaction) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCypressNodeBase
{
    typedef std::vector<TCypressNodeBase*> TIndexToChild;
    typedef yhash_map<TCypressNodeBase*, int> TChildToIndex;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    explicit TListNode(const TVersionedNodeId& id);

    virtual void Save(const NCellMaster::TSaveContext& context) const override;
    virtual void Load(const NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    explicit TListNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

private:
    typedef TListNodeTypeHandler TThis;
    typedef TCypressNodeTypeHandlerBase<TListNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TListNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TListNode* node) override;

    virtual void DoBranch(
        const TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoMerge(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoClone(
        TListNode* node,
        TListNode* clonedNode,
        NTransactionServer::TTransaction* transaction) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
