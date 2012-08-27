#pragma once

#include "node.h"
#include "cypress_manager.h"

#include <ytlib/misc/serialize.h>

#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ypath.pb.h>

#include <server/object_server/object_detail.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <ytlib/meta_state/meta_state_manager.h>

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
        const TNodeId& nodeId)
        : Bootstrap(bootstrap)
        , NodeId(nodeId)
    { }

    virtual void Destroy() override
    { }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    TNodeId NodeId;

    TImpl* GetImpl()
    {
        return Bootstrap->GetCypressManager()->GetNode(NodeId);
    }

    TIntrusivePtr<TProxy> GetProxy()
    {
        auto proxy = Bootstrap->GetCypressManager()->GetVersionedNodeProxy(NodeId);
        auto* typedProxy = dynamic_cast<TProxy*>(~proxy);
        YASSERT(typedProxy);
        return typedProxy;
    }

};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    explicit TCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual TAutoPtr<ICypressNode> Instantiate(const TVersionedNodeId& id) override
    {
        return new TImpl(id);
    }

    virtual TAutoPtr<ICypressNode> Create(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        // TODO(babenko): Release is needed due to cast to ICypressNode.
        return DoCreate(transaction, request, response).Release();
    }

    virtual void Destroy(ICypressNode* node) override
    {
        auto id = node->GetId();
        auto objectManager = Bootstrap->GetObjectManager();
        if (objectManager->FindAttributes(id)) {
            objectManager->RemoveAttributes(id);
        }

        DoDestroy(dynamic_cast<TImpl*>(node));
    }

    virtual TAutoPtr<ICypressNode> Branch(
        const ICypressNode* originatingNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode) override
    {
        auto originatingId = originatingNode->GetId();
        auto branchedId = TVersionedNodeId(originatingId.ObjectId, GetObjectId(transaction));

        // Create a branched copy.
        TAutoPtr<TImpl> branchedNode(new TImpl(branchedId));
        branchedNode->SetParentId(originatingNode->GetParentId());
        branchedNode->SetCreationTime(originatingNode->GetCreationTime());
        branchedNode->SetLockMode(mode);
        branchedNode->SetTrunkNode(originatingNode->GetTrunkNode());

        // Branch user attributes.
        Bootstrap->GetObjectManager()->BranchAttributes(originatingId, branchedId);
        
        // Run custom branching.
        DoBranch(dynamic_cast<const TImpl*>(originatingNode), ~branchedNode);

        return branchedNode.Release();
    }

    virtual void Merge(
        ICypressNode* originatingNode,
        ICypressNode* branchedNode) override
    {
        auto originatingId = originatingNode->GetId();
        auto branchedId = branchedNode->GetId();
        YASSERT(branchedId.IsBranched());

        // Merge user attributes.
        Bootstrap->GetObjectManager()->MergeAttributes(originatingId, branchedId);

        // Merge parent id.
        originatingNode->SetParentId(branchedNode->GetParentId());

        // Run custom merging.
        DoMerge(dynamic_cast<TImpl*>(originatingNode), dynamic_cast<TImpl*>(branchedNode));
    }

    virtual INodeBehaviorPtr CreateBehavior(const TNodeId& id) override
    {
        UNUSED(id);
        return NULL;
    }

protected:
    NCellMaster::TBootstrap* Bootstrap;

    virtual TAutoPtr<TImpl> DoCreate(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response)
    {
        UNUSED(transaction);
        UNUSED(request);
        UNUSED(response);

        auto objectManager = Bootstrap->GetObjectManager();
        auto nodeId = objectManager->GenerateId(GetObjectType());
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

    bool IsRecovery() const
    {
        return Bootstrap->GetMetaStateFacade()->GetManager()->IsRecovery();
    }

private:
    typedef TCypressNodeTypeHandlerBase<TImpl> TThis;

};

//////////////////////////////////////////////////////////////////////////////// 

class TCypressNodeBase
    : public NObjectServer::TObjectBase
    , public ICypressNode
{
    // This also overrides appropriate methods from ICypressNode.
    DEFINE_BYREF_RW_PROPERTY(TLockMap, Locks);

    DEFINE_BYVAL_RW_PROPERTY(TNodeId, ParentId);
    DEFINE_BYVAL_RW_PROPERTY(ELockMode, LockMode);
    DEFINE_BYVAL_RW_PROPERTY(ICypressNode*, TrunkNode);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);

public:
    explicit TCypressNodeBase(const TVersionedNodeId& id);

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual const TVersionedNodeId& GetId() const override;

    virtual i32 RefObject() override;
    virtual i32 UnrefObject() override;
    virtual i32 GetObjectRefCounter() const override;

    virtual void Save(TOutputStream* output) const override;
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input) override;

protected:
    TVersionedNodeId Id;

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

    virtual void Save(TOutputStream* output) const override
    {
        TCypressNodeBase::Save(output);
        ::Save(output, Value_);
    }
    
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input) override
    {
        TCypressNodeBase::Load(context, input);
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
    TScalarNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
        : TCypressNodeTypeHandlerBase< TScalarNode<TValue> >(bootstrap)
    { }

    virtual NObjectClient::EObjectType GetObjectType() override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    virtual NYTree::ENodeType GetNodeType() override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    virtual ICypressNodeProxyPtr GetProxy(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

protected:
    virtual void DoBranch(
        const TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        branchedNode->Value() = originatingNode->Value();
    }

    virtual void DoMerge(
        TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        originatingNode->Value() = branchedNode->Value();
    }

};

typedef TScalarNodeTypeHandler<Stroka> TStringNodeTypeHandler;
typedef TScalarNodeTypeHandler<i64>    TIntegerNodeTypeHandler;
typedef TScalarNodeTypeHandler<double> TDoubleNodeTypeHandler;

//////////////////////////////////////////////////////////////////////////////// 

class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TNodeId> TKeyToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);
    DEFINE_BYREF_RW_PROPERTY(i32, ChildCountDelta);

public:
    explicit TMapNode(const TVersionedNodeId& id);

    virtual void Save(TOutputStream* output) const override;
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input) override;
};

//////////////////////////////////////////////////////////////////////////////// 

class TMapNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TMapNode>
{
public:
    explicit TMapNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

    virtual ICypressNodeProxyPtr GetProxy(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

private:
    typedef TMapNodeTypeHandler TThis;

    virtual void DoDestroy(TMapNode* node) override;

    virtual void DoBranch(
        const TMapNode* originatingNode,
        TMapNode* branchedNode) override;

    virtual void DoMerge(
        TMapNode* originatingNode,
        TMapNode* branchedNode) override;
};

//////////////////////////////////////////////////////////////////////////////// 

class TListNode
    : public TCypressNodeBase
{
    typedef std::vector<TNodeId> TIndexToChild;
    typedef yhash_map<TNodeId, int> TChildToIndex;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    explicit TListNode(const TVersionedNodeId& id);

    virtual void Save(TOutputStream* output) const override;
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input) override;

};

//////////////////////////////////////////////////////////////////////////////// 

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    explicit TListNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

    virtual ICypressNodeProxyPtr GetProxy(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

private:
    typedef TListNodeTypeHandler TThis;

    virtual void DoDestroy(TListNode* node) override;

    virtual void DoBranch(
        const TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoMerge(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypressServer
} // namespace NYT
