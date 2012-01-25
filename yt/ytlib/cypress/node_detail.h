#pragma once

#include "common.h"
#include "node.h"
#include "cypress_manager.h"
#include "ypath.pb.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/tree_builder.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl, class TProxy>
class TNodeBehaviorBase
    : public INodeBehavior
{
public:
    TNodeBehaviorBase(
        const ICypressNode& node,
        TCypressManager* cypressManager)
        : CypressManager(cypressManager)
        , NodeId(node.GetId().ObjectId)
    { }

    virtual void Destroy()
    { }

protected:
    TCypressManager::TPtr CypressManager;
    TNodeId NodeId;

    TImpl& GetImpl()
    {
        return CypressManager->GetNode(NodeId);
    }

    TIntrusivePtr<TProxy> GetProxy()
    {
        auto proxy = CypressManager->GetNodeProxy(NodeId, NullTransactionId);
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
    TCypressNodeTypeHandlerBase(TCypressManager* cypressManager)
        : CypressManager(cypressManager)
        , ObjectManager(cypressManager->GetObjectManager())
    { }

    virtual TAutoPtr<ICypressNode> Create(const TVersionedNodeId& id)
    {
        return new TImpl(id, GetObjectType());
    }

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode* manifest)
    {
        UNUSED(nodeId);
        UNUSED(transactionId);
        UNUSED(manifest);
        ythrow yexception() << Sprintf("Nodes of type %s cannot be created from a manifest",
            ~GetObjectType().ToString().Quote());
    }

    virtual void Destroy(ICypressNode& node)
    {
        auto id = node.GetId();
        if (ObjectManager->FindAttributes(id)) {
            ObjectManager->RemoveAttributes(id);
        }

        DoDestroy(dynamic_cast<TImpl&>(node));
    }

    virtual TAutoPtr<ICypressNode> Branch(
        const ICypressNode& committedNode,
        const TTransactionId& transactionId)
    {
        YASSERT(committedNode.GetState() == ENodeState::Committed);

        const auto& typedCommittedNode = dynamic_cast<const TImpl&>(committedNode);

        auto committedId = committedNode.GetId();
        auto branchedId = TVersionedNodeId(committedId.ObjectId, transactionId);

        // Create a branched copy.
        TAutoPtr<TImpl> branchedNode = new TImpl(branchedId, typedCommittedNode);

        // Branch user attributes.
        const auto* committedAttributes = ObjectManager->FindAttributes(committedId);
        if (committedAttributes) {
            auto* branchedAttributes = ObjectManager->CreateAttributes(branchedId);
            branchedAttributes->Attributes() = committedAttributes->Attributes();
        }

        // Run custom branching.
        DoBranch(typedCommittedNode, *branchedNode);

        return branchedNode.Release();
    }

    virtual void Merge(
        ICypressNode& committedNode,
        ICypressNode& branchedNode)
    {
        YASSERT(committedNode.GetState() == ENodeState::Committed);
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        auto committedId = committedNode.GetId();
        auto branchedId = branchedNode.GetId();

        // Merge user attributes.
        const auto* branchedAttributes = ObjectManager->FindAttributes(branchedId);
        if (branchedAttributes) {
            auto* committedAttributes = ObjectManager->FindAttributesForUpdate(committedId);
            if (!committedAttributes) {
                committedAttributes = ObjectManager->CreateAttributes(committedId);
            }
            committedAttributes->Attributes() = branchedAttributes->Attributes();
        } else {
            if (ObjectManager->FindAttributes(committedId)) {
                ObjectManager->RemoveAttributes(committedId);
            }
        }

        // Run custom merging.
        DoMerge(dynamic_cast<TImpl&>(committedNode), dynamic_cast<TImpl&>(branchedNode));
    }

    virtual INodeBehavior::TPtr CreateBehavior(const ICypressNode& node)
    {
        UNUSED(node);
        return NULL;
    }

protected:
    virtual void DoDestroy(TImpl& node)
    {
        UNUSED(node);
    }

    virtual void DoBranch(
        const TImpl& committedNode,
        TImpl& branchedNode)
    {
        UNUSED(committedNode);
        UNUSED(branchedNode);
    }

    virtual void DoMerge(
        TImpl& committedNode,
        TImpl& branchedNode)
    {
        UNUSED(committedNode);
        UNUSED(branchedNode);
    }

    TCypressManager::TPtr CypressManager;
    NObjectServer::TObjectManager::TPtr ObjectManager;

private:
    typedef TCypressNodeTypeHandlerBase<TImpl> TThis;

};

//////////////////////////////////////////////////////////////////////////////// 

class TCypressNodeBase
    : public ICypressNode
{
    // This also overrides appropriate methods from ICypressNode.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TLockId>, LockIds);
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, ParentId);
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, State);

public:
    TCypressNodeBase(const TVersionedNodeId& id, EObjectType objectType);
    TCypressNodeBase(const TVersionedNodeId& id, const TCypressNodeBase& other);

    virtual EObjectType GetObjectType() const;
    virtual TVersionedNodeId GetId() const;

    virtual i32 RefObject();
    virtual i32 UnrefObject();
    virtual i32 GetObjectRefCounter() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

protected:
    TVersionedNodeId Id;
    EObjectType ObjectType;
    i32 RefCounter;

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
    static const EObjectType::EDomain ObjectType;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : NYTree::NDetail::TScalarTypeTraits<i64>
{
    static const EObjectType::EDomain ObjectType;
};

template <>
struct TCypressScalarTypeTraits<double>
    : NYTree::NDetail::TScalarTypeTraits<double>
{
    static const EObjectType::EDomain ObjectType;
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
    TScalarNode(const TVersionedNodeId& id, EObjectType objectType)
        : TCypressNodeBase(id, objectType)
    { }

    TScalarNode(const TVersionedNodeId& id, const TThis& other)
        : TCypressNodeBase(id, other)
        , Value_(other.Value_)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TThis(Id, *this);
    }

    virtual void Save(TOutputStream* output) const
    {
        TCypressNodeBase::Save(output);
        ::Save(output, Value_);
    }
    
    virtual void Load(TInputStream* input)
    {
        TCypressNodeBase::Load(input);
        ::Load(input, Value_);
    }
};

typedef TScalarNode<Stroka> TStringNode;
typedef TScalarNode<i64>    TInt64Node;
typedef TScalarNode<double> TDoubleNode;

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue>
class TScalarNodeTypeHandler
    : public TCypressNodeTypeHandlerBase< TScalarNode<TValue> >
{
public:
    TScalarNodeTypeHandler(TCypressManager* cypressManager)
        : TCypressNodeTypeHandlerBase< TScalarNode<TValue> >(cypressManager)
    { }

    virtual EObjectType GetObjectType()
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    virtual NYTree::ENodeType GetNodeType()
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

protected:
    virtual void DoMerge(
        TScalarNode<TValue>& committedNode,
        TScalarNode<TValue>& branchedNode)
    {
        committedNode.Value() = branchedNode.Value();
    }

};

typedef TScalarNodeTypeHandler<Stroka> TStringNodeTypeHandler;
typedef TScalarNodeTypeHandler<i64>    TInt64NodeTypeHandler;
typedef TScalarNodeTypeHandler<double> TDoubleNodeTypeHandler;

//////////////////////////////////////////////////////////////////////////////// 

class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TNodeId> TKeyToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);

public:
    TMapNode(const TVersionedNodeId& id, EObjectType objectType);
    TMapNode(const TVersionedNodeId& id, const TMapNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);


};

//////////////////////////////////////////////////////////////////////////////// 

class TMapNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TMapNode>
{
public:
    TMapNodeTypeHandler(TCypressManager* cypressManager);

    virtual EObjectType GetObjectType();
    virtual NYTree::ENodeType GetNodeType();

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

private:
    typedef TMapNodeTypeHandler TThis;

    virtual void DoDestroy(TMapNode& node);

    virtual void DoBranch(
        const TMapNode& committedNode,
        TMapNode& branchedNode);

    virtual void DoMerge(
        TMapNode& committedNode,
        TMapNode& branchedNode);

};

//////////////////////////////////////////////////////////////////////////////// 

class TListNode
    : public TCypressNodeBase
{
    typedef yvector<TNodeId> TIndexToChild;
    typedef yhash_map<TNodeId, int> TChildToIndex;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    explicit TListNode(const TVersionedNodeId& id, EObjectType objectType);
    TListNode(const TVersionedNodeId& id, const TListNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

};

//////////////////////////////////////////////////////////////////////////////// 

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    TListNodeTypeHandler(
        TCypressManager* cypressManager);

    virtual EObjectType GetObjectType();
    virtual NYTree::ENodeType GetNodeType();

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

private:
    typedef TListNodeTypeHandler TThis;

    virtual void DoDestroy(TListNode& node);

    virtual void DoBranch(
        const TListNode& committedNode,
        TListNode& branchedNode);

    virtual void DoMerge(
        TListNode& originatingNode,
        TListNode& branchedNode);

};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypress
} // namespace NYT
