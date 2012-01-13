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
        , NodeId(node.GetId().NodeId)
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
    {
        RegisterGetter("node_id", FromMethod(&TThis::GetNodeId));
        RegisterGetter("parent_id", FromMethod(&TThis::GetParentId));
        // NB: No smartpointer for this here.
        RegisterGetter("type", FromMethod(&TThis::GetType, this));
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TVersionedNodeId& id)
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
        // Release the reference to the attributes, if any.
        if (node.GetAttributesId() != NullNodeId) {
            // TODO(babenko): fixme
            //auto& attrImpl = CypressManager->GetNodeForUpdate(TVersionedNodeId(
            //    node.GetAttributesId(),
            //    NullTransactionId));
            //CypressManager->UnrefNode(attrImpl);
        }

        DoDestroy(dynamic_cast<TImpl&>(node));
    }

    virtual TAutoPtr<ICypressNode> Branch(
        const ICypressNode& committedNode,
        const TTransactionId& transactionId)
    {
        YASSERT(committedNode.GetState() == ENodeState::Committed);

        const auto& typedNode = dynamic_cast<const TImpl&>(committedNode);

        // Create a branched copy.
        TAutoPtr<TImpl> branchedNode = new TImpl(
            TVersionedNodeId(committedNode.GetId().NodeId, transactionId),
            typedNode);

        // Add a reference to the attributes, if any.
        if (committedNode.GetAttributesId() != NullNodeId) {
            //auto& attrImpl = CypressManager->GetNodeForUpdate(TVersionedNodeId(
            //    committedNode.GetAttributesId(),
            //    NullTransactionId));
            //CypressManager->RefNode(attrImpl);
            // TODO(babenko): fixme
        }

        // Run custom branching.
        DoBranch(typedNode, *branchedNode);

        return branchedNode.Release();
    }

    virtual void Merge(
        ICypressNode& committedNode,
        ICypressNode& branchedNode)
    {
        YASSERT(committedNode.GetState() == ENodeState::Committed);
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        // Drop the reference to attributes, if any.
        if (committedNode.GetAttributesId() != NullNodeId) {
            //auto& attrImpl = CypressManager->GetNodeForUpdate(TVersionedNodeId(
            //    committedNode.GetAttributesId(),
            //    NullTransactionId));
            //CypressManager->UnrefNode(attrImpl);
            // TODO(babenko): fixme
        }

        // Replace the attributes with the branched copy.
        committedNode.SetAttributesId(branchedNode.GetAttributesId());

        // Run custom merging.
        DoMerge(dynamic_cast<TImpl&>(committedNode), dynamic_cast<TImpl&>(branchedNode));
    }

    virtual void GetAttributeNames(
        const ICypressNode& node,
        yvector<Stroka>* names)
    {
        UNUSED(node);
        FOREACH (const auto& pair, Getters) {
            names->push_back(pair.First());
        }
    }

    virtual NYTree::IYPathService::TPtr GetAttributeService(
        const ICypressNode& node,
        const Stroka& name)
    {
        auto it = Getters.find(name);
        if (it == Getters.end())
            return NULL;

        auto builder = CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
        builder->BeginTree();

        TGetAttributeParam param;
        param.Node = &dynamic_cast<const TImpl&>(node);
        param.Consumer = builder.Get();
        it->Second()->Do(param);

        return builder->EndTree();
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

    struct TGetAttributeParam
    {
        const TImpl* Node;
        NYTree::IYsonConsumer* Consumer;
    };

    typedef IParamAction<const TGetAttributeParam&> TGetter;

    yhash_map<Stroka, typename TGetter::TPtr> Getters;

    void RegisterGetter(const Stroka& name, typename TGetter::TPtr getter)
    {
        YVERIFY(Getters.insert(MakePair(name, getter)).Second());
    }

    static void GetNodeId(const TGetAttributeParam& param)
    {
        NYTree::BuildYsonFluently(param.Consumer)
            .Scalar(param.Node->GetId().NodeId.ToString());
    }

    static void GetParentId(const TGetAttributeParam& param)
    {
        NYTree::BuildYsonFluently(param.Consumer)
            .Scalar(param.Node->GetParentId().ToString());
    }

    void GetType(const TGetAttributeParam& param)
    {
        NYTree::BuildYsonFluently(param.Consumer)
            // TODO(babenko): convert camel case to underscore
            .Scalar(GetObjectType().ToString());
    }

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
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, AttributesId);
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
    typedef yhash_map<Stroka, TNodeId> TNameToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToName;

    DEFINE_BYREF_RW_PROPERTY(TNameToChild, NameToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToName, ChildToName);

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

    static void GetSize(const TGetAttributeParam& param);

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

    static void GetSize(const TGetAttributeParam& param);

};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypress
} // namespace NYT
