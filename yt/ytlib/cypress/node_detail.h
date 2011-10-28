#pragma once

#include "common.h"

#include "node.h"
#include "cypress_manager.h"
#include "../misc/serialize.h"
#include "../ytree/node_detail.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    TCypressNodeTypeHandlerBase(TCypressManager::TPtr cypressManager)
        : CypressManager(cypressManager)
    {
        RegisterGetter("node_id", FromMethod(&TThis::GetNodeId));
        // NB: no smartpointer for this here
        RegisterGetter("type", FromMethod(&TThis::GetType, this));
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TBranchedNodeId& id)
    {
        return new TImpl(id);
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode::TPtr description)
    {
        UNUSED(nodeId);
        UNUSED(transactionId);
        UNUSED(description);
        throw TYTreeException() << Sprintf("Cannot create a node of type %s via dynamic interface",
            ~GetTypeName().Quote());
    }

    virtual void Destroy(ICypressNode& node)
    {
        // Release the reference to the attributes, if any.
        if (node.GetAttributesId() != NullNodeId) {
            auto& attrImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
                node.GetAttributesId(),
                NullTransactionId));
            CypressManager->UnrefNode(attrImpl);
        }

        DoDestroy(dynamic_cast<TImpl&>(node));
    }

    virtual TAutoPtr<ICypressNode> Branch(
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        YASSERT(node.GetState() == ENodeState::Committed);

        const auto& typedNode = dynamic_cast<const TImpl&>(node);

        // Create a branched copy.
        TAutoPtr<TImpl> branchedNode = new TImpl(
            TBranchedNodeId(node.GetId().NodeId, transactionId),
            typedNode);

        // Add a reference to the attributes, if any.
        if (node.GetAttributesId() != NullNodeId) {
            auto& attrImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
                node.GetAttributesId(),
                NullTransactionId));
            CypressManager->RefNode(attrImpl);
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
            auto& attrImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
                committedNode.GetAttributesId(),
                NullTransactionId));
            CypressManager->UnrefNode(attrImpl);
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

    virtual bool GetAttribute(
        const ICypressNode& node,
        const Stroka& name,
        IYsonConsumer* consumer)
    {
        auto it = Getters.find(name);
        if (it == Getters.end())
            return false;

        TGetAttributeRequest request;
        request.Node = &dynamic_cast<const TImpl&>(node);
        request.Consumer = consumer;

        it->Second()->Do(request);
        return true;
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

    // TODO: rename
    struct TGetAttributeRequest
    {
        const TImpl* Node;
        IYsonConsumer* Consumer;
    };

    typedef IParamAction<const TGetAttributeRequest&> TGetter;

    yhash_map<Stroka, typename TGetter::TPtr> Getters;

    void RegisterGetter(const Stroka& name, typename TGetter::TPtr getter)
    {
        YVERIFY(Getters.insert(MakePair(name, getter)).Second());
    }

    static void GetNodeId(const TGetAttributeRequest& request)
    {
        NYTree::BuildYsonFluently(request.Consumer)
            .Scalar(request.Node->GetId().NodeId.ToString());
    }

    void GetType(const TGetAttributeRequest& request)
    {
        NYTree::BuildYsonFluently(request.Consumer)
            .Scalar(GetTypeName());
    }

private:
    typedef TCypressNodeTypeHandlerBase<TImpl> TThis;

};

//////////////////////////////////////////////////////////////////////////////// 

class TCypressNodeBase
    : public ICypressNode
{
    // This also overrides appropriate methods from ICypressNode.
    DECLARE_BYREF_RW_PROPERTY(Locks, yhash_set<TLockId>);
    DECLARE_BYVAL_RW_PROPERTY(ParentId, TNodeId);
    DECLARE_BYVAL_RW_PROPERTY(AttributesId, TNodeId);
    DECLARE_BYVAL_RW_PROPERTY(State, ENodeState);

public:
    explicit TCypressNodeBase(const TBranchedNodeId& id);

    virtual TBranchedNodeId GetId() const;

    virtual i32 Ref();
    virtual i32 Unref();

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

protected:
    TCypressNodeBase(const TBranchedNodeId& id, const TCypressNodeBase& other);

    TBranchedNodeId Id;
    i32 RefCounter;

};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue>
struct TCypressScalarTypeTraits
{ };

template <>
struct TCypressScalarTypeTraits<Stroka>
    : TScalarTypeTraits<Stroka>
{
    static const ERuntimeNodeType::EDomain RuntimeType;
    static const char* TypeName;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : TScalarTypeTraits<i64>
{
    static const ERuntimeNodeType::EDomain RuntimeType;
    static const char* TypeName;
};

template <>
struct TCypressScalarTypeTraits<double>
    : TScalarTypeTraits<double>
{
    static const ERuntimeNodeType::EDomain RuntimeType;
    static const char* TypeName;
};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue>
class TScalarNode
    : public TCypressNodeBase
{
    typedef TScalarNode<TValue> TThis;

    DECLARE_BYREF_RW_PROPERTY(Value, TValue)

public:
    explicit TScalarNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

    TScalarNode(const TBranchedNodeId& id, const TThis& other)
        : TCypressNodeBase(id, other)
        , Value_(other.Value_)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TThis(Id, *this);
    }

    virtual ERuntimeNodeType GetRuntimeType() const
    {
        return TCypressScalarTypeTraits<TValue>::RuntimeType;
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
    TScalarNodeTypeHandler(TCypressManager::TPtr cypressManager)
        : TCypressNodeTypeHandlerBase< TScalarNode<TValue> >(cypressManager)
    { }

    virtual ERuntimeNodeType GetRuntimeType()
    {
        return TCypressScalarTypeTraits<TValue>::RuntimeType;
    }

    virtual Stroka GetTypeName()
    {
        return TCypressScalarTypeTraits<TValue>::TypeName;
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

    DECLARE_BYREF_RW_PROPERTY(NameToChild, TNameToChild);
    DECLARE_BYREF_RW_PROPERTY(ChildToName, TChildToName);

public:
    explicit TMapNode(const TBranchedNodeId& id);
    TMapNode(const TBranchedNodeId& id, const TMapNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

};

//////////////////////////////////////////////////////////////////////////////// 

class TMapNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TMapNode>
{
public:
    TMapNodeTypeHandler(TCypressManager::TPtr cypressManager);

    virtual ERuntimeNodeType GetRuntimeType();
    virtual Stroka GetTypeName();

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

protected:
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

    DECLARE_BYREF_RW_PROPERTY(IndexToChild, TIndexToChild);
    DECLARE_BYREF_RW_PROPERTY(ChildToIndex, TChildToIndex);

public:
    explicit TListNode(const TBranchedNodeId& id);
    TListNode(const TBranchedNodeId& id, const TListNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

};

//////////////////////////////////////////////////////////////////////////////// 

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    TListNodeTypeHandler(TCypressManager::TPtr cypressManager);

    virtual ERuntimeNodeType GetRuntimeType();
    virtual Stroka GetTypeName();

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

protected:
    virtual void DoDestroy(TListNode& node);

    virtual void DoBranch(
        const TListNode& committedNode,
        TListNode& branchedNode);
    virtual void DoMerge(
        TListNode& committedNode,
        TListNode& branchedNode);

};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypress
} // namespace NYT
