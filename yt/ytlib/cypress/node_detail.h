#pragma once

#include "common.h"
#include "node.h"
#include "cypress_manager.h"
#include "ypath_rpc.pb.h"

#include "../misc/serialize.h"
#include "../ytree/node_detail.h"
#include "../ytree/fluent.h"
#include "../ytree/ephemeral.h"
#include "../ytree/tree_builder.h"

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
        return CypressManager->GetNode(TBranchedNodeId(NodeId, NullTransactionId));
    }

    TIntrusivePtr<TProxy> GetProxy()
    {
        auto proxy = CypressManager->GetNodeProxy(NodeId, NullTransactionId);
        auto* typedProxy = dynamic_cast<TProxy*>(~proxy);
        YASSERT(typedProxy != NULL);
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
        const TBranchedNodeId& id)
    {
        return new TImpl(id, GetRuntimeType());
    }

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::INode* manifest)
    {
        UNUSED(nodeId);
        UNUSED(transactionId);
        UNUSED(manifest);
        ythrow yexception() << Sprintf("Nodes of type %s cannot be created from a manifest",
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
        const ICypressNode& committedNode,
        const TTransactionId& transactionId)
    {
        YASSERT(committedNode.GetState() == ENodeState::Committed);

        const auto& typedNode = dynamic_cast<const TImpl&>(committedNode);

        // Create a branched copy.
        TAutoPtr<TImpl> branchedNode = new TImpl(
            TBranchedNodeId(committedNode.GetId().NodeId, transactionId),
            typedNode);

        // Add a reference to the attributes, if any.
        if (committedNode.GetAttributesId() != NullNodeId) {
            auto& attrImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
                committedNode.GetAttributesId(),
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
        YASSERT(committedNode.GetState() != ENodeState::Branched);
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

        auto result = builder->EndTree();

        return NYTree::IYPathService::FromNode(~result);
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
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TLockId>, LockIds);
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, ParentId);
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, AttributesId);
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, State);

public:
    TCypressNodeBase(const TBranchedNodeId& id, ERuntimeNodeType runtimeType);
    TCypressNodeBase(const TBranchedNodeId& id, const TCypressNodeBase& other);

    virtual ERuntimeNodeType GetRuntimeType() const;
    virtual TBranchedNodeId GetId() const;

    virtual i32 Ref();
    virtual i32 Unref();
    virtual i32 GetRefCounter() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

protected:
    TBranchedNodeId Id;
    ERuntimeNodeType RuntimeType;
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
    static const ERuntimeNodeType::EDomain RuntimeType;
    static const char* TypeName;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : NYTree::NDetail::TScalarTypeTraits<i64>
{
    static const ERuntimeNodeType::EDomain RuntimeType;
    static const char* TypeName;
};

template <>
struct TCypressScalarTypeTraits<double>
    : NYTree::NDetail::TScalarTypeTraits<double>
{
    static const ERuntimeNodeType::EDomain RuntimeType;
    static const char* TypeName;
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
    TScalarNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
        : TCypressNodeBase(id, runtimeType)
    { }

    TScalarNode(const TBranchedNodeId& id, const TThis& other)
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

    virtual ERuntimeNodeType GetRuntimeType()
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::RuntimeType;
    }

    virtual NYTree::ENodeType GetNodeType()
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    virtual Stroka GetTypeName()
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::TypeName;
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
    TMapNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType);
    TMapNode(const TBranchedNodeId& id, const TMapNode& other);

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

    virtual ERuntimeNodeType GetRuntimeType();
    virtual NYTree::ENodeType GetNodeType();
    virtual Stroka GetTypeName();

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
    explicit TListNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType);
    TListNode(const TBranchedNodeId& id, const TListNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual void Save(TOutputStream* output) const;
    virtual void Load(TInputStream* input);

};

//////////////////////////////////////////////////////////////////////////////// 

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    TListNodeTypeHandler(TCypressManager* cypressManager);

    virtual ERuntimeNodeType GetRuntimeType();
    virtual NYTree::ENodeType GetNodeType();
    virtual Stroka GetTypeName();

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
