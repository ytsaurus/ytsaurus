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

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    TCypressNodeTypeHandlerBase(TCypressManager::TPtr cypressManager)
        : CypressManager(cypressManager)
    {
        RegisterGetter("node_id", FromMethod(&TThis::GetNodeId));
        RegisterGetter("parent_id", FromMethod(&TThis::GetParentId));
        // NB: no smartpointer for this here
        RegisterGetter("type", FromMethod(&TThis::GetType, this));
    }

    virtual TAutoPtr<ICypressNode> Create(
        const TBranchedNodeId& id)
    {
        return new TImpl(id);
    }

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode::TPtr manifest)
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
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        YASSERT(node.GetState() != ENodeState::Branched);

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
        ICypressNode& originatingNode,
        ICypressNode& branchedNode)
    {
        YASSERT(originatingNode.GetState() != ENodeState::Branched);
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        // Set the parent of the originating node.
        // Valid parents are first set for branched copies at TCypressNodeProxyBase::AttachChild
        // and then propagates to their originators during transaction commit.
        originatingNode.SetParentId(branchedNode.GetParentId());

        // Drop the reference to attributes, if any.
        if (originatingNode.GetAttributesId() != NullNodeId) {
            auto& attrImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
                originatingNode.GetAttributesId(),
                NullTransactionId));
            CypressManager->UnrefNode(attrImpl);
        }

        // Replace the attributes with the branched copy.
        originatingNode.SetAttributesId(branchedNode.GetAttributesId());

        // Run custom merging.
        DoMerge(dynamic_cast<TImpl&>(originatingNode), dynamic_cast<TImpl&>(branchedNode));
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

protected:
    virtual void DoDestroy(TImpl& node)
    {
        UNUSED(node);
    }

    virtual void DoBranch(
        const TImpl& originatingNode,
        TImpl& branchedNode)
    {
        UNUSED(originatingNode);
        UNUSED(branchedNode);
    }

    virtual void DoMerge(
        TImpl& originatingNode,
        TImpl& branchedNode)
    {
        UNUSED(originatingNode);
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
        return NDetail::TCypressScalarTypeTraits<TValue>::RuntimeType;
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
        TScalarNode<TValue>& originatingNode,
        TScalarNode<TValue>& branchedNode)
    {
        originatingNode.Value() = branchedNode.Value();
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
    virtual NYTree::ENodeType GetNodeType();
    virtual Stroka GetTypeName();

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

private:
    typedef TMapNodeTypeHandler TThis;

    virtual void DoDestroy(TMapNode& node);

    virtual void DoBranch(
        const TMapNode& originatingNode,
        TMapNode& branchedNode);

    virtual void DoMerge(
        TMapNode& originatingNode,
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
    virtual NYTree::ENodeType GetNodeType();
    virtual Stroka GetTypeName();

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

private:
    typedef TListNodeTypeHandler TThis;

    virtual void DoDestroy(TListNode& node);

    virtual void DoBranch(
        const TListNode& originatingNode,
        TListNode& branchedNode);

    virtual void DoMerge(
        TListNode& originatingNode,
        TListNode& branchedNode);

    static void GetSize(const TGetAttributeParam& param);

};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypress
} // namespace NYT
