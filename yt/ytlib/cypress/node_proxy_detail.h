#pragma once

#include "common.h"
#include "node_proxy.h"
#include "attribute_detail.h"

#include "../ytree/ytree.h"
#include "../ytree/ypath.h"
#include "../ytree/ypath_detail.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public INodeFactory
{
public:
    TNodeFactory(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId);

    virtual IStringNode::TPtr CreateString();
    virtual IInt64Node::TPtr CreateInt64();
    virtual IDoubleNode::TPtr CreateDouble();
    virtual IMapNode::TPtr CreateMap();
    virtual IListNode::TPtr CreateList();
    virtual IEntityNode::TPtr CreateEntity();

private:
    TCypressManager::TPtr CypressManager;
    TTransactionId TransactionId;

};

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCypressNodeProxyBase
    : public TNodeBase
    , public ICypressNodeProxy
    , public virtual IBase
{
public:
    typedef TIntrusivePtr<TCypressNodeProxyBase> TPtr;

    TCypressNodeProxyBase(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : CypressManager(cypressManager)
        , TransactionId(transactionId)
        , NodeId(nodeId)
        , NodeFactory(cypressManager, transactionId)
        , Locked(false)
    {
        YASSERT(~cypressManager != NULL);
    }

    INodeFactory* GetFactory() const
    {
        return &NodeFactory;
    }

    virtual TTransactionId GetTransactionId() const
    {
        return TransactionId;
    }

    virtual TNodeId GetNodeId() const
    {
        return NodeId;
    }


    virtual const ICypressNode& GetImpl() const
    {
        return this->GetTypedImpl();
    }

    virtual ICypressNode& GetImplForUpdate()
    {
        return this->GetTypedImplForUpdate();
    }


    virtual ICompositeNode::TPtr GetParent() const
    {
        return GetProxy<ICompositeNode>(GetImpl().GetParentId());
    }

    virtual void SetParent(ICompositeNode::TPtr parent)
    {
        auto proxy = ToProxy(INode::TPtr(~parent));
        GetImplForUpdate().SetParentId(~proxy == NULL ? NullNodeId : proxy->GetNodeId());
    }


    virtual IMapNode::TPtr GetAttributes() const
    {
        return GetProxy<IMapNode>(GetImpl().GetAttributesId());
    }

    virtual void SetAttributes(IMapNode::TPtr attributes)
    {
        auto& impl = GetImplForUpdate();
        if (impl.GetAttributesId() != NullNodeId) {
            auto& attrImpl = GetImplForUpdate(impl.GetAttributesId());
            DetachChild(attrImpl);
            impl.SetAttributesId(NullNodeId);
        }

        if (~attributes != NULL) {
            auto attrProxy = ToProxy(INode::TPtr(~attributes));
            auto& attrImpl = GetImplForUpdate(attrProxy->GetNodeId());
            AttachChild(attrImpl);
            impl.SetAttributesId(attrProxy->GetNodeId());
        }
    }

    virtual Stroka GetTypeName() const
    {
        switch (GetType()) {
            case ENodeType::String: return "string";
            case ENodeType::Int64:  return "int64";
            case ENodeType::Double: return "double";
            case ENodeType::Map:    return "map";
            case ENodeType::List:   return "list";
            default: YUNREACHABLE();
        }
    }


protected:
    TCypressManager::TPtr CypressManager;
    TTransactionId TransactionId;
    TNodeId NodeId;
    mutable TNodeFactory NodeFactory;
    //! Keeps a cached flag that gets raised when the node is locked.
    bool Locked;


    const ICypressNode& GetImpl(const TNodeId& nodeId) const
    {
        return CypressManager->GetTransactionNode(nodeId, TransactionId);
    }

    ICypressNode& GetImplForUpdate(const TNodeId& nodeId) const
    {
        return CypressManager->GetTransactionNodeForUpdate(nodeId, TransactionId);
    }


    const TImpl& GetTypedImpl() const
    {
        return dynamic_cast<const TImpl&>(GetImpl(NodeId));
    }

    TImpl& GetTypedImplForUpdate()
    {
        return dynamic_cast<TImpl&>(GetImplForUpdate(NodeId));
    }


    template <class T>
    TIntrusivePtr<T> GetProxy(const TNodeId& nodeId) const
    {
        if (nodeId == NullNodeId) {
            return NULL;
        }

        auto& impl = CypressManager->GetTransactionNode(nodeId, TransactionId);
        auto proxy = impl.GetProxy(CypressManager, TransactionId);
        return dynamic_cast<T*>(~proxy);
    }

    static typename ICypressNodeProxy::TPtr ToProxy(INode::TPtr node)
    {
        return dynamic_cast<ICypressNodeProxy*>(~node);
    }


    void EnsureLocked()
    {
        // A shortcut.
        if (Locked)
            return;

        if (CypressManager->IsTransactionNodeLocked(NodeId, TransactionId))
            return;

        LockSelf();
    }

    TLockResult LockSelf()
    {
        CypressManager->LockTransactionNode(NodeId, TransactionId);

        // Set the flag to speedup further checks.
        Locked = true;

        return TLockResult::CreateDone();
    }


    void AttachChild(ICypressNode& child)
    {
        YASSERT(child.GetState() == ENodeState::Uncommitted);
        child.SetParentId(NodeId);
        CypressManager->RefNode(child);
    }

    void DetachChild(ICypressNode& child)
    {
        child.SetParentId(NullNodeId);
        CypressManager->UnrefNode(child);
    }


    virtual IAttributeProvider* GetAttributeProvider()
    {
        return TCypressNodeAttributeProvider::Get();
    }

    virtual yvector<Stroka> GetVirtualAttributeNames()
    {
        yvector<Stroka> names;
        GetAttributeProvider()->GetAttributeNames(CypressManager, this, names);
        return names;
    }

    virtual bool GetVirtualAttribute(const Stroka& name, IYsonConsumer* consumer)
    {
        return GetAttributeProvider()->GetAttribute(CypressManager, this, name, consumer);
    }
};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    TScalarNodeProxy(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            cypressManager,
            transactionId,
            nodeId)
    { }

    virtual TValue GetValue() const
    {
        return this->GetTypedImpl().Value();
    }

    virtual void SetValue(const TValue& value)
    {
        this->EnsureLocked();
        this->GetTypedImplForUpdate().Value() = value;
    }
};

//////////////////////////////////////////////////////////////////////////////// 

#define DECLARE_TYPE_OVERRIDES(name) \
public: \
    virtual ENodeType GetType() const \
    { \
        return ENodeType::name; \
    } \
    \
    virtual TIntrusivePtr<const I ## name ## Node> As ## name() const \
    { \
        return this; \
    } \
    \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() \
    { \
        return this; \
    } \
    \
    virtual TSetResult SetSelf(TYsonProducer::TPtr producer) \
    { \
        SetNodeFromProducer(TIntrusivePtr<I##name##Node>(this), producer); \
        return TSetResult::CreateDone(); \
    }

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## NodeProxy \
        : public TScalarNodeProxy<type, I ## name ## Node, T ## name ## Node> \
    { \
        DECLARE_TYPE_OVERRIDES(name) \
    \
    public: \
        T ## name ## NodeProxy( \
            TCypressManager::TPtr cypressManager, \
            const TTransactionId& transactionId, \
            const TNodeId& nodeId) \
            : TScalarNodeProxy<type, I ## name ## Node, T ## name ## Node>( \
                cypressManager, \
                transactionId, \
                nodeId) \
        { } \
    }; \
    \
    template <> \
    inline ICypressNodeProxy::TPtr TScalarNode<type>::GetProxy( \
        TIntrusivePtr<TCypressManager> cypressManager, \
        const TTransactionId& transactionId) const \
    { \
        return New<T ## name ## NodeProxy>(cypressManager, transactionId, Id.NodeId); \
    }


DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCompositeNodeProxyBase
    : public TCypressNodeProxyBase<IBase, TImpl>
{
protected:
    TCompositeNodeProxyBase(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            cypressManager,
            transactionId,
            nodeId)
    { }

public:
    virtual TIntrusivePtr<const ICompositeNode> AsComposite() const
    {
        return const_cast<TCompositeNodeProxyBase*>(this);
    }

    virtual TIntrusivePtr<ICompositeNode> AsComposite()
    {
        return this;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeProxy
    : public TCompositeNodeProxyBase<IMapNode, TMapNode>
    , public TMapNodeMixin
{
    DECLARE_TYPE_OVERRIDES(Map)

public:
    TMapNodeProxy(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, INode::TPtr> > GetChildren() const;
    virtual INode::TPtr FindChild(const Stroka& name) const;
    virtual bool AddChild(INode::TPtr child, const Stroka& name);
    virtual bool RemoveChild(const Stroka& name);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

private:
    virtual IAttributeProvider* GetAttributeProvider();

    virtual TSetResult SetRecursive(TYPath path, TYsonProducer::TPtr producer);
    virtual TNavigateResult NavigateRecursive(TYPath path);

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCompositeNodeProxyBase<IListNode, TListNode>
    , public TListNodeMixin
{
    DECLARE_TYPE_OVERRIDES(List)

public:
    TListNodeProxy(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector<INode::TPtr> GetChildren() const;
    virtual INode::TPtr FindChild(int index) const;
    virtual void AddChild(INode::TPtr child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

private:
    virtual IAttributeProvider* GetAttributeProvider();

    virtual TNavigateResult NavigateRecursive(TYPath path);
    virtual TSetResult SetRecursive(TYPath path, TYsonProducer::TPtr producer);

};

////////////////////////////////////////////////////////////////////////////////

#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
