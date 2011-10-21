#pragma once

#include "common.h"
#include "cypress_manager.h"

#include "../ytree/ytree.h"
#include "../ytree/ypath.h"
#include "../ytree/ypath_detail.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy
    : public virtual TRefCountedBase
    , public virtual INode
{
    typedef TIntrusivePtr<ICypressNodeProxy> TPtr;

    virtual TTransactionId GetTransactionId() const = 0;
    virtual TNodeId GetNodeId() const = 0;

    virtual const ICypressNode& GetImpl() const = 0;
    virtual ICypressNode& GetImplForUpdate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IAttributeProvider
{
    virtual ~IAttributeProvider()
    { }

    virtual void GetAttributeNames(
        TCypressManager::TPtr cypressManager,
        ICypressNodeProxy::TPtr proxy,
        yvector<Stroka>& names) = 0;

    virtual bool GetAttribute(
        TCypressManager::TPtr cypressManager,
        ICypressNodeProxy::TPtr proxy,
        const Stroka& name,
        IYsonConsumer* consumer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TAttributeProviderBase
    : public IAttributeProvider
{
public:
    virtual void GetAttributeNames(
        TCypressManager::TPtr cypressManager,
        ICypressNodeProxy::TPtr proxy,
        yvector<Stroka>& names);

    virtual bool GetAttribute(
        TCypressManager::TPtr cypressManager,
        ICypressNodeProxy::TPtr proxy,
        const Stroka& name,
        IYsonConsumer* consumer);

protected:
    struct TGetRequest
    {
        TCypressManager::TPtr CypressManager;
        ICypressNodeProxy::TPtr Proxy;
        IYsonConsumer* Consumer;
    };

    typedef IParamAction<const TGetRequest&> TGetter;

    yhash_map<Stroka, TGetter::TPtr> Getters;

    void RegisterGetter(const Stroka& name, TGetter::TPtr getter);
};

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeAttributeProvider
    : public TAttributeProviderBase
{
public:
    static IAttributeProvider* Get();

    TCypressNodeAttributeProvider();

private:
    typedef TCypressNodeAttributeProvider TThis;

    static void GetId(const TGetRequest& request);
    static void GetType(const TGetRequest& request);
    static Stroka FormatType(ENodeType type);
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeAttributeProvider
    : public TCypressNodeAttributeProvider
{
public:
    static IAttributeProvider* Get();

    TCompositeNodeAttributeProvider();

private:
    typedef TCompositeNodeAttributeProvider TThis;

    static void GetSize(const TGetRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public INodeFactory
{
public:
    TNodeFactory(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId)
        : CypressManager(cypressManager)
        , TransactionId(transactionId)
    {
        YASSERT(~cypressManager != NULL);
    }

    virtual IStringNode::TPtr CreateString()
    {
        return CypressManager->CreateStringNodeProxy(TransactionId);
    }

    virtual IInt64Node::TPtr CreateInt64()
    {
        return CypressManager->CreateInt64NodeProxy(TransactionId);
    }

    virtual IDoubleNode::TPtr CreateDouble()
    {
        return CypressManager->CreateDoubleNodeProxy(TransactionId);
    }

    virtual IMapNode::TPtr CreateMap()
    {
        return CypressManager->CreateMapNodeProxy(TransactionId);
    }

    virtual IListNode::TPtr CreateList()
    {
        return CypressManager->CreateListNodeProxy(TransactionId);
    }

    virtual IEntityNode::TPtr CreateEntity()
    {
        YUNIMPLEMENTED();
    }

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
        auto proxy = ToProxy(INode::TPtr(~attributes));
        GetImplForUpdate().SetAttributesId(~proxy == NULL ? NullNodeId : proxy->GetNodeId());
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
        // First try to fetch a branched copy.
        auto* impl = CypressManager->FindNode(TBranchedNodeId(nodeId, TransactionId));
        if (impl != NULL) {
            return *impl;
        }

        // If failed, then try the unbranched one.
        impl = CypressManager->FindNode(TBranchedNodeId(nodeId, NullTransactionId));
        if (impl != NULL) {
            return *impl;
        }

        YASSERT(false);
        return *impl;
    }

    ICypressNode& GetImplForUpdate(const TNodeId& nodeId) const
    {
        // First fetch an unbranched copy and check if it is uncommitted.
        auto* nonbranchedImpl = CypressManager->FindNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        if (nonbranchedImpl != NULL && nonbranchedImpl->GetState() == ENodeState::Uncommitted) {
            return *nonbranchedImpl;
        }

        // Then try to fetch a branched copy.
        auto* branchedImpl = CypressManager->FindNodeForUpdate(TBranchedNodeId(nodeId, TransactionId));
        if (branchedImpl != NULL) {
            return *branchedImpl;
        }

        // Now a non-branched copy must exist and be committed.
        YASSERT(nonbranchedImpl != NULL);
        YASSERT(nonbranchedImpl->GetState() == ENodeState::Committed);

        // Branch it!
        return CypressManager->BranchNode(*nonbranchedImpl, TransactionId);
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
        if (nodeId == NullNodeId)
            return NULL;

        auto node = CypressManager->FindNode(nodeId, TransactionId);
        YASSERT(~node != NULL);
        return dynamic_cast<T*>(~node);
    }

    static typename ICypressNodeProxy::TPtr ToProxy(INode::TPtr node)
    {
        return dynamic_cast<ICypressNodeProxy*>(~node);
    }


    void EnsureInTransaction()
    {
        if (TransactionId == NullTransactionId) {
            throw TYTreeException() << "Cannot modify the node outside of a transaction";
        }
    }

    void EnsureModifiable()
    {
        EnsureInTransaction();
        if (IsLockNeeded()) {
            LockSelf();
        }
    }

    bool IsLockNeeded() const
    {
        // A shortcut.
        if (Locked) {
            return false;
        }

        // Check if this node is created by the current transaction and is still uncommitted.
        const auto* impl = CypressManager->FindNode(TBranchedNodeId(NodeId, NullTransactionId));
        if (impl != NULL && impl->GetState() == ENodeState::Uncommitted) {
            return false;
        }

        // Walk up to the root.
        auto currentNodeId = NodeId;
        while (currentNodeId != NullNodeId) {
            const auto& currentImpl = CypressManager->GetNode(TBranchedNodeId(currentNodeId, NullTransactionId));
            // Check the locks assigned to the current node.
            FOREACH (const auto& lockId, currentImpl.LockIds()) {
                const auto& lock = CypressManager->GetLock(lockId);
                if (lock.GetTransactionId() == TransactionId) {
                    return false;
                }
            }
            currentNodeId = currentImpl.GetParentId();
        }

        return true;
    }

    TLockResult LockSelf()
    {
        auto& impl = CypressManager->GetNodeForUpdate(TBranchedNodeId(NodeId, NullTransactionId));

        // Make sure that the node is committed.
        if (impl.GetState() != ENodeState::Committed) {
            throw TYTreeException() << "Cannot lock an uncommitted node";
        }

        // Make sure that the node is not locked by another transaction.
        FOREACH (const auto& lockId, impl.LockIds()) {
            const auto& lock = CypressManager->GetLock(lockId);
            if (lock.GetTransactionId() != TransactionId) {
                throw TYTreeException() << Sprintf("Node is already locked by another transaction (TransactionId: %s)",
                    ~lock.GetTransactionId().ToString());
            }
        }

        // Create a lock and register it within the transaction.
        auto* lock = CypressManager->CreateLock(NodeId, TransactionId);

        // Walk up to the root and apply locks.
        auto currentNodeId = NodeId;
        while (currentNodeId != NullNodeId) {
            auto& impl = CypressManager->GetNodeForUpdate(TBranchedNodeId(currentNodeId, NullTransactionId));
            impl.LockIds().insert(lock->GetId());
            currentNodeId = impl.GetParentId();
        }

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
        if (child.GetState() == ENodeState::Uncommitted) {
            CypressManager->UnrefNode(child);
        }
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
        this->EnsureModifiable();
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
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const \
    { \
        return const_cast<T ## name ## NodeProxy*>(this); \
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
        return ~New<T ## name ## NodeProxy>(cypressManager, transactionId, Id.NodeId); \
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
    virtual TIntrusiveConstPtr<ICompositeNode> AsComposite() const
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
