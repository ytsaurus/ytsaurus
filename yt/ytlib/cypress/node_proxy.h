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

    virtual bool IsLocked() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public INodeFactory
{
public:
    TNodeFactory(
        TCypressManager::TPtr state,
        const TTransactionId& transactionId)
        : State(state)
        , TransactionId(transactionId)
    {
        YASSERT(~state != NULL);
    }

    virtual IStringNode::TPtr CreateString()
    {
        return State->CreateStringNode(TransactionId);
    }

    virtual IInt64Node::TPtr CreateInt64()
    {
        return State->CreateInt64Node(TransactionId);
    }

    virtual IDoubleNode::TPtr CreateDouble()
    {
        return State->CreateDoubleNode(TransactionId);
    }

    virtual IMapNode::TPtr CreateMap()
    {
        return State->CreateMapNode(TransactionId);
    }

    virtual IListNode::TPtr CreateList()
    {
        YASSERT(false);
        return NULL;
    }

    virtual IEntityNode::TPtr CreateEntity()
    {
        YASSERT(false);
        return NULL;
    }

private:
    TCypressManager::TPtr State;
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
        return GetProxy<ICompositeNode>(GetImpl().ParentId());
    }

    virtual void SetParent(ICompositeNode::TPtr parent)
    {
        auto parentProxy = ToProxy(INode::TPtr(~parent));
        GetImplForUpdate().ParentId() = parentProxy->GetNodeId();
    }

    virtual IMapNode::TPtr GetAttributes() const
    {
        return NULL;
    }

    virtual void SetAttributes(IMapNode::TPtr attributes)
    {
        UNUSED(attributes);
        YASSERT(false);
    }

    virtual TLockResult Lock(TYPath path)
    {
        if (!path.empty()) {
            return Navigate(path);
        }

        return LockSelf();
    }

    virtual bool IsLocked() const
    {
        // Walk up to the root.
        auto currentNodeId = NodeId;
        while (currentNodeId != NullNodeId) {
            const auto& impl = CypressManager->GetNode(TBranchedNodeId(currentNodeId, NullTransactionId));
            // Check the locks assigned to the current node.
            FOREACH (const auto& lockId, impl.LockIds()) {
                const auto& lock = CypressManager->GetLock(lockId);
                if (lock.GetTransactionId() == TransactionId) {
                    return true;
                }
            }
            currentNodeId = impl.ParentId();
        }
        return false;
    }

protected:
    TCypressManager::TPtr CypressManager;
    TTransactionId TransactionId;
    TNodeId NodeId;
    mutable TNodeFactory NodeFactory;


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
        // First try to fetch a branched copy.
        auto* branchedImpl = CypressManager->FindNodeForUpdate(TBranchedNodeId(nodeId, TransactionId));
        if (branchedImpl != NULL) {
            return *branchedImpl;
        }

        // If failed, then try the unbranched one.
        auto* nonbranchedImpl = CypressManager->FindNode(TBranchedNodeId(nodeId, NullTransactionId));
        YASSERT(nonbranchedImpl != NULL);

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
        auto node = CypressManager->FindNode(nodeId, TransactionId);
        YASSERT(~node != NULL);
        return dynamic_cast<T*>(~node);
    }

    static typename ICypressNodeProxy::TPtr ToProxy(INode::TPtr node)
    {
        YASSERT(~node != NULL);
        return dynamic_cast<ICypressNodeProxy*>(~node);
    }


    TLockResult LockSelf()
    {
        auto& impl = CypressManager->GetNodeForUpdate(TBranchedNodeId(NodeId, NullTransactionId));

        // Make sure that the node is not locked by another transaction.
        FOREACH (const auto& lockId, impl.LockIds()) {
            const auto& lock = CypressManager->GetLock(lockId);
            if (lock.GetTransactionId() != TransactionId) {
                return TLockResult::CreateError(Sprintf("Node is already locked by another transaction (TransactionId: %s)",
                    ~lock.GetTransactionId().ToString()));
            }
        }

        // Create a lock and register it within the transaction.
        auto* lock = CypressManager->CreateLock(NodeId, TransactionId);

        // Walk up to the root and apply locks.
        auto currentNodeId = NodeId;
        while (currentNodeId != NullNodeId) {
            auto& impl = CypressManager->GetNodeForUpdate(TBranchedNodeId(currentNodeId, NullTransactionId));
            impl.LockIds().insert(lock->GetId());
            currentNodeId = impl.ParentId();
        }

        return TLockResult::CreateDone();
    }

    bool IsBranched() const
    {
        return CypressManager->FindNode(TBranchedNodeId(NullNodeId, TransactionId)) != NULL;
    }

    ICypressNodeProxy::TPtr Branch()
    {
        return NULL;
    }
};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    TScalarNodeProxy(
        TCypressManager::TPtr state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            state,
            transactionId,
            nodeId)
    { }

    virtual TValue GetValue() const
    {
        return this->GetTypedImpl().Value();
    }

    virtual void SetValue(const TValue& value)
    {
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
    virtual void DoSetSelf(TYsonProducer::TPtr producer) \
    { \
        SetNodeFromProducer(TIntrusivePtr<I##name##Node>(this), producer); \
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
        TCypressManager::TPtr state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            state,
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
{
    DECLARE_TYPE_OVERRIDES(Map)

public:
    TMapNodeProxy(
        TCypressManager::TPtr state,
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

    virtual TNavigateResult Navigate(TYPath path);
    virtual TSetResult Set(
        TYPath path,
        TYsonProducer::TPtr producer);
};

////////////////////////////////////////////////////////////////////////////////

#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
