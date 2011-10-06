#pragma once

#include "common.h"
#include "cypress_state.h"
#include "../ytree/ytree.h"
#include "../ytree/ypath.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<ICypressNodeProxy> TPtr;
    typedef TIntrusiveConstPtr<ICypressNodeProxy> TConstPtr;

    virtual TTransactionId GetTransactionId() const = 0;
    virtual TNodeId GetNodeId() const = 0;

    virtual TNodeId GetParentId() const = 0;
    virtual void SetParentId(const TNodeId& parentId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCypressNodeProxyBase
    : public TNodeBase
    , public virtual ICypressNodeProxy
    , public virtual IBase
{
public:
    typedef TIntrusivePtr<TCypressNodeProxyBase> TPtr;

    TCypressNodeProxyBase(
        TCypressState::TPtr state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        bool isMutable)
        : State(state)
        , TransactionId(transactionId)
        , NodeId(nodeId)
        , IsMutable(isMutable)
    {
        YASSERT(~state != NULL);

        Impl = FindImpl(TBranchedNodeId(nodeId, transactionId), isMutable);
        if (~Impl == NULL) {
            Impl = FindImpl(TBranchedNodeId(nodeId, NullTransactionId), isMutable);
        }
        YASSERT(~Impl != NULL);
    }

    virtual TTransactionId GetTransactionId() const
    {
        return TransactionId;
    }

    virtual TNodeId GetNodeId() const
    {
        return NodeId;
    }

    virtual ICompositeNode::TConstPtr GetParent() const
    {
        return GetProxy<ICompositeNode>(Impl->ParentId());
    }

    virtual void SetParent(ICompositeNode::TPtr parent)
    {
        auto parentProxy = ToProxy(INode::TPtr(~parent));
        Impl->ParentId() = parentProxy->GetNodeId();
    }

    virtual TNodeId GetParentId() const
    {
        return Impl->ParentId();
    }

    void SetParentId(const TNodeId& parentId)
    {
        Impl->ParentId() = parentId;
    }

    virtual IMapNode::TConstPtr GetAttributes() const
    {
        YASSERT(false);
        return NULL;
    }

    virtual void SetAttributes(IMapNode::TPtr attributes)
    {
        UNUSED(attributes);
        YASSERT(false);
    }

protected:
    TCypressState::TPtr State;
    TTransactionId TransactionId;
    TNodeId NodeId;
    bool IsMutable;
    TIntrusivePtr<TImpl> Impl;

    TIntrusivePtr<TImpl> FindImpl(const TBranchedNodeId& id, bool isMutable)
    {
        if (isMutable) {
            return dynamic_cast<TImpl*>(State->FindNodeForUpdate(id));
        } else {
            return dynamic_cast<TImpl*>(const_cast<TCypressNodeBase*>(State->FindNode(id)));
        }
    }

    template <class T>
    TIntrusiveConstPtr<T> GetProxy(const TNodeId& nodeId) const
    {
        return dynamic_cast<T*>(const_cast<INode*>(~State->GetNode(TransactionId, nodeId)));
    }

    static typename ICypressNodeProxy::TPtr ToProxy(INode::TPtr node)
    {
        return dynamic_cast<ICypressNodeProxy*>(~node);
    }

    static typename ICypressNodeProxy::TConstPtr ToProxy(INode::TConstPtr node)
    {
        return dynamic_cast<ICypressNodeProxy*>(const_cast<INode*>(~node));
    }
};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    TScalarNodeProxy(
        TCypressState::TPtr state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        bool isMutable)
        : TCypressNodeProxyBase<IBase, TImpl>(
            state,
            transactionId,
            nodeId,
            isMutable)
    { }

    virtual TValue GetValue() const
    {
        return Impl->Value();
    }

    virtual void SetValue(const TValue& value)
    {
        Impl->Value() = value;
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
    virtual I ## name ## Node::TConstPtr As ## name() const \
    { \
        return const_cast<T ## name ## NodeProxy*>(this); \
    } \
    \
    virtual I ## name ## Node::TPtr As ## name() \
    { \
        return this; \
    } \
    \
    virtual INodeFactory* GetFactory() const \
    { \
        YASSERT(false); \
        return NULL; \
    } \
private: \
    virtual TNodeBase::TPtr AsMutableImpl() const \
    { \
        return ~New<T ## name ## NodeProxy>(State, TransactionId, NodeId, true); \
    } \
    \
    virtual TNodeBase::TConstPtr AsImmutableImpl() const \
    { \
        return ~New<T ## name ## NodeProxy>(State, TransactionId, NodeId, false); \
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
            TCypressState::TPtr state, \
            const TTransactionId& transactionId, \
            const TNodeId& nodeId, \
            bool isMutable) \
            : TScalarNodeProxy<type, I ## name ## Node, T ## name ## Node>( \
                state, \
                transactionId, \
                nodeId, \
                isMutable) \
        { } \
    }


DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64, i64);
DECLARE_SCALAR_TYPE(Double, double);

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCompositeNodeProxyBase
    : public TCypressNodeProxyBase<IBase, TImpl>
{
protected:
    TCompositeNodeProxyBase(
        TCypressState::TPtr state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        bool isMutable)
        : TCypressNodeProxyBase<IBase, TImpl>(
            state,
            transactionId,
            nodeId,
            isMutable)
    { }

public:
    virtual ICompositeNode::TPtr AsComposite()
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
        TCypressState::TPtr state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        bool isMutable);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, INode::TConstPtr> > GetChildren() const;
    virtual INode::TConstPtr FindChild(const Stroka& name) const;
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
