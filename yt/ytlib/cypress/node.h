#pragma once

#include "common.h"

#include "../ytree/node.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node possibly branched by a transaction.
struct TBranchedNodeId
{
    //! Id of the node itself.
    TNodeId NodeId;

    //! Id of the transaction that had branched the node.
    TTransactionId TransactionId;

    TBranchedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId)
        : NodeId(nodeId)
        , TransactionId(transactionId)
    { }

    bool IsBranched() const
    {
        return TransactionId != TTransactionId();
    }

    //! Formats the id into the string (for debugging and logging purposes mainly).
    Stroka ToString() const
    {
        return Sprintf("%s:%s",
            ~NodeId.ToString(),
            ~TransactionId.ToString());
    }
};

//! Compares TBranchedNodeId s for equality.
inline bool operator==(const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return lhs.NodeId == rhs.NodeId &&
           lhs.TransactionId == rhs.TransactionId;
}

//! Compares TBranchedNodeId s for inequality.
inline bool operator!=(const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeImplBase
    : public TRefCountedBase
{
public:
    typedef yhash_set<TLockId> TLocks;

    TCypressNodeImplBase(const TBranchedNodeId& id)
        : Id(id)
    { }

    const TBranchedNodeId& GetId()
    {
        return Id;
    }

    TLocks& Locks()
    {
        return Locks_;
    }


private:
    TBranchedNodeId Id;
    TLocks Locks_;

};

//////////////////////////////////////////////////////////////////////////////// 

template<class TValue>
class TScalarNodeImpl
    : public TCypressNodeImplBase
{
public:
    TScalarNodeImpl(const TBranchedNodeId& id)
        : TCypressNodeImplBase(id)
    { }

    TValue& Value()
    {
        return Value_;
    }

private:
    TValue Value_;

};

typedef TScalarNodeImpl<Stroka>  TStringNodeImpl;
typedef TScalarNodeImpl<i64>     TInt64NodeImpl;
typedef TScalarNodeImpl<double>  TDoubleNodeImpl;

//////////////////////////////////////////////////////////////////////////////// 

class TCypressState;

template <class IBase, class TImpl>
class TCypressNodeBase
    : public NYTree::TNodeBase
    , public virtual IBase
{
public:
    TCypressNodeBase(
        TIntrusivePtr<TCypressState> state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : State(state)
        , TransactionId(transactionId)
        , NodeId(nodeId)
    {
        YASSERT(~state != NULL);

        Impl = state->FindNode(TBranchedNodeId(nodeId, transactionId));
        if (Impl == NULL) {
            Impl = state->FindNode(TBranchedNodeId(nodeId, TTransactionId()));
        }
        YASSERT(Impl != NULL);
    }

private:
    TIntrusivePtr<TCypressState> State;
    TTransactionId TransactionId;
    TNodeId NodeId;
    TIntrusiveConstPtr<TImpl> Impl;

    // TNodeBase overrides.
    virtual NYTree::TNodeBase* AsMutableImpl() const
    {
        // TODO:
        return NYTree::TNodeBase::AsMutableImpl();
    }

    virtual const NYTree::TNodeBase* AsImmutableImpl() const
    {
        // TODO:
        return NYTree::TNodeBase::AsImmutableImpl();
    }
};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue, class IBase, class TImpl>
class TScalarNode
    : public TCypressNodeBase<IBase, TImpl>
{
public:
    TScalarNode(
        TIntrusivePtr<TCypressState> state,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
    :   TCypressNodeBase<IBase, TImpl>(
            state,
            transactionId,
            nodeId)
    { }

    virtual TValue GetValue() const
    {
        return TValue();
        // TODO:
        //return Impl->Value();
    }

    virtual void SetValue(const TValue& value)
    {
        // TODO:
    }
};

//////////////////////////////////////////////////////////////////////////////// 

#define DECLARE_TYPE_OVERRIDES(name) \
    virtual NYTree::ENodeType GetType() const \
    { \
        return NYTree::ENodeType::name; \
    } \
    \
    virtual NYTree::I ## name ## Node::TConstPtr As ## name() const \
    { \
        return const_cast<T ## name ## Node*>(this); \
    } \
    \
    virtual NYTree::I ## name ## Node::TPtr As ## name() \
    { \
        return this; \
    }

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, NYTree::I ## name ## Node, T ## name ## NodeImpl> \
    { \
    public: \
        DECLARE_TYPE_OVERRIDES(name) \
    \
    }

////////////////////////////////////////////////////////////////////////////////

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64, i64);
DECLARE_SCALAR_TYPE(Double, double);

////////////////////////////////////////////////////////////////////////////////

#undef DECLARE_SCALAR_TYPE
#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

//! A hasher for TBranchedNodeId.
template<>
struct hash<NYT::NCypress::TBranchedNodeId>
{
    i32 operator()(const NYT::NCypress::TBranchedNodeId& id) const
    {
        return (i32) THash<NYT::TGuid>()(id.NodeId) * 497 +
               (i32) THash<NYT::TGuid>()(id.TransactionId);
    }
};
