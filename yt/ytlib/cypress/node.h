#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../ytree/node.h"
#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransactionId;
using NTransaction::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node possibly branched by a transaction.
struct TBranchedNodeId
{
    //! Id of the node itself.
    TNodeId NodeId;

    //! Id of the transaction that had branched the node.
    TTransactionId TransactionId;

    TBranchedNodeId(
        const TNodeId& nodeId, const TTransactionId& transactionId)
        : NodeId(nodeId)
        , TransactionId(transactionId)
    { }

    bool IsBranched() const
    {
        return TransactionId != NullTransactionId;
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

struct ICypressNodeProxy;
class TCypressManager;

DECLARE_ENUM(ENodeState,
    (Committed)
    (Branched)
    (Uncommitted)
);

// TODO: type vs const type& in properties
struct ICypressNode
{
    virtual ~ICypressNode()
    { }

    virtual TBranchedNodeId GetId() const = 0;

    virtual ENodeState GetState() const = 0;
    virtual void SetState(ENodeState value) = 0;

    virtual TNodeId GetParentId() const = 0;
    virtual void SetParentId(TNodeId value) = 0;

    virtual const yhash_set<TLockId>& LockIds() const = 0;
    virtual yhash_set<TLockId>& LockIds() = 0;

    virtual TAutoPtr<ICypressNode> Clone() const = 0;

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const = 0;

    virtual TAutoPtr<ICypressNode> Branch(const TTransactionId& transactionId) const = 0;
    
    // #branchedNode is non-const for performance reasons (i.e. to swap the data instead of copying).
    virtual void Merge(ICypressNode& branchedNode) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeBase
    : public ICypressNode
{
    // This also overrides appropriate methods from ICypressNode.
    DECLARE_BYREF_RW_PROPERTY(LockIds, yhash_set<TLockId>);
    DECLARE_BYVAL_RW_PROPERTY(ParentId, TNodeId);
    DECLARE_BYVAL_RW_PROPERTY(State, ENodeState);

public:
    TCypressNodeBase(const TBranchedNodeId& id)
        : ParentId_(NullNodeId)
        , State_(ENodeState::Uncommitted)
        , Id(id)
    { }

    virtual TBranchedNodeId GetId() const
    {
        return Id;
    }

protected:
    TBranchedNodeId Id;

};

//////////////////////////////////////////////////////////////////////////////// 

template<class TValue>
class TScalarNode
    : public TCypressNodeBase
{
    DECLARE_BYREF_RW_PROPERTY(Value, TValue)

private:
    typedef TScalarNode<TValue> TThis;

public:
    TScalarNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

    TScalarNode(const TBranchedNodeId& id, const TThis& other)
        : TCypressNodeBase(id)
        , Value_(other.Value_)
    { }

    virtual TAutoPtr<ICypressNode> Branch(const TTransactionId& transactionId) const
    {
        YASSERT(!Id.IsBranched());
        return new TThis(
            TBranchedNodeId(Id.NodeId, transactionId),
            *this);
    }

    virtual void Merge(ICypressNode& branchedNode)
    {
        const auto& typedBranchedNode = dynamic_cast<const TThis&>(branchedNode);
        Value() = typedBranchedNode.Value();
    }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TThis(Id, *this);
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const;
};

typedef TScalarNode<Stroka> TStringNode;
typedef TScalarNode<i64>    TInt64Node;
typedef TScalarNode<double> TDoubleNode;

//////////////////////////////////////////////////////////////////////////////// 

// TODO: move impl to cpp
class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TNodeId> TNameToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToName;

    DECLARE_BYREF_RW_PROPERTY(NameToChild, TNameToChild);
    DECLARE_BYREF_RW_PROPERTY(ChildToName, TChildToName);

private:
    typedef TMapNode TThis;

public:
    TMapNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

    TMapNode(const TBranchedNodeId& id, const TMapNode& other)
        : TCypressNodeBase(id)
    {
        NameToChild() = other.NameToChild();
        ChildToName() = other.ChildToName();
    }

    virtual TAutoPtr<ICypressNode> Branch(const TTransactionId& transactionId) const
    {
        YASSERT(!Id.IsBranched());
        return new TThis(
            TBranchedNodeId(Id.NodeId, transactionId),
            *this);
    }

    virtual void Merge(ICypressNode& branchedNode)
    {
        auto& typedBranchedNode = dynamic_cast<TThis&>(branchedNode);
        NameToChild().swap(typedBranchedNode.NameToChild());
        ChildToName().swap(typedBranchedNode.ChildToName());
    }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TThis(Id, *this);
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const;
};

//////////////////////////////////////////////////////////////////////////////// 


} // namespace NCypress
} // namespace NYT

//! A hasher for TBranchedNodeId.
template<>
struct hash<NYT::NCypress::TBranchedNodeId>
{
    i32 operator()(const NYT::NCypress::TBranchedNodeId& id) const
    {
        return static_cast<i32>(THash<NYT::TGuid>()(id.NodeId)) * 497 +
               static_cast<i32>(THash<NYT::TGuid>()(id.TransactionId));
    }
};
