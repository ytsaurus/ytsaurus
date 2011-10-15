#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../ytree/node_detail.h"
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

    //! Initializes a instance by given node and transaction ids.
    TBranchedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId);

    //! Checks that the id is branched, i.e. #TransactionId it not #NullTransactionId.
    bool IsBranched() const;

    //! Formats the id to string (for debugging and logging purposes mainly).
    Stroka ToString() const;
};

bool operator==(const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);
inline bool operator!=(const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);

} // namespace NCypress
} // namespace NYT

//! A hasher for TBranchedNodeId.
template <>
struct hash<NYT::NCypress::TBranchedNodeId>
{
    i32 operator()(const NYT::NCypress::TBranchedNodeId& id) const;
};

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy;
class TCypressManager;

DECLARE_ENUM(ENodeState,
    (Committed)
    (Branched)
    (Uncommitted)
);

struct ICypressNode
{
    virtual ~ICypressNode()
    { }

    virtual TBranchedNodeId GetId() const = 0;

    virtual ENodeState GetState() const = 0;
    virtual void SetState(const ENodeState& value) = 0;

    virtual TNodeId GetParentId() const = 0;
    virtual void SetParentId(const TNodeId& value) = 0;

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
    TCypressNodeBase(const TBranchedNodeId& id);

    virtual TBranchedNodeId GetId() const;

protected:
    TBranchedNodeId Id;

};

//////////////////////////////////////////////////////////////////////////////// 

// TODO: move impl to inl
template <class TValue>
class TScalarNode
    : public TCypressNodeBase
{
    DECLARE_BYREF_RW_PROPERTY(Value, TValue)

private:
    typedef TScalarNode<TValue> TThis;

    TScalarNode(const TBranchedNodeId& id, const TThis& other)
        : TCypressNodeBase(id)
        , Value_(other.Value_)
    { }

public:
    TScalarNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
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

class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TNodeId> TNameToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToName;

    DECLARE_BYREF_RW_PROPERTY(NameToChild, TNameToChild);
    DECLARE_BYREF_RW_PROPERTY(ChildToName, TChildToName);

private:
    typedef TMapNode TThis;

    TMapNode(const TBranchedNodeId& id, const TMapNode& other);

public:
    TMapNode(const TBranchedNodeId& id);

    virtual TAutoPtr<ICypressNode> Branch(const TTransactionId& transactionId) const;
    virtual void Merge(ICypressNode& branchedNode);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const;
};

//////////////////////////////////////////////////////////////////////////////// 

class TListNode
    : public TCypressNodeBase
{
    typedef yvector<TNodeId> TIndexToChild;
    typedef yhash_map<TNodeId, int> TChildToIndex;

    DECLARE_BYREF_RW_PROPERTY(IndexToChild, TIndexToChild);
    DECLARE_BYREF_RW_PROPERTY(ChildToIndex, TChildToIndex);

private:
    typedef TListNode TThis;

    TListNode(const TBranchedNodeId& id, const TListNode& other);

public:
    TListNode(const TBranchedNodeId& id);

    virtual TAutoPtr<ICypressNode> Branch(const TTransactionId& transactionId) const;
    virtual void Merge(ICypressNode& branchedNode);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const;
};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypress
} // namespace NYT
