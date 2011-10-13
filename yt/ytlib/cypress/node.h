#pragma once

#include "common.h"

#include "../ytree/node.h"
#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransactionId;

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

struct ICypressNodeProxy;
class TCypressManager;

struct ICypressNode
{
    virtual ~ICypressNode()
    { }

    virtual const TBranchedNodeId& GetId() = 0;

    virtual const TNodeId& ParentId() const = 0;
    virtual TNodeId& ParentId() = 0;

    virtual const yhash_set<TLockId>& Locks() const = 0;
    virtual yhash_set<TLockId>& Locks() = 0;

    virtual TAutoPtr<ICypressNode> Clone() const = 0;

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const = 0;

    virtual TAutoPtr<ICypressNode> Branch(const TTransactionId& transactionId) const = 0;
    
    // #branchedNode is non-cost for performance reasons (i.e. to swap the data instead of copying).
    virtual void Merge(ICypressNode& branchedNode) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO: move
#define DECLARE_PROPERTY(name, type) \
private: \
    type name ## _; \
public: \
    FORCED_INLINE type& name() \
    { \
        return name ## _; \
    } \
    \
    FORCED_INLINE const type& name() const \
    { \
        return name ## _; \
    }

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeBase
    : public ICypressNode
{
    // This also overrides appropriate methods from ICypressNode.
    DECLARE_PROPERTY(Locks, yhash_set<TLockId>)  
    DECLARE_PROPERTY(ParentId, TNodeId)

public:
    TCypressNodeBase(const TBranchedNodeId& id)
        : ParentId_(NullNodeId)
        , Id(id)
    { }

    virtual const TBranchedNodeId& GetId()
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
    DECLARE_PROPERTY(Value, TValue)

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

    DECLARE_PROPERTY(NameToChild, TNameToChild)
    DECLARE_PROPERTY(ChildToName, TChildToName)

private:
    typedef TMapNode TThis;

public:
    TMapNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

    TMapNode(const TBranchedNodeId& id, const TThis& other)
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

// TODO: drop
#undef DECLARE_PROPERTY

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
