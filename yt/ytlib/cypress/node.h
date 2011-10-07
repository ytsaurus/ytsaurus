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

struct ICypressNodeProxy;
class TCypressState;

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
        TIntrusivePtr<TCypressState> state,
        const TTransactionId& transactionId) const = 0;
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
class TScalarNodeBase
    : public TCypressNodeBase
{
    DECLARE_PROPERTY(Value, TValue)

public:
    TScalarNodeBase(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }
};

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNodeBase<type> \
    { \
    public: \
        T ## name ## Node(const TBranchedNodeId& id) \
            : TScalarNodeBase<type>(id) \
        { } \
        \
        T ## name ## Node(const T ## name ## Node& other) \
            : TScalarNodeBase<type>(other.Id) \
        { \
            Value() = other.Value(); \
        } \
        \
        virtual TAutoPtr<ICypressNode> Clone() const \
        { \
            return new T ## name ## Node(*this); \
        } \
        \
        virtual TIntrusivePtr<ICypressNodeProxy> GetProxy( \
            TIntrusivePtr<TCypressState> state, \
            const TTransactionId& transactionId) const; \
    };

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

//////////////////////////////////////////////////////////////////////////////// 

class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TNodeId> TNameToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToName;

    DECLARE_PROPERTY(NameToChild, TNameToChild)
    DECLARE_PROPERTY(ChildToName, TChildToName)

public:
    // TODO: move to impl
    TMapNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

    // TODO: move to impl
    TMapNode(const TMapNode& other)
        : TCypressNodeBase(other.Id)
    {
        NameToChild() = other.NameToChild();
        ChildToName() = other.ChildToName();
    }

    // TODO: move to impl
    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TMapNode(*this);
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressState> state,
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
