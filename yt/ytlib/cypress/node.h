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

#define DECLARE_PROPERTY(name, type) \
private: \
    type name ## _; \
public: \
    type& name() \
    { \
        return name ## _; \
    } \
    \
    const type& name() const \
    { \
        return name ## _; \
    }

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeBase
    : public TRefCountedBase
{
    DECLARE_PROPERTY(Locks, yhash_set<TLockId>)
    DECLARE_PROPERTY(ParentId, TNodeId)

public:
    typedef TIntrusivePtr<TCypressNodeBase> TPtr;

    TCypressNodeBase(const TBranchedNodeId& id)
        : Id(id)
    { }

    const TBranchedNodeId& GetId()
    {
        return Id;
    }

private:
    TBranchedNodeId Id;

};

//////////////////////////////////////////////////////////////////////////////// 

template<class TValue>
class TScalarNode
    : public TCypressNodeBase
{
    DECLARE_PROPERTY(Value, TValue)

public:
    TScalarNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

};

typedef TScalarNode<Stroka>  TStringNode;
typedef TScalarNode<i64>     TInt64Node;
typedef TScalarNode<double>  TDoubleNode;

//////////////////////////////////////////////////////////////////////////////// 

class TMapNode
    : public TCypressNodeBase
{
    typedef yhash_map<Stroka, TNodeId> TNameToChild;
    typedef yhash_map<TNodeId, Stroka> TChildToName;

    DECLARE_PROPERTY(NameToChild, TNameToChild)
    DECLARE_PROPERTY(ChildToName, TChildToName)

public:
    TMapNode(const TBranchedNodeId& id)
        : TCypressNodeBase(id)
    { }

};

//////////////////////////////////////////////////////////////////////////////// 

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
