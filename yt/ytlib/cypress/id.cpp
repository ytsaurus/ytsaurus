#include "stdafx.h"
#include "id.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TNodeId NullNodeId = NObjectServer::NullObjectId;

////////////////////////////////////////////////////////////////////////////////

TVersionedNodeId::TVersionedNodeId()
{ }

TVersionedNodeId::TVersionedNodeId(const TNodeId& nodeId)
    : NodeId(nodeId)
{ }

TVersionedNodeId::TVersionedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId)
    : NodeId(nodeId)
    , TransactionId(transactionId)
{ }

bool TVersionedNodeId::IsBranched() const
{
    return TransactionId != NullTransactionId;
}

Stroka TVersionedNodeId::ToString() const
{
    return Sprintf("%s:%s",
        ~NodeId.ToString(),
        ~TransactionId.ToString());
}

TVersionedNodeId TVersionedNodeId::FromString(const Stroka& str)
{
    auto tokens = splitStroku(str, ":");
    if (tokens.size() < 1 || 2 < tokens.size()) {
        ythrow yexception() << Sprintf("Invalid number of tokens in %s", ~str.Quote());
    }

    auto nodeId = TNodeId::FromString(tokens[0]);
    auto transactionId =
        tokens.size() == 2
        ? TTransactionId::FromString(tokens[1])
        : NullTransactionId;
    return TVersionedNodeId(nodeId, transactionId);
}

bool operator == (const TVersionedNodeId& lhs, const TVersionedNodeId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedNodeId)) == 0;
}

bool operator != (const TVersionedNodeId& lhs, const TVersionedNodeId& rhs)
{
    return !(lhs == rhs);
}

bool operator < (const TVersionedNodeId& lhs, const TVersionedNodeId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedNodeId)) < 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

