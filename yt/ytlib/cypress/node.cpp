#include "stdafx.h"
#include "node.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TBranchedNodeId::TBranchedNodeId()
{ }

TBranchedNodeId::TBranchedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId)
    : NodeId(nodeId)
    , TransactionId(transactionId)
{ }

bool TBranchedNodeId::IsBranched() const
{
    return TransactionId != NullTransactionId;
}

Stroka TBranchedNodeId::ToString() const
{
    return Sprintf("%s:%s",
        ~NodeId.ToString(),
        ~TransactionId.ToString());
}

TBranchedNodeId TBranchedNodeId::FromString(const Stroka& s)
{
    auto tokens = splitStroku(s, ":");
    YASSERT(1 <= tokens.size() && tokens.size() <= 2);
    auto nodeId = TNodeId::FromString(tokens[0]);

    TTransactionId transactionId;
    if (tokens.size() == 2) {
        transactionId = TTransactionId::FromString(tokens[1]);
    } else {
        transactionId = NullTransactionId;
    }
    return TBranchedNodeId(nodeId, transactionId);
}

bool operator == (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TBranchedNodeId)) == 0;
}

bool operator != (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return !(lhs == rhs);
}

bool operator < (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TBranchedNodeId)) < 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

