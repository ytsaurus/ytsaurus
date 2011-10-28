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

