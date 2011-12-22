#include "stdafx.h"
#include "node.h"

#include <util/string/vector.h>

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

TBranchedNodeId TBranchedNodeId::FromString(const Stroka& str)
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

