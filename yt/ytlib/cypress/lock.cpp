#include "stdafx.h"
#include "lock.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(
    const TLockId& id,
    const TNodeId& nodeId,
    const NTransaction::TTransactionId& transactionId,
    ELockMode mode)
    : Id_(id)
    , NodeId_(nodeId)
    , TransactionId_(transactionId)
    , Mode_(mode)
{ }

TAutoPtr<TLock> TLock::Clone() const
{
    return new TLock(*this);
}

void TLock::Save(TOutputStream* output) const
{
    ::Save(output, NodeId_);
    ::Save(output, TransactionId_);
    ::Save(output, Mode_);
}

TAutoPtr<TLock> TLock::Load(const TLockId& id, TInputStream* input)
{
    TNodeId nodeId;
    NTransaction::TTransactionId transactionId;
    ELockMode mode;
    ::Load(input, nodeId);
    ::Load(input, transactionId);
    ::Load(input, mode);
    return new TLock(
        id,
        nodeId,
        transactionId,
        mode);
}

TLock::TLock(const TLock& other)
    : Id_(other.Id_)
    , NodeId_(other.NodeId_)
    , TransactionId_(other.TransactionId_)
    , Mode_(other.Mode_)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

