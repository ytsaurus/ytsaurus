#include "stdafx.h"
#include "lock.h"

namespace NYT {

using namespace NCellMaster;

namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(
    const TLockId& id,
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    ELockMode mode)
    : TObjectWithIdBase(id)
    , NodeId_(nodeId)
    , TransactionId_(transactionId)
    , Mode_(mode)
{ }


TLock::TLock(const TLockId& id)
    : TObjectWithIdBase(id)
{ }

void TLock::Save(TOutputStream* output) const
{
    ::Save(output, NodeId_);
    ::Save(output, TransactionId_);
    ::Save(output, Mode_);
}

void TLock::Load(TInputStream* input, const TLoadContext& context)
{
    ::Load(input, NodeId_);
    ::Load(input, TransactionId_);
    ::Load(input, Mode_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

