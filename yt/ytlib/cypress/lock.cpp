#include "stdafx.h"
#include "lock.h"

#include <ytlib/transaction_server/transaction.h>
#include <ytlib/cell_master/load_context.h>

namespace NYT {

using namespace NCellMaster;
using namespace NTransactionServer;

namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(
    const TLockId& id,
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode mode)
    : TObjectWithIdBase(id)
    , NodeId_(nodeId)
    , Transaction_(transaction)
    , Mode_(mode)
{ }

TLock::TLock(const TLockId& id)
    : TObjectWithIdBase(id)
{ }

void TLock::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, NodeId_);
    SaveObjectRef(output, Transaction_);
    ::Save(output, Mode_);
}

void TLock::Load(const TLoadContext& context, TInputStream* input)
{
    TObjectWithIdBase::Load(input);
    ::Load(input, NodeId_);
    LoadObjectRef(input, Transaction_, context);
    ::Load(input, Mode_);
}

void TLock::PromoteToTransaction(TTransaction* transaction)
{
    YASSERT(transaction);
    Transaction_ = transaction;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

