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
    ::Save(output, NodeId_);
    SaveObject(output, Transaction_);
    ::Save(output, Mode_);
}

void TLock::Load(const TLoadContext& context, TInputStream* input)
{
    ::Load(input, NodeId_);
    LoadObject(input, Transaction_, context);
    ::Load(input, Mode_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

