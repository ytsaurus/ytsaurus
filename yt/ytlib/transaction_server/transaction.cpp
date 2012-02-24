#include "stdafx.h"
#include "transaction.h"

#include <util/ysaveload.h>

namespace NYT {

using namespace NCellMaster;

namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TObjectWithIdBase(id)
{ }

void TTransaction::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, State_);
    SaveSet(output, NestedTransactionIds_);
    ::Save(output, ParentId_);
    SaveSet(output, CreatedObjectIds_);
    ::Save(output, LockIds_);
    ::Save(output, BranchedNodeIds_);
    ::Save(output, CreatedNodeIds_);
}

void TTransaction::Load(TInputStream* input, const TLoadContext& context)
{
    TObjectWithIdBase::Load(input);
    ::Load(input, State_);
    LoadSet(input, NestedTransactionIds_);
    ::Load(input, ParentId_);
    LoadSet(input, CreatedObjectIds_);
    ::Load(input, LockIds_);
    ::Load(input, BranchedNodeIds_);
    ::Load(input, CreatedNodeIds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

