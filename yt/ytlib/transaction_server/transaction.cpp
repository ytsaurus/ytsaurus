#include "stdafx.h"
#include "transaction.h"

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/load_context.h>

#include <util/ysaveload.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TObjectWithIdBase(id)
    , Parent_(NULL)
{ }

void TTransaction::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, State_);
    SaveObjectRefs(output, NestedTransactions_);
    SaveObjectRef(output, Parent_);
    ::Save(output, StartTime_);
    SaveSet(output, CreatedObjectIds_);
    SaveObjectRefs(output, LockedNodes_);
    SaveObjectRefs(output, BranchedNodes_);
    SaveObjectRefs(output, CreatedNodes_);
}

void TTransaction::Load(const TLoadContext& context, TInputStream* input)
{
    TObjectWithIdBase::Load(input);
    ::Load(input, State_);
    LoadObjectRefs(input, NestedTransactions_, context);
    LoadObjectRef(input, Parent_, context);
    ::Load(input, StartTime_);
    LoadSet(input, CreatedObjectIds_);
    LoadObjectRefs(input, LockedNodes_, context);
    LoadObjectRefs(input, BranchedNodes_, context);
    LoadObjectRefs(input, CreatedNodes_, context);
}

bool TTransaction::IsActive() const
{
    return State_ == ETransactionState::Active;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

