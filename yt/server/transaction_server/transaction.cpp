#include "stdafx.h"
#include "transaction.h"

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/load_context.h>

#include <util/ysaveload.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TObjectWithIdBase(id)
    , Parent_(NULL)
    , StartTime_(TInstant::Max())
{ }

void TTransaction::Save(const NCellMaster::TSaveContext& context) const
{
    TObjectWithIdBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, State_);
    SaveObjectRefs(output, NestedTransactions_);
    SaveObjectRef(output, Parent_);
    ::Save(output, StartTime_);
    SaveSet(output, CreatedObjectIds_);
    SaveObjectRefs(output, LockedNodes_);
    SaveObjectRefs(output, BranchedNodes_);
    SaveObjectRefs(output, CreatedNodes_);
}

void TTransaction::Load(const NCellMaster::TLoadContext& context)
{
    TObjectWithIdBase::Load(context);

    auto* input = context.GetInput();
    ::Load(input, State_);
    LoadObjectRefs(input, NestedTransactions_, context);
    LoadObjectRef(input, Parent_, context);
    ::Load(input, StartTime_);
    ::Load(input, CreatedObjectIds_);
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

