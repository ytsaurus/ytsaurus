#include "stdafx.h"
#include "transaction.h"

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialization_context.h>

#include <server/security_server/account.h>

#include <util/ysaveload.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TUnversionedObjectBase(id)
    , UncommittedAccountingEnabled_(false)
    , StagedAccountingEnabled_(false)
    , Parent_(nullptr)
    , StartTime_(TInstant::Zero())
{ }

void TTransaction::Save(const NCellMaster::TSaveContext& context) const
{
    TUnversionedObjectBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, State_);
    ::Save(output, Timeout_);
    ::Save(output, UncommittedAccountingEnabled_);
    ::Save(output, StagedAccountingEnabled_);
    SaveObjectRefs(output, NestedTransactions_);
    SaveObjectRef(output, Parent_);
    ::Save(output, StartTime_);
    SaveObjectRefs(output, StagedObjects_);
    SaveObjectRefs(output, LockedNodes_);
    SaveObjectRefs(output, BranchedNodes_);
    SaveObjectRefs(output, StagedNodes_);
    SaveObjectRefs(output, AccountResourceUsage_);
}

void TTransaction::Load(const NCellMaster::TLoadContext& context)
{
    TUnversionedObjectBase::Load(context);

    auto* input = context.GetInput();
    ::Load(input, State_);
    ::Load(input, Timeout_);
    ::Load(input, UncommittedAccountingEnabled_);
    ::Load(input, StagedAccountingEnabled_);
    LoadObjectRefs(input, NestedTransactions_, context);
    LoadObjectRef(input, Parent_, context);
    ::Load(input, StartTime_);
    LoadObjectRefs(input, StagedObjects_, context);
    LoadObjectRefs(input, LockedNodes_, context);
    LoadObjectRefs(input, BranchedNodes_, context);
    LoadObjectRefs(input, StagedNodes_, context);
    LoadObjectRefs(input, AccountResourceUsage_, context);
}

bool TTransaction::IsActive() const
{
    return State_ == ETransactionState::Active;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

