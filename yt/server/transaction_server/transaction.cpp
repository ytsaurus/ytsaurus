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
    : TNonversionedObjectBase(id)
    , UncommittedAccountingEnabled_(false)
    , StagedAccountingEnabled_(false)
    , Parent_(nullptr)
    , StartTime_(TInstant::Zero())
{ }

void TTransaction::Save(const NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, State_);
    ::Save(output, Timeout_);
    ::Save(output, UncommittedAccountingEnabled_);
    ::Save(output, StagedAccountingEnabled_);
    SaveObjectRefs(context, NestedTransactions_);
    SaveObjectRef(context, Parent_);
    ::Save(output, StartTime_);
    SaveObjectRefs(context, StagedObjects_);
    SaveObjectRefs(context, LockedNodes_);
    SaveObjectRefs(context, BranchedNodes_);
    SaveObjectRefs(context, StagedNodes_);
    SaveObjectRefs(context, AccountResourceUsage_);
}

void TTransaction::Load(const NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    auto* input = context.GetInput();
    ::Load(input, State_);
    ::Load(input, Timeout_);
    ::Load(input, UncommittedAccountingEnabled_);
    ::Load(input, StagedAccountingEnabled_);
    LoadObjectRefs(context, NestedTransactions_);
    LoadObjectRef(context, Parent_);
    ::Load(input, StartTime_);
    LoadObjectRefs(context, StagedObjects_);
    LoadObjectRefs(context, LockedNodes_);
    LoadObjectRefs(context, BranchedNodes_);
    LoadObjectRefs(context, StagedNodes_);
    LoadObjectRefs(context, AccountResourceUsage_);
}

bool TTransaction::IsActive() const
{
    return State_ == ETransactionState::Active;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

