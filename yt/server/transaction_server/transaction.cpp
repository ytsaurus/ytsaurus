#include "stdafx.h"
#include "transaction.h"

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialization_context.h>

#include <server/security_server/account.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TNonversionedObjectBase(id)
    , UncommittedAccountingEnabled_(true)
    , StagedAccountingEnabled_(true)
    , Parent_(nullptr)
    , StartTime_(TInstant::Zero())
    , Acd_(this)
{ }

void TTransaction::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, State_ == ETransactionState::TransientlyPrepared ? ETransactionState(ETransactionState::Active) : State_);
    Save(context, Timeout_);
    Save(context, UncommittedAccountingEnabled_);
    Save(context, StagedAccountingEnabled_);
    SaveObjectRefs(context, NestedTransactions_);
    SaveObjectRef(context, Parent_);
    Save(context, StartTime_);
    SaveObjectRefs(context, StagedObjects_);
    SaveObjectRefs(context, LockedNodes_);
    SaveObjectRefs(context, Locks_);
    SaveObjectRefs(context, BranchedNodes_);
    SaveObjectRefs(context, StagedNodes_);
    SaveObjectRefs(context, AccountResourceUsage_);
    Save(context, Acd_);
}

void TTransaction::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, State_);
    Load(context, Timeout_);
    Load(context, UncommittedAccountingEnabled_);
    Load(context, StagedAccountingEnabled_);
    LoadObjectRefs(context, NestedTransactions_);
    LoadObjectRef(context, Parent_);
    Load(context, StartTime_);
    LoadObjectRefs(context, StagedObjects_);
    LoadObjectRefs(context, LockedNodes_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 24) {
        LoadObjectRefs(context, Locks_);
    }
    LoadObjectRefs(context, BranchedNodes_);
    LoadObjectRefs(context, StagedNodes_);
    LoadObjectRefs(context, AccountResourceUsage_);
    Load(context, Acd_);
}

bool TTransaction::IsActive() const
{
    return State_ == ETransactionState::Active;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

