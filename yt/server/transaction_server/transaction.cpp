#include "stdafx.h"
#include "transaction.h"

#include <core/misc/string.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialize.h>

#include <server/security_server/account.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TTransaction::TExportEntry::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Object);
    Persist(context, DestinationCellTag);
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TNonversionedObjectBase(id)
    , AccountingEnabled_(true)
    , Parent_(nullptr)
    , StartTime_(TInstant::Zero())
    , Acd_(this)
{ }

void TTransaction::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, GetPersistentState());
    Save(context, Timeout_);
    Save(context, AccountingEnabled_);
    Save(context, NestedTransactions_);
    Save(context, Parent_);
    Save(context, StartTime_);
    Save(context, StagedObjects_);
    Save(context, ExportedObjects_);
    Save(context, ImportedObjects_);
    Save(context, LockedNodes_);
    Save(context, Locks_);
    Save(context, BranchedNodes_);
    Save(context, StagedNodes_);
    Save(context, AccountResourceUsage_);
    Save(context, Acd_);
}

void TTransaction::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    // COMPAT(babenko)
    YCHECK(context.GetVersion() >= 200);

    using NYT::Load;
    Load(context, State_);
    Load(context, Timeout_);
    Load(context, AccountingEnabled_);
    Load(context, NestedTransactions_);
    Load(context, Parent_);
    Load(context, StartTime_);
    Load(context, StagedObjects_);
    Load(context, ImportedObjects_);
    Load(context, ExportedObjects_);
    Load(context, LockedNodes_);
    Load(context, Locks_);
    Load(context, BranchedNodes_);
    Load(context, StagedNodes_);
    Load(context, AccountResourceUsage_);
    Load(context, Acd_);
}

ETransactionState TTransaction::GetPersistentState() const
{
    switch (State_) {
        case ETransactionState::TransientCommitPrepared:
        case ETransactionState::TransientAbortPrepared:
            return ETransactionState::Active;
        default:
            return State_;
    }
}

void TTransaction::ThrowInvalidState() const
{
    THROW_ERROR_EXCEPTION("Transaction %v is in %Qlv state",
        Id_,
        State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

