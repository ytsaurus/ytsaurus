#include "transaction.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/account.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/string.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTransactionServer {

using namespace NYTree;
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
    Save(context, GetPersistentState());
    Save(context, Timeout_);
    Save(context, Title_);
    Save(context, UncommittedAccountingEnabled_);
    Save(context, StagedAccountingEnabled_);
    Save(context, NestedTransactions_);
    Save(context, Parent_);
    Save(context, StartTime_);
    Save(context, StagedObjects_);
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

    using NYT::Load;
    Load(context, State_);
    // COMPAT(babenko)
    if (context.GetVersion() < 107) {
        Timeout_ = Load<TDuration>(context);
    } else {
        Load(context, Timeout_);
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 123) {
        Load(context, Title_);
    }
    Load(context, UncommittedAccountingEnabled_);
    Load(context, StagedAccountingEnabled_);
    Load(context, NestedTransactions_);
    Load(context, Parent_);
    Load(context, StartTime_);
    Load(context, StagedObjects_);
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

TYsonString TTransaction::GetDescription() const
{
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("id").Value(Id_)
            .Item("start_time").Value(StartTime_)
            .DoIf(Title_.HasValue(), [&] (TFluentMap fluent) {
                fluent
                    .Item("title").Value(*Title_);
            })
            .DoIf(Parent_ != nullptr, [&] (TFluentMap fluent) {
                fluent
                    .Item("parent").Value(Parent_->GetDescription());
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

