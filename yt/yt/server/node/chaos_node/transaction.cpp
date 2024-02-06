#include "transaction.h"
#include "automaton.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NChaosNode {

using namespace NHiveServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TTransactionId id)
    : TTransactionBase(id)
{ }

void TTransaction::Save(TSaveContext& context) const
{
    TTransactionBase::Save(context);

    using NYT::Save;

    Save(context, Timeout_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);
    Save(context, User_);
}

void TTransaction::Load(TLoadContext& context)
{
    TTransactionBase::Load(context);

    using NYT::Load;

    Load(context, Timeout_);
    SetPersistentState(Load<ETransactionState>(context));
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);
    Load(context, User_);

    // COMPAT(kvk1920)
    if (context.GetVersion() >= EChaosReign::SaneTxActionAbort &&
        context.GetVersion() < EChaosReign::SaneTxActionAbortFix)
    {
        Load(context, PreparedActionCount_);
    }
}

TFuture<void> TTransaction::GetFinished() const
{
    return Finished_;
}

void TTransaction::SetFinished()
{
    Finished_.Set();
}

void TTransaction::ResetFinished()
{
    Finished_.Set();
    Finished_ = NewPromise<void>();
}

TTimestamp TTransaction::GetPersistentPrepareTimestamp() const
{
    switch (GetTransientState()) {
        case ETransactionState::TransientCommitPrepared:
            return NullTimestamp;
        default:
            return PrepareTimestamp_;
    }
}

TInstant TTransaction::GetStartTime() const
{
    return TimestampToInstant(StartTimestamp_).first;
}

TCellTag TTransaction::GetCellTag() const
{
    return CellTagFromId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
