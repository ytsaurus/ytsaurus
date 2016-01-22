#include "transaction.h"
#include "automaton.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTabletNode {

using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TTransactionWriteRecord::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletId);
    Save(context, Data);
}

void TTransactionWriteRecord::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletId);
    Load(context, Data);
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TTransactionBase(id)
    , RegisterTime_(TInstant::Zero())
    , StartTimestamp_(NullTimestamp)
    , PrepareTimestamp_(NullTimestamp)
    , CommitTimestamp_(NullTimestamp)
    , Finished_(NewPromise<void>())
{ }

void TTransaction::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Timeout_);
    Save(context, RegisterTime_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);
}

void TTransaction::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Timeout_);
    Load(context, RegisterTime_);
    Load(context, State_);
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);
}

TCallback<void(TSaveContext&)> TTransaction::AsyncSave()
{
    auto writeLogSnapshot = WriteLog_.MakeSnapshot();
    return BIND([writeLogSnapshot = std::move(writeLogSnapshot)] (TSaveContext& context) {
        using NYT::Save;

        TSizeSerializer::Save(context, writeLogSnapshot.Size());
        for (const auto& record : writeLogSnapshot) {
            Save(context, record);
        }
    });
}

void TTransaction::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    YCHECK(WriteLog_.Empty());

    int recordCount = TSizeSerializer::Load(context);
    for (int index = 0; index < recordCount; ++index) {
        WriteLog_.Enqueue(Load<TTransactionWriteRecord>(context));
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
    switch (State_) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

