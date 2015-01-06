#include "stdafx.h"
#include "transaction.h"
#include "automaton.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "dynamic_memory_store.h"

#include <core/misc/small_vector.h>

#include <ytlib/new_table_client/versioned_row.h>

#include <server/hydra/composite_automaton.h>

namespace NYT {
namespace NTabletNode {

using namespace NTransactionClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : Id_(id)
    , StartTime_(TInstant::Zero())
    , State_(ETransactionState::Active)
    , StartTimestamp_(NullTimestamp)
    , PrepareTimestamp_(NullTimestamp)
    , CommitTimestamp_(NullTimestamp)
    , Finished_(NewPromise<void>())
{ }

void TTransaction::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Timeout_);
    Save(context, StartTime_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);

    Save(context, LockedRows_.size());
    for (const auto& rowRef : LockedRows_) {
        auto* store = rowRef.Store;
        if (store->GetState() == EStoreState::Orphaned) {
            Save(context, NullTabletId);
            continue;
        }

        auto row = rowRef.Row;
        auto* tablet = store->GetTablet();

        int keyColumnCount = tablet->GetKeyColumnCount();
        int schemaColumnCount = tablet->GetSchemaColumnCount();
        int columnLockCount = tablet->GetColumnLockCount();
        const auto& columnIndexToLockIndex = tablet->ColumnIndexToLockIndex();
        const auto* locks = row.BeginLocks(keyColumnCount);

        // Tablet
        Save(context, tablet->GetId());

        // Keys
        SaveRowKeys(context, tablet->Schema(), tablet->KeyColumns(), row);

        // Fixed values
        ui32 lockMask = 0;
        for (int columnIndex = keyColumnCount; columnIndex < schemaColumnCount; ++columnIndex) {
            int lockIndex = columnIndexToLockIndex[columnIndex];
            if (locks[lockIndex].Transaction == this) {
                auto list = rowRef.Row.GetFixedValueList(columnIndex, keyColumnCount, columnLockCount);
                if (list && list.HasUncommitted()) {
                    NVersionedTableClient::Save(context, TUnversionedValue(list.GetUncommitted()));
                    lockMask |= (1 << lockIndex);
                }
            }
        }
        NVersionedTableClient::Save(context, MakeUnversionedSentinelValue(EValueType::TheBottom));

        // Misc
        Save(context, lockMask);
        Save(context, row.GetDeleteLockFlag());
    }
}

void TTransaction::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Timeout_);
    Load(context, StartTime_);
    Load(context, State_);
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);

    auto tabletManager = context.GetSlot()->GetTabletManager();

    size_t lockedRowCount = Load<size_t>(context);
    LockedRows_.reserve(lockedRowCount);

    auto* tempPool = context.GetTempPool();
    auto* rowBuilder = context.GetRowBuilder();

    for (size_t rowIndex = 0; rowIndex < lockedRowCount; ++rowIndex) {
        // Tablet
        auto tabletId = Load<TTabletId>(context);
        if (tabletId == NullTabletId) {
            continue;
        }

        auto* tablet = tabletManager->GetTablet(tabletId);
        const auto& store = tablet->GetActiveStore();
        YCHECK(store);

        tempPool->Clear();
        rowBuilder->Reset();

        // Keys
        LoadRowKeys(context, tablet->Schema(), tablet->KeyColumns(), tempPool, rowBuilder);

        // Values
        while (true) {
            TUnversionedValue value;
            Load(context, value, tempPool);
            if (value.Type == EValueType::TheBottom)
                break;
            rowBuilder->AddValue(value);
        }

        // Misc
        bool deleteLockFlag = Load<bool>(context);
        ui32 lockMask = Load<ui32>(context);

        TDynamicRow dynamicRow;
        auto row = rowBuilder->GetRow();
        if ((lockMask & TDynamicRow::PrimaryLockMask) && deleteLockFlag) {
            dynamicRow = store->DeleteRow(this, row, false);
        } else {
            dynamicRow = store->WriteRow(this, row, false, lockMask);
        }
        YCHECK(dynamicRow);

        if (PrepareTimestamp_ != NullTimestamp) {
            store->PrepareRow(this, dynamicRow);
        }
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

TTimestamp TTransaction::GetPersistentPrepareTimestamp() const
{
    switch (State_) {
        case ETransactionState::TransientCommitPrepared:
            return NullTimestamp;
        default:
            return PrepareTimestamp_;
    }
}

void TTransaction::ThrowInvalidState() const
{
    THROW_ERROR_EXCEPTION("Transaction %v is in %Qlv state",
        Id_,
        State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

