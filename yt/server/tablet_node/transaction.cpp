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
    , Finished_(NewPromise())
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

        // Tablet
        Save(context, tablet->GetId());

        // Lock mode
        Save(context, row.GetLockMode());

        // Keys
        auto* keyBegin = row.GetKeys();
        auto* keyEnd = keyBegin + tablet->GetKeyColumnCount();
        for (auto* it = keyBegin; it != keyEnd; ++it) {
            NVersionedTableClient::Save(context, *it);
        }

        // Fixed values
        for (int listIndex = 0; listIndex < tablet->GetSchemaColumnCount() - tablet->GetKeyColumnCount(); ++listIndex) {
            auto list = rowRef.Row.GetFixedValueList(listIndex, tablet->GetKeyColumnCount());
            if (list) {
                const auto& value = list.Back();
                if ((value.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    NVersionedTableClient::Save(context, TUnversionedValue(value));
                }
            }
        }

        // Sentinel
        NVersionedTableClient::Save(context, MakeUnversionedSentinelValue(EValueType::TheBottom));
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

        // Lock mode
        auto lockMode = Load<ERowLockMode>(context);

        // Keys and fixed values
        tempPool->Clear();
        rowBuilder->Reset();

        while (true) {
            TUnversionedValue value;
            Load(context, value, tempPool);
            if (value.Type == EValueType::TheBottom)
                break;
            rowBuilder->AddValue(value);
        }

        auto deserializedRow = rowBuilder->GetRow();
        TDynamicRow dynamicRow;
        switch (lockMode) {
            case ERowLockMode::Delete:
                dynamicRow = store->DeleteRow(this, deserializedRow, false);
                break;

            case ERowLockMode::Write:
                dynamicRow = store->WriteRow(this, deserializedRow, false);
                break;

            default:
                YUNREACHABLE();
        }

        YASSERT(dynamicRow.GetTransaction() == this);
        YASSERT(dynamicRow.GetLockMode() == lockMode);
        YASSERT(dynamicRow.GetLockIndex() == rowIndex);
        
        if (PrepareTimestamp_ != NullTimestamp) {
            store->PrepareRow(dynamicRow);
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
    Finished_ = NewPromise();
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
    THROW_ERROR_EXCEPTION("Transaction %v is in %v state",
        ~ToString(Id_),
        ~FormatEnum(State_).Quote());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

