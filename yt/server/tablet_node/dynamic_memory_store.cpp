#include "stdafx.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "private.h"

#include <core/misc/small_vector.h>
#include <core/misc/skip_list.h>

#include <core/profiling/timing.h>

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_lookuper.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;
static auto NullRowFuture = MakeFuture(TVersionedRow());

////////////////////////////////////////////////////////////////////////////////

static const int InitialEditListCapacity = 2;
static const int EditListCapacityMultiplier = 2;
static const int MaxEditListCapacity = 256;
static const int TypicalEditListCount = 16;
static const int TabletReaderPoolSize = 1024;
static const i64 MemoryUsageGranularity = (i64) 1024 * 1024;

struct TDynamicMemoryStoreFetcherPoolTag { };

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T, class TTimestampExtractor>
T* SearchList(
    TEditList<T> list,
    TTimestamp maxTimestamp,
    const TTimestampExtractor& timestampExtractor)
{
    if (maxTimestamp == SyncLastCommittedTimestamp || maxTimestamp == AsyncLastCommittedTimestamp) {
        while (list) {
            int size = list.GetSize();
            if (size > 0) {
                return &list[size - 1];
            }
            list = list.GetSuccessor();
        }
        return nullptr;
    } else {
        while (list) {
            if (list.GetSize() > 0 && timestampExtractor(list[0]) <= maxTimestamp) {
                break;
            }
            list = list.GetSuccessor();
        }

        if (!list) {
            return nullptr;
        }

        auto* left = list.Begin();
        auto* right = list.End();
        while (right - left > 1) {
            auto* mid = left + (right - left) / 2;
            if (timestampExtractor(*mid) <= maxTimestamp) {
                left = mid;
            }
            else {
                right = mid;
            }
        }

        YASSERT(!left || timestampExtractor(*left) <= maxTimestamp);
        return left;
    }
}

template <class T>
bool AllocateListForPushIfNeeded(
    TEditList<T>* list,
    TChunkedMemoryPool* pool)
{
    if (*list) {
        YASSERT(!list->HasUncommitted());
        if (list->GetSize() < list->GetCapacity()) {
            return false;
        }
    }

    int newCapacity = std::min(
        *list ? list->GetCapacity() * EditListCapacityMultiplier : InitialEditListCapacity,
        MaxEditListCapacity);
    auto newList = TEditList<T>::Allocate(pool, newCapacity);

    if (*list) {
        newList.SetSuccessor(*list);
    }

    *list = newList;
    return true;
}

template <class T>
void EnumerateListsAndReverse(
    TEditList<T> list,
    SmallVector<TEditList<T>, TypicalEditListCount>* result)
{
    result->clear();
    while (list) {
        result->push_back(list);
        list = list.GetSuccessor();
    }
    std::reverse(result->begin(), result->end());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TFetcherBase
{
public:
    explicit TFetcherBase(
        TDynamicMemoryStorePtr store,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : Store_(std::move(store))
        , Timestamp_(timestamp)
        , ColumnFilter_(columnFilter)
        , KeyColumnCount_(Store_->KeyColumnCount_)
        , SchemaColumnCount_(Store_->SchemaColumnCount_)
        , ColumnLockCount_(Store_->ColumnLockCount_)
        , Pool_(TDynamicMemoryStoreFetcherPoolTag(), TabletReaderPoolSize)
        , FixedValueListSnapshots_(SchemaColumnCount_)
    {
        YCHECK(Timestamp_ != AsyncAllCommittedTimestamp || ColumnFilter_.All);

        if (columnFilter.All) {
            LockMask_ = TDynamicRow::AllLocksMask;
        } else {
            LockMask_ = TDynamicRow::PrimaryLockMask;
            for (int columnIndex : columnFilter.Indexes) {
                int lockIndex = Store_->ColumnIndexToLockIndex_[columnIndex];
                LockMask_ |= (1 << lockIndex);
            }
        }
    }

protected:
    TDynamicMemoryStorePtr Store_;
    TTimestamp Timestamp_;
    TColumnFilter ColumnFilter_;

    int KeyColumnCount_;
    int SchemaColumnCount_;
    int ColumnLockCount_;

    TChunkedMemoryPool Pool_;

    template <class T>
    struct TEditListSnapshot
    {
        TEditList<T> List;
        int Size;

        int GetFullSize() const
        {
            return List ? Size + List.GetSuccessorsSize() : 0;
        }
    };

    typedef TEditListSnapshot<TTimestamp> TTimestampListSnapshot;
    typedef TEditListSnapshot<TVersionedValue> TValueListSnapshot;

    std::vector<TValueListSnapshot> FixedValueListSnapshots_;

    ui32 LockMask_;

    static TTimestampListSnapshot TakeSnapshot(TTimestampList list)
    {
        return TTimestampListSnapshot{list, list ? list.GetSize() : 0};
    }

    static TValueListSnapshot TakeSnapshot(TValueList list, TTimestamp maxTimestamp)
    {
        while (list) {
            int size = list.GetSize();
            while (size > 0) {
                const auto& value = list[size - 1];
                if (value.Timestamp <= maxTimestamp) {
                    return TValueListSnapshot{list, size};
                }
                --size;
            }
            list = list.GetSuccessor();
        }
        return TValueListSnapshot{TValueList(), 0};
    }

    TVersionedRow ProduceSingleRowVersion(TDynamicRow dynamicRow)
    {
        Store_->WaitOnBlockedRow(dynamicRow, LockMask_, Timestamp_);

        auto writeTimestampList = dynamicRow.GetTimestampList(ETimestampListKind::Write, KeyColumnCount_, ColumnLockCount_);
        const auto* writeTimestampPtr = SearchList(
            writeTimestampList,
            Timestamp_,
            [] (TTimestamp value) {
                return value;
            });
        auto writeTimestamp = writeTimestampPtr ? *writeTimestampPtr : NullTimestamp;

        auto deleteTimestampList = dynamicRow.GetTimestampList(ETimestampListKind::Delete, KeyColumnCount_, ColumnLockCount_);
        const auto* deleteTimestampPtr = SearchList(
            deleteTimestampList,
            Timestamp_,
            [] (TTimestamp value) {
                return value;
            });
        auto deleteTimestamp = deleteTimestampPtr ? *deleteTimestampPtr : NullTimestamp;

        if (writeTimestamp == NullTimestamp && deleteTimestamp == NullTimestamp) {
            return TVersionedRow();
        }

        int writeTimestampCount = 1;
        int deleteTimestampCount = 1;
        int valueCount = SchemaColumnCount_ - KeyColumnCount_; // an upper bound

        if (deleteTimestamp == NullTimestamp) {
            deleteTimestampCount = 0;
        } else if (deleteTimestamp > writeTimestamp) {
            writeTimestampCount = 0;
            valueCount = 0;
        }

        auto versionedRow = TVersionedRow::Allocate(
            &Pool_,
            KeyColumnCount_,
            valueCount,
            writeTimestampCount,
            deleteTimestampCount);

        ProduceKeys(dynamicRow, versionedRow.BeginKeys());

        if (writeTimestampCount > 0) {
            versionedRow.BeginWriteTimestamps()[0] = writeTimestamp;
        }

        if (deleteTimestampCount > 0) {
            versionedRow.BeginDeleteTimestamps()[0] = deleteTimestamp;
        }

        if (valueCount > 0) {
            auto* currentRowValue = versionedRow.BeginValues();
            auto fillValue = [&] (int index) {
                auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                // NB: Inserting a new item into value list and adding a new write timestamp cannot
                // be done atomically. We always append values before timestamps but in the middle of these
                // two steps there might be "phantom" values present in the row.
                // To work this around, we cap the value lists by #writeTimestamp to make sure that
                // no "phantom" value is listed.
                const auto* value = SearchList(
                    list,
                    writeTimestamp, // sic!
                    [] (const TVersionedValue& value) {
                        return value.Timestamp;
                    });
                if (value && value->Timestamp > deleteTimestamp) {
                    *currentRowValue++ = *value;
                }
            };
            if (ColumnFilter_.All) {
                for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
                    fillValue(index);
                }
            } else {
                for (int index : ColumnFilter_.Indexes) {
                    if (index >= KeyColumnCount_) {
                        fillValue(index);
                    }
                }
            }
            versionedRow.GetHeader()->ValueCount = currentRowValue - versionedRow.BeginValues();
        }

        return versionedRow;
    }

    TVersionedRow ProduceAllRowVersions(TDynamicRow dynamicRow)
    {
        // Take snapshots and count items.
        auto writeTimestampListSnapshot = TakeSnapshot(dynamicRow.GetTimestampList(
            ETimestampListKind::Write,
            KeyColumnCount_,
            ColumnLockCount_));
        int writeTimestampCount = writeTimestampListSnapshot.GetFullSize();

        auto deleteTimestampListSnapshot = TakeSnapshot(dynamicRow.GetTimestampList(
            ETimestampListKind::Delete,
            KeyColumnCount_,
            ColumnLockCount_));
        int deleteTimestampCount = deleteTimestampListSnapshot.GetFullSize();

        if (writeTimestampCount == 0 && deleteTimestampCount == 0) {
            return TVersionedRow();
        }

        // NB: See remarks above.
        auto maxWriteTimestamp = writeTimestampListSnapshot.List
            ? writeTimestampListSnapshot.List[writeTimestampListSnapshot.Size - 1]
            : NullTimestamp;

        int valueCount = 0;
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            auto& snapshot = FixedValueListSnapshots_[index];
            snapshot = TakeSnapshot(
                dynamicRow.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_),
                maxWriteTimestamp);
            valueCount += snapshot.GetFullSize();
        }

        auto versionedRow = TVersionedRow::Allocate(
            &Pool_,
            KeyColumnCount_,
            valueCount,
            writeTimestampCount,
            deleteTimestampCount);

        // Keys.
        ProduceKeys(dynamicRow, versionedRow.BeginKeys());

        // Timestamps (sorted in descending order).
        auto copyTimestamps = [] (
            const TTimestampListSnapshot& snapshot,
            TTimestamp* beginTimestamps,
            TTimestamp* endTimestamps)
        {
            auto currentList = snapshot.List;
            auto* currentTimestamp = beginTimestamps;
            while (currentList) {
                int currentSize = (currentList == snapshot.List ? snapshot.Size : currentList.GetSize());
                for (const auto* timestamp = currentList.Begin() + currentSize - 1; timestamp >= currentList.Begin(); --timestamp) {
                    *currentTimestamp++ = *timestamp;
                }
                currentList = currentList.GetSuccessor();
            }
            YCHECK(currentTimestamp == endTimestamps);
        };
        copyTimestamps(
            writeTimestampListSnapshot,
            versionedRow.BeginWriteTimestamps(),
            versionedRow.EndWriteTimestamps());
        copyTimestamps(
            deleteTimestampListSnapshot,
            versionedRow.BeginDeleteTimestamps(),
            versionedRow.EndDeleteTimestamps());

        // Fixed values (sorted by |id| in ascending order and then by |timestamp| in descending order).
        auto copyFixedValues = [] (const TValueListSnapshot& snapshot, TVersionedValue* beginValues) -> TVersionedValue* {
            auto currentList = snapshot.List;
            auto* currentValue = beginValues;
            while (currentList) {
                int currentSize = (currentList == snapshot.List ? snapshot.Size : currentList.GetSize());
                for (const auto* value = currentList.Begin() + currentSize - 1; value >= currentList.Begin(); --value) {
                    *currentValue++ = *value;
                }
                currentList = currentList.GetSuccessor();
            }
            return currentValue;
        };
        {
            auto* currentValue = versionedRow.BeginValues();
            for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
                currentValue = copyFixedValues(FixedValueListSnapshots_[index], currentValue);
            }
            YCHECK(currentValue == versionedRow.EndValues());
        }

        return versionedRow;
    }

    void ProduceKeys(TDynamicRow dynamicRow, TUnversionedValue* dstKey)
    {
        ui32 nullKeyMask = dynamicRow.GetNullKeyMask();
        ui32 nullKeyBit = 1;
        const auto* srcKey = dynamicRow.BeginKeys();
        auto columnIt = Store_->Schema_.Columns().begin();
        for (int index = 0;
             index < KeyColumnCount_;
             ++index, nullKeyBit <<= 1, ++srcKey, ++dstKey, ++columnIt)
        {
            dstKey->Id = index;
            if (nullKeyMask & nullKeyBit) {
                dstKey->Type = EValueType::Null;
            } else {
                dstKey->Type = columnIt->Type;
                if (IsStringLikeType(EValueType(dstKey->Type))) {
                    dstKey->Length = srcKey->String->Length;
                    dstKey->Data.String = srcKey->String->Data;
                } else {
                    ::memcpy(&dstKey->Data, srcKey, sizeof(TDynamicValueData));
                }
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TReader
    : public TFetcherBase
    , public IVersionedReader
{
public:
    TReader(
        TDynamicMemoryStorePtr store,
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : TFetcherBase(
            std::move(store),
            timestamp,
            columnFilter)
        , LowerKey_(std::move(lowerKey))
        , UpperKey_(std::move(upperKey))
    { }

    virtual TFuture<void> Open() override
    {
        Iterator_ = Store_->Rows_->FindGreaterThanOrEqualTo(TKeyWrapper{LowerKey_.Get()});
        return VoidFuture;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        YASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        auto keyComparer = Store_->GetTablet()->GetRowKeyComparer();

        while (Iterator_.IsValid() && rows->size() < rows->capacity()) {
            if (keyComparer(Iterator_.GetCurrent(), TKeyWrapper{UpperKey_.Get()}) >= 0)
                break;

            auto row = ProduceRow();
            if (row) {
                rows->push_back(row);
            }

            Iterator_.MoveNext();
        }

        if (rows->empty()) {
            Finished_ = true;
            return false;
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

private:
    TOwningKey LowerKey_;
    TOwningKey UpperKey_;

    TSkipList<TDynamicRow, TDynamicRowKeyComparer>::TIterator Iterator_;

    bool Finished_ = false;


    TVersionedRow ProduceRow()
    {
        auto dynamicRow = Iterator_.GetCurrent();
        return
            Timestamp_ == AsyncAllCommittedTimestamp
            ? ProduceAllRowVersions(dynamicRow)
            : ProduceSingleRowVersion(dynamicRow);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TLookuper
    : public TFetcherBase
    , public IVersionedLookuper
{
public:
    TLookuper(
        TDynamicMemoryStorePtr store,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : TFetcherBase(
            std::move(store),
            timestamp,
            columnFilter)
    { }

    virtual TFutureHolder<TVersionedRow> Lookup(TKey key) override
    {
        auto iterator = Store_->Rows_->FindEqualTo(TRowWrapper{key});
        if (!iterator.IsValid()) {
            return NullRowFuture;
        }

        auto dynamicRow = iterator.GetCurrent();
        auto versionedRow = ProduceSingleRowVersion(dynamicRow);
        return MakeFuture(versionedRow);
    }

};

////////////////////////////////////////////////////////////////////////////////

TDynamicMemoryStore::TDynamicMemoryStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(
        id,
        tablet)
    , Config_(config)
    , RowBuffer_(
        Config_->AlignedPoolChunkSize,
        Config_->UnalignedPoolChunkSize,
        Config_->MaxPoolSmallBlockRatio)
    , Rows_(new TSkipList<TDynamicRow, TDynamicRowKeyComparer>(
        RowBuffer_.GetAlignedPool(),
        tablet->GetRowKeyComparer()))
{
    State_ = EStoreState::ActiveDynamic;

    LOG_DEBUG("Dynamic memory store created (TabletId: %v, StoreId: %v)",
        TabletId_,
        StoreId_);
}

TDynamicMemoryStore::~TDynamicMemoryStore()
{
    LOG_DEBUG("Dynamic memory store destroyed (StoreId: %v)",
        StoreId_);

    MemoryUsageUpdated_.Fire(-MemoryUsage_);
    MemoryUsage_ = 0;
}

int TDynamicMemoryStore::GetLockCount() const
{
    return StoreLockCount_;
}

int TDynamicMemoryStore::Lock()
{
    int result = ++StoreLockCount_;
    LOG_TRACE("Store locked (StoreId: %v, Count: %v)",
        StoreId_,
        result);
    return result;
}

int TDynamicMemoryStore::Unlock()
{
    YASSERT(StoreLockCount_ > 0);
    int result = --StoreLockCount_;
    LOG_TRACE("Store unlocked (StoreId: %v, Count: %v)",
        StoreId_,
        result);
    return result;
}

void TDynamicMemoryStore::WaitOnBlockedRow(
    TDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    if (timestamp == AsyncLastCommittedTimestamp)
        return;

    auto now = NProfiling::GetCpuInstant();
    auto deadline = now + NProfiling::DurationToCpuDuration(Config_->MaxBlockedRowWaitTime);

    while (true) {
        int lockIndex = GetBlockingLockIndex(row, lockMask, timestamp);
        if (lockIndex < 0)
            break;

        RowBlocked_.Fire(row, lockIndex);

        if (NProfiling::GetCpuInstant() > deadline) {
            THROW_ERROR_EXCEPTION("Timed out waiting on blocked row")
                << TErrorAttribute("lock", LockIndexToName_[lockIndex])
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("key", RowToKey(row))
                << TErrorAttribute("timeout", Config_->MaxBlockedRowWaitTime);
        }
    }
}

TDynamicRow TDynamicMemoryStore::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock,
    ui32 lockMask)
{
    YASSERT(lockMask != 0);

    TDynamicRow result;

    auto addValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            AddUncommittedFixedValue(dynamicRow, MakeVersionedValue(value, UncommittedTimestamp));
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(State_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row);

        // Acquire the lock.
        AcquireRowLocks(dynamicRow, transaction, prelock, lockMask, false);

        // Copy values.
        addValues(dynamicRow);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Make sure the row is not blocked.
        ValidateRowNotBlocked(dynamicRow, lockMask, transaction->GetStartTimestamp());

        // Check for lock conflicts and acquire the lock.
        CheckRowLocks(dynamicRow, transaction, lockMask);
        AcquireRowLocks(dynamicRow, transaction, prelock, lockMask, false);

        // Copy values.
        addValues(dynamicRow);

        result = dynamicRow;
    };

    Rows_->Insert(TRowWrapper{row}, newKeyProvider, existingKeyConsumer);

    OnMemoryUsageUpdated();

    return result;
}

TDynamicRow TDynamicMemoryStore::DeleteRow(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool prelock)
{
    TDynamicRow result;

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(State_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, key);

        // Acquire the lock.
        AcquireRowLocks(dynamicRow, transaction, prelock, TDynamicRow::PrimaryLockMask, true);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Make sure the row is not blocked.
        ValidateRowNotBlocked(dynamicRow, TDynamicRow::PrimaryLockMask, transaction->GetStartTimestamp());

        // Check for lock conflicts and acquire the lock.
        CheckRowLocks(dynamicRow, transaction, TDynamicRow::PrimaryLockMask);
        AcquireRowLocks(dynamicRow, transaction, prelock, TDynamicRow::PrimaryLockMask, true);

        result = dynamicRow;
    };

    Rows_->Insert(TRowWrapper{key}, newKeyProvider, existingKeyConsumer);

    OnMemoryUsageUpdated();

    return result;
}

TDynamicRow TDynamicMemoryStore::MigrateRow(TTransaction* transaction, TDynamicRow row)
{
    auto migrateLocksAndValues = [&] (TDynamicRow migratedRow) {
        auto* locks = row.BeginLocks(KeyColumnCount_);
        auto* migratedLocks = migratedRow.BeginLocks(KeyColumnCount_);

        // Migrate locks.
        {
            const auto* lock = locks;
            auto* migratedLock = migratedLocks;
            for (int index = 0; index < ColumnLockCount_; ++index, ++lock, ++migratedLock) {
                if (lock->Transaction == transaction) {
                    YASSERT(lock->PrepareTimestamp != NotPreparedTimestamp);
                    YASSERT(!migratedLock->Transaction);
                    YASSERT(migratedLock->PrepareTimestamp == NotPreparedTimestamp);
                    migratedLock->Transaction = lock->Transaction;
                    migratedLock->PrepareTimestamp = lock->PrepareTimestamp;
                    migratedLock->LastCommitTimestamp = std::max(migratedLock->LastCommitTimestamp, lock->LastCommitTimestamp);
                    if (index == TDynamicRow::PrimaryLockIndex) {
                        YASSERT(!migratedRow.GetDeleteLockFlag());
                        migratedRow.SetDeleteLockFlag(row.GetDeleteLockFlag());
                    }
                }
            }
        }

        // Migrate fixed values.
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            int lockIndex = ColumnIndexToLockIndex_[columnIndex];
            if (locks[lockIndex].Transaction == transaction) {
                auto list = row.GetFixedValueList(columnIndex, KeyColumnCount_, ColumnLockCount_);
                if (list.HasUncommitted()) {
                    AddUncommittedFixedValue(migratedRow, list.GetUncommitted());
                }
            }
        }

        Lock();
    };

    TDynamicRow result;
    auto newKeyProvider = [&] () -> TDynamicRow {
        // Create migrated row.
        auto migratedRow = result = AllocateRow();

        // Migrate keys.
        {
            ui32 nullKeyMask = row.GetNullKeyMask();
            migratedRow.SetNullKeyMask(nullKeyMask);
            ui32 nullKeyBit = 1;
            const auto* key = row.BeginKeys();
            auto* migratedKey = migratedRow.BeginKeys();
            auto columnIt = Schema_.Columns().begin();
            for (int index = 0;
                 index < KeyColumnCount_;
                 ++index, nullKeyBit <<= 1, ++key, ++migratedKey, ++columnIt)
            {
                if (!(nullKeyMask & nullKeyBit) && IsStringLikeType(columnIt->Type)) {
                    *migratedKey = CaptureStringValue(*key);
                } else {
                    *migratedKey = *key;
                }
            }
        }

        migrateLocksAndValues(migratedRow);

        return migratedRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow migratedRow) {
        result = migratedRow;

        migrateLocksAndValues(migratedRow);
    };

    Rows_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

    OnMemoryUsageUpdated();

    return result;
}

void TDynamicMemoryStore::ConfirmRow(TTransaction* transaction, TDynamicRow row)
{
    transaction->LockedRows().push_back(TDynamicRowRef(this, row));
}

void TDynamicMemoryStore::PrepareRow(TTransaction* transaction, TDynamicRow row)
{
    auto prepareTimestamp = transaction->GetPrepareTimestamp();
    YASSERT(prepareTimestamp != NullTimestamp);

    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->PrepareTimestamp = prepareTimestamp;
            }
        }
    }
}

void TDynamicMemoryStore::CommitRow(TTransaction* transaction, TDynamicRow row)
{
    auto commitTimestamp = transaction->GetCommitTimestamp();
    YASSERT(commitTimestamp != NullTimestamp);

    auto* locks = row.BeginLocks(KeyColumnCount_);

    if (row.GetDeleteLockFlag()) {
        AddTimestamp(row, commitTimestamp, ETimestampListKind::Delete);
    } else {
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            const auto& lock = locks[ColumnIndexToLockIndex_[index]];
            if (lock.Transaction == transaction) {
                auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                if (list.HasUncommitted()) {
                    list.GetUncommitted().Timestamp = commitTimestamp;
                    list.Commit();
                }
            }
        }
        // NB: Add write timestamp _after_ the values are committed.
        // See remarks in TFetcherBase.
        AddTimestamp(row, commitTimestamp, ETimestampListKind::Write);
    }

    {
        auto* lock = locks;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->Transaction = nullptr;
                lock->PrepareTimestamp = NotPreparedTimestamp;
                YASSERT(lock->LastCommitTimestamp <= commitTimestamp);
                lock->LastCommitTimestamp = commitTimestamp;
            }
        }
    }

    row.SetDeleteLockFlag(false);

    Unlock();

    // NB: Don't update min/max timestamps for passive stores since
    // others are relying on its properties to remain constant.
    // See, e.g., TStoreManager::MaxTimestampToStore_.
    if (State_ == EStoreState::ActiveDynamic) {
        MinTimestamp_ = std::min(MinTimestamp_, commitTimestamp);
        MaxTimestamp_ = std::max(MaxTimestamp_, commitTimestamp);
    }
}

void TDynamicMemoryStore::AbortRow(TTransaction* transaction, TDynamicRow row)
{
    auto* locks = row.BeginLocks(KeyColumnCount_);

    if (!row.GetDeleteLockFlag()) {
        // Fixed values.
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            const auto& lock = locks[ColumnIndexToLockIndex_[index]];
            if (lock.Transaction == transaction) {
                auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                if (list.HasUncommitted()) {
                    list.Abort();
                }
            }
        }
    }

    {
        auto* lock = locks;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->Transaction = nullptr;
                lock->PrepareTimestamp = NotPreparedTimestamp;
            }
        }
    }

    row.SetDeleteLockFlag(false);

    Unlock();
}

TDynamicRow TDynamicMemoryStore::AllocateRow()
{
    return TDynamicRow::Allocate(
        RowBuffer_.GetAlignedPool(),
        KeyColumnCount_,
        ColumnLockCount_,
        SchemaColumnCount_);
}

int TDynamicMemoryStore::GetBlockingLockIndex(
    TDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    const auto* lock = row.BeginLocks(KeyColumnCount_);
    ui32 lockMaskBit = 1;
    for (int index = 0;
         index < ColumnLockCount_;
         ++index, ++lock, lockMaskBit <<= 1)
    {
        if ((lockMask & lockMaskBit) && lock->PrepareTimestamp < timestamp) {
            return index;
        }
    }
    return -1;
}

void TDynamicMemoryStore::ValidateRowNotBlocked(
    TDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    int lockIndex = GetBlockingLockIndex(row, lockMask, timestamp);
    if (lockIndex >= 0) {
        throw TRowBlockedException(this, row, lockMask, timestamp);
    }
}

void TDynamicMemoryStore::CheckRowLocks(
    TDynamicRow row,
    TTransaction* transaction,
    ui32 lockMask)
{
    const auto* lock = row.BeginLocks(KeyColumnCount_);
    ui32 lockMaskBit = 1;
    for (int index = 0; index < ColumnLockCount_; ++index, ++lock, lockMaskBit <<= 1) {
        if (lock->Transaction == transaction) {
            THROW_ERROR_EXCEPTION("Multiple modifications to a row within a single transaction are not allowed")
                << TErrorAttribute("transaction_id", transaction->GetId())
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("key", RowToKey(row));
        }
        // Check locks requested in #lockMask with the following exceptions:
        // * if primary lock is requested then all locks are checked
        // * primary lock is always checked
        if ((lockMask & lockMaskBit) ||
            (lockMask & TDynamicRow::PrimaryLockMask) ||
            (index == TDynamicRow::PrimaryLockIndex))
        {
            if (lock->Transaction) {
                THROW_ERROR_EXCEPTION("Row lock conflict")
                    << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_id", lock->Transaction->GetId())
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("key", RowToKey(row))
                    << TErrorAttribute("lock", LockIndexToName_[index]);
            }
            if (lock->LastCommitTimestamp > transaction->GetStartTimestamp()) {
                THROW_ERROR_EXCEPTION("Row lock conflict")
                    << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_commit_timestamp", lock->LastCommitTimestamp)
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("key", RowToKey(row))
                    << TErrorAttribute("lock", LockIndexToName_[index]);
            }
        }
    }
}

void TDynamicMemoryStore::AcquireRowLocks(
    TDynamicRow row,
    TTransaction* transaction,
    bool prelock,
    ui32 lockMask,
    bool deleteFlag)
{
    if (!prelock) {
        transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    }
    
    // Acquire locks requested in #lockMask with the following exceptions:
    // * if primary lock is requested then all locks are acquired
    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        ui32 lockMaskBit = 1;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock, lockMaskBit <<= 1) {
            if ((lockMask & lockMaskBit) || (lockMask & TDynamicRow::PrimaryLockMask)) {
                YASSERT(!lock->Transaction);
                lock->Transaction = transaction;
                YASSERT(lock->PrepareTimestamp == NotPreparedTimestamp);
            }
        }
    }

    if (deleteFlag) {
        YASSERT(!row.GetDeleteLockFlag());
        row.SetDeleteLockFlag(true);
    }

    Lock();
}

TValueList TDynamicMemoryStore::AddUncommittedFixedValue(TDynamicRow row, const TVersionedValue& value)
{
    YASSERT(value.Id >= KeyColumnCount_ && value.Id < SchemaColumnCount_);

    auto list = row.GetFixedValueList(value.Id, KeyColumnCount_, ColumnLockCount_);
    if (AllocateListForPushIfNeeded(&list, RowBuffer_.GetAlignedPool())) {
        row.SetFixedValueList(value.Id, list, KeyColumnCount_, ColumnLockCount_);
    }

    CaptureValue(list.Prepare(), value);
    ++StoreValueCount_;

    return list;
}

void TDynamicMemoryStore::AddTimestamp(TDynamicRow row, TTimestamp timestamp, ETimestampListKind kind)
{
    auto timestampList = row.GetTimestampList(kind, KeyColumnCount_, ColumnLockCount_);
    if (AllocateListForPushIfNeeded(&timestampList, RowBuffer_.GetAlignedPool())) {
        row.SetTimestampList(timestampList, kind, KeyColumnCount_, ColumnLockCount_);
    }
    timestampList.Push(timestamp);
}

void TDynamicMemoryStore::SetKeys(TDynamicRow dst, TUnversionedRow src)
{
    ui32 nullKeyMask = 0;
    ui32 nullKeyBit = 1;
    auto* dstValue = dst.BeginKeys();
    auto columnIt = Schema_.Columns().begin();
    for (int index = 0;
         index < KeyColumnCount_;
         ++index, nullKeyBit <<= 1, ++dstValue, ++columnIt)
    {
        const auto& srcValue = src[index];
        YASSERT(srcValue.Id == index);
        if (srcValue.Type == EValueType::Null) {
            nullKeyMask |= nullKeyBit;
        } else {
            YASSERT(srcValue.Type == columnIt->Type);
            if (IsStringLikeType(columnIt->Type)) {
                *dstValue = CaptureStringValue(srcValue);
            } else {
                ::memcpy(dstValue, &srcValue.Data, sizeof(TDynamicValueData));
            }
        }
    }
    dst.SetNullKeyMask(nullKeyMask);
}

void TDynamicMemoryStore::CaptureValue(TUnversionedValue* dst, const TUnversionedValue& src)
{
    *dst = src;
    CaptureValueData(dst, src);
}

void TDynamicMemoryStore::CaptureValue(TVersionedValue* dst, const TVersionedValue& src)
{
    *dst = src;
    CaptureValueData(dst, src);
}

void TDynamicMemoryStore::CaptureValueData(TUnversionedValue* dst, const TUnversionedValue& src)
{
    if (IsStringLikeType(EValueType(src.Type))) {
        dst->Data.String = RowBuffer_.GetUnalignedPool()->AllocateUnaligned(src.Length);
        ::memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
    }
}

TDynamicValueData TDynamicMemoryStore::CaptureStringValue(TDynamicValueData src)
{
    ui32 length = src.String->Length;
    TDynamicValueData dst;
    dst.String = reinterpret_cast<TDynamicString*>(RowBuffer_.GetAlignedPool()->AllocateAligned(
        sizeof(ui32) + length,
        sizeof(ui32)));
    ::memcpy(dst.String, src.String, sizeof(ui32) + length);
    return dst;
}

TDynamicValueData TDynamicMemoryStore::CaptureStringValue(const TUnversionedValue& src)
{
    YASSERT(IsStringLikeType(EValueType(src.Type)));
    ui32 length = src.Length;
    TDynamicValueData dst;
    dst.String = reinterpret_cast<TDynamicString*>(RowBuffer_.GetAlignedPool()->AllocateAligned(
        sizeof(ui32) + length,
        sizeof(ui32)));
    dst.String->Length = length;
    ::memcpy(dst.String->Data, src.Data.String, length);
    return dst;
}

int TDynamicMemoryStore::GetValueCount() const
{
    return StoreValueCount_;
}

int TDynamicMemoryStore::GetKeyCount() const
{
    return Rows_->GetSize();
}

i64 TDynamicMemoryStore::GetAlignedPoolSize() const
{
    return RowBuffer_.GetAlignedPool()->GetSize();
}

i64 TDynamicMemoryStore::GetAlignedPoolCapacity() const
{
    return RowBuffer_.GetAlignedPool()->GetCapacity();
}

i64 TDynamicMemoryStore::GetUnalignedPoolSize() const
{
    return RowBuffer_.GetUnalignedPool()->GetSize();
}

i64 TDynamicMemoryStore::GetUnalignedPoolCapacity() const
{
    return RowBuffer_.GetUnalignedPool()->GetCapacity();
}

EStoreType TDynamicMemoryStore::GetType() const
{
    return EStoreType::DynamicMemory;
}

i64 TDynamicMemoryStore::GetUncompressedDataSize() const
{
    return GetUnalignedPoolCapacity() + GetAlignedPoolCapacity();
}

i64 TDynamicMemoryStore::GetRowCount() const
{
    return Rows_->GetSize();
}

TOwningKey TDynamicMemoryStore::GetMinKey() const
{
    return MinKey();
}

TOwningKey TDynamicMemoryStore::GetMaxKey() const
{
    return MaxKey();
}

TTimestamp TDynamicMemoryStore::GetMinTimestamp() const
{
    return MinTimestamp_;
}

TTimestamp TDynamicMemoryStore::GetMaxTimestamp() const
{
    return MaxTimestamp_;
}

IVersionedReaderPtr TDynamicMemoryStore::CreateReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    return New<TReader>(
        this,
        std::move(lowerKey),
        std::move(upperKey),
        timestamp,
        columnFilter);
}

IVersionedLookuperPtr TDynamicMemoryStore::CreateLookuper(
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    return New<TLookuper>(this, timestamp, columnFilter);
}

void TDynamicMemoryStore::CheckRowLocks(
    NVersionedTableClient::TKey key,
    TTransaction* transaction,
    ui32 lockMask)
{
    auto it = Rows_->FindEqualTo(TRowWrapper{key});
    if (!it.IsValid())
        return;

    auto row = it.GetCurrent();
    CheckRowLocks(row, transaction, lockMask);
}

void TDynamicMemoryStore::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);

    using NYT::Save;

    Save(context, GetPersistentState());

    SmallVector<TValueList, TypicalEditListCount> valueLists;
    SmallVector<TTimestampList, TypicalEditListCount> timestampLists;

    // Rows
    Save<i32>(context, Rows_->GetSize());
    for (auto rowIt = Rows_->FindGreaterThanOrEqualTo(TKeyWrapper{MinKey().Get()});
         rowIt.IsValid();
         rowIt.MoveNext())
    {
        auto row = rowIt.GetCurrent();

        // Keys.
        SaveRowKeys(context, Schema_, KeyColumns_, row);

        // Values.
        auto saveFixedValues = [&] (int index) {
            auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
            if (!list) {
                Save<i32>(context, 0);
                return;
            }

            Save<i32>(context, list.GetFullSize());
            EnumerateListsAndReverse(list, &valueLists);
            for (auto list : valueLists) {
                for (const auto* value = list.Begin(); value != list.End(); ++value) {
                    YASSERT(value->Timestamp != UncommittedTimestamp);
                    NVersionedTableClient::Save(context, *value);
                }
            }
        };
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            saveFixedValues(index);
        }

        // Timestamps.
        auto saveTimestamps = [&] (ETimestampListKind kind) {
            auto list = row.GetTimestampList(kind, KeyColumnCount_, ColumnLockCount_);
            if (!list) {
                Save<i32>(context, 0);
                return;
            }

            Save<i32>(context, list.GetFullSize());
            EnumerateListsAndReverse(list, &timestampLists);
            for (auto list : timestampLists) {
                for (const auto* timestamp = list.Begin(); timestamp != list.End(); ++timestamp) {
                    YASSERT(*timestamp != UncommittedTimestamp);
                    Save(context, *timestamp);
                }
            }
        };
        saveTimestamps(ETimestampListKind::Write);
        saveTimestamps(ETimestampListKind::Delete);
    }
}

void TDynamicMemoryStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);

    using NYT::Load;

    Load(context, State_);

    auto* slot = context.GetSlot();
    auto transactionManager = slot->GetTransactionManager();

    // Rows
    int rowCount = Load<i32>(context);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto row = AllocateRow();

        // Keys.
        LoadRowKeys(context, Schema_, KeyColumns_, RowBuffer_.GetAlignedPool(), row);

        // Values.
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            int valueCount = Load<i32>(context);
            for (int valueIndex = 0; valueIndex < valueCount; ++valueIndex) {
                TVersionedValue value;
                NVersionedTableClient::Load(context, value, RowBuffer_.GetUnalignedPool());
                auto list = AddUncommittedFixedValue(row, value);
                list.Commit();
            }
        }

        // Timestamps.
        auto loadTimestamps = [&] (ETimestampListKind kind)  {
            int timestampCount = Load<i32>(context);
            for (int timestampIndex = 0; timestampIndex < timestampCount; ++timestampIndex) {
                auto timestamp = Load<TTimestamp>(context);
                YASSERT(timestamp != UncommittedTimestamp);
                AddTimestamp(row, timestamp, kind);
            }
        };
        loadTimestamps(ETimestampListKind::Write);
        loadTimestamps(ETimestampListKind::Delete);

        Rows_->Insert(row);
    }

    OnMemoryUsageUpdated();
}

void TDynamicMemoryStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    TStoreBase::BuildOrchidYson(consumer);

    BuildYsonMapFluently(consumer)
        .Item("key_count").Value(GetKeyCount())
        .Item("lock_count").Value(GetLockCount())
        .Item("value_count").Value(GetValueCount())
        .Item("aligned_pool_size").Value(GetAlignedPoolSize())
        .Item("aligned_pool_capacity").Value(GetAlignedPoolCapacity())
        .Item("unaligned_pool_size").Value(GetUnalignedPoolSize())
        .Item("unaligned_pool_capacity").Value(GetUnalignedPoolCapacity());
}

i64 TDynamicMemoryStore::GetMemoryUsage() const
{
    return MemoryUsage_;
}

void TDynamicMemoryStore::OnMemoryUsageUpdated()
{
    i64 memoryUsage = GetAlignedPoolCapacity() + GetUnalignedPoolCapacity();
    YASSERT(memoryUsage >= MemoryUsage_);
    if (memoryUsage > MemoryUsage_ + MemoryUsageGranularity) {
        i64 delta = memoryUsage - MemoryUsage_;
        MemoryUsage_ = memoryUsage;
        MemoryUsageUpdated_.Fire(delta);
    }
}

TOwningKey TDynamicMemoryStore::RowToKey(TDynamicRow row)
{
    return NTabletNode::RowToKey(Schema_, KeyColumns_, row);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
