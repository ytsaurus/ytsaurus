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

#include <core/concurrency/scheduler.h>

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_lookuper.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

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
    if (!list) {
        return nullptr;
    }

    if (maxTimestamp == LastCommittedTimestamp) {
        auto& value = list.Back();
        if (timestampExtractor(value) != UncommittedTimestamp) {
            return &value;
        }
        if (list.GetSize() > 1) {
            return &value - 1;
        }
        auto nextList = list.GetNext();
        if (!nextList) {
            return nullptr;
        }
        return &nextList.Back();
    } else {
        while (true) {
            if (!list) {
                return nullptr;
            }
            if (timestampExtractor(list.Front()) <= maxTimestamp) {
                break;
            }
            list = list.GetNext();
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
    if (list->GetSize() < list->GetCapacity()) {
        return false;
    }

    int newCapacity = std::min(list->GetCapacity() * EditListCapacityMultiplier, MaxEditListCapacity);
    auto newList = TEditList<T>::Allocate(pool, newCapacity);
    newList.SetNext(*list);
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
        list = list.GetNext();
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
    {
        YCHECK(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.All);

        if (columnFilter.All) {
            LockMask_ = TDynamicRow::AllLocksMask;
        } else {
            LockMask_ = TDynamicRow::PrimaryLockMask;
            const auto& columnIndexToLockIndex = Store_->Tablet_->ColumnIndexToLockIndex();
            for (int columnIndex : columnFilter.Indexes) {
                int lockIndex = columnIndexToLockIndex[columnIndex];
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

    ui32 LockMask_;


    int GetBlockingLockIndex(TDynamicRow dynamicRow)
    {
        const auto* lock = dynamicRow.BeginLocks(KeyColumnCount_);
        ui32 lockMaskBit = 1;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock, lockMaskBit <<= 1) {
            if ((LockMask_ & lockMaskBit) && lock->PrepareTimestamp < Timestamp_) {
                return index;
            }
        }
        return -1;
    }

    TVersionedRow ProduceSingleRowVersion(TDynamicRow dynamicRow)
    {
        if (Timestamp_ != LastCommittedTimestamp) {
            while (true) {
                int lockIndex = GetBlockingLockIndex(dynamicRow);
                if (lockIndex < 0)
                    break;
                Store_->RowBlocked_.Fire(dynamicRow, lockIndex);
            }
        }

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
        int valueCount = SchemaColumnCount_ - KeyColumnCount_;

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
                const auto* value = SearchList(
                    list,
                    Timestamp_,
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
        auto writeTimestampList = dynamicRow.GetTimestampList(ETimestampListKind::Write, KeyColumnCount_, ColumnLockCount_);
        auto deleteTimestampList = dynamicRow.GetTimestampList(ETimestampListKind::Delete, KeyColumnCount_, ColumnLockCount_);

        auto getCommittedTimestampCount = [] (TTimestampList list) {
            if (!list) {
                return 0;
            }
            int result = list.GetSize() + list.GetSuccessorsSize();
            if (list.Back() == UncommittedTimestamp) {
                --result;
            }
            return result;
        };
        int writeTimestampCount = getCommittedTimestampCount(writeTimestampList);
        int deleteTimestampCount = getCommittedTimestampCount(deleteTimestampList);

        if (writeTimestampCount == 0 && deleteTimestampCount == 0) {
            return TVersionedRow();
        }

        int valueCount = 0;
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
            if (list) {
                valueCount += list.GetSize() + list.GetSuccessorsSize();
                if (list.Back().Timestamp == UncommittedTimestamp) {
                    --valueCount;
                }
            }
        }

        auto versionedRow = TVersionedRow::Allocate(
            &Pool_,
            KeyColumnCount_,
            valueCount,
            writeTimestampCount,
            deleteTimestampCount);

        // Keys.
        ProduceKeys(dynamicRow, versionedRow.BeginKeys());

        // Fixed values.
        auto* currentValue = versionedRow.BeginValues();
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            auto currentList = dynamicRow.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
            while (currentList) {
                for (const auto* it = &currentList.Back(); it >= &currentList.Front(); --it) {
                    const auto& value = *it;
                    if (value.Timestamp != UncommittedTimestamp) {
                        *currentValue++ = value;
                    }
                }
                currentList = currentList.GetNext();
            }
        }

        // Timestamps.
        auto copyTimestamps = [] (TTimestampList list, TTimestamp* timestamps) {
            auto currentList = list;
            auto* currentTimestamp = timestamps;
            while (currentList) {
                for (const auto* it = &currentList.Back(); it >= &currentList.Front(); --it) {
                    auto timestamp = *it;
                    if (timestamp != UncommittedTimestamp) {
                        *currentTimestamp++ = timestamp;
                    }
                }
                currentList = currentList.GetNext();
            }
        };
        copyTimestamps(writeTimestampList, versionedRow.BeginWriteTimestamps());
        copyTimestamps(deleteTimestampList, versionedRow.BeginDeleteTimestamps());

        return versionedRow;
    }

    void ProduceKeys(TDynamicRow dynamicRow, TUnversionedValue* dstKey)
    {
        ui32 nullKeyMask = dynamicRow.GetNullKeyMask();
        ui32 nullKeyBit = 1;
        const auto* srcKey = dynamicRow.BeginKeys();
        auto columnIt = Store_->GetTablet()->Schema().Columns().begin();
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
                    *reinterpret_cast<TDynamicValueData*>(&dstKey->Data) = *srcKey;
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

    virtual TAsyncError Open() override
    {
        Iterator_ = Store_->Rows_->FindGreaterThanOrEqualTo(LowerKey_.Get());
        return OKFuture;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        YASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        TDynamicRowKeyComparer keyComparer(KeyColumnCount_, Store_->GetTablet()->Schema());

        while (Iterator_.IsValid() && rows->size() < rows->capacity()) {
            if (keyComparer(Iterator_.GetCurrent(), UpperKey_.Get()) >= 0)
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

    virtual TAsyncError GetReadyEvent() override
    {
        return OKFuture;
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
            Timestamp_ == AllCommittedTimestamp
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

    virtual TFuture<TErrorOr<TVersionedRow>> Lookup(TKey key) override
    {
        auto iterator = Store_->Rows_->FindEqualTo(key);
        if (!iterator.IsValid()) {
            return NullRow_;
        }

        auto dynamicRow = iterator.GetCurrent();
        auto versionedRow = ProduceSingleRowVersion(dynamicRow);
        return MakeFuture<TErrorOr<TVersionedRow>>(versionedRow);
    }

private:
    TFuture<TErrorOr<TVersionedRow>> NullRow_ = MakeFuture<TErrorOr<TVersionedRow>>(TVersionedRow());

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
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->GetSchemaColumnCount())
    , ColumnLockCount_(Tablet_->GetColumnLockCount())
    , RowBuffer_(
        Config_->AlignedPoolChunkSize,
        Config_->UnalignedPoolChunkSize,
        Config_->MaxPoolSmallBlockRatio)
    , Rows_(new TSkipList<TDynamicRow, TDynamicRowKeyComparer>(
        RowBuffer_.GetAlignedPool(),
        TDynamicRowKeyComparer(KeyColumnCount_, Tablet_->Schema())))
{
    State_ = EStoreState::ActiveDynamic;

    LOG_DEBUG("Dynamic memory store created (TabletId: %v, StoreId: %v)",
        Tablet_->GetId(),
        Id_);
}

TDynamicMemoryStore::~TDynamicMemoryStore()
{
    LOG_DEBUG("Dynamic memory store destroyed (StoreId: %v)",
        Id_);

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
        Id_,
        result);
    return result;
}

int TDynamicMemoryStore::Unlock()
{
    YASSERT(StoreLockCount_ > 0);
    int result = --StoreLockCount_;
    LOG_TRACE("Store unlocked (StoreId: %v, Count: %v)",
        Id_,
        result);
    return result;
}

TDynamicRow TDynamicMemoryStore::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock,
    ui32 lockMask)
{
    TDynamicRow result;

    auto addValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            AddUncommittedFixedValue(dynamicRow, value.Id, value);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(State_ == EStoreState::ActiveDynamic);

        auto dynamicRow = result = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row);

        // Acquire the lock.
        AcquireRowLocks(dynamicRow, transaction, prelock, lockMask, false);

        // Copy values.
        addValues(dynamicRow);

        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        result = dynamicRow;

        // Check for lock conflicts and acquire the lock.
        CheckRowLocks(dynamicRow, transaction, lockMask);
        AcquireRowLocks(dynamicRow, transaction, prelock, lockMask, false);

        // Copy values.
        addValues(dynamicRow);
    };

    Rows_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

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

        auto dynamicRow = result = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, key);

        // Acquire the lock.
        AcquireRowLocks(dynamicRow, transaction, prelock, TDynamicRow::PrimaryLockMask, true);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        result = dynamicRow;

        // Check for lock conflicts and acquire the lock.
        CheckRowLocks(dynamicRow, transaction, TDynamicRow::PrimaryLockMask);
        AcquireRowLocks(dynamicRow, transaction, prelock, TDynamicRow::PrimaryLockMask, true);
    };

    Rows_->Insert(
        key,
        newKeyProvider,
        existingKeyConsumer);

    OnMemoryUsageUpdated();

    return result;
}

TDynamicRow TDynamicMemoryStore::MigrateRow(
    TTransaction* transaction,
    const TDynamicRowRef& rowRef)
{
    auto row = rowRef.Row;
    // NB: We may change rowRef if the latter references
    // an element from transaction->LockedRows().
    auto* store = rowRef.Store;

    auto migrateLocksAndValues = [&] (TDynamicRow migratedRow) {
        auto migratedRowRef = TDynamicRowRef(this, migratedRow);

        auto* locks = row.BeginLocks(KeyColumnCount_);
        auto* migratedLocks = migratedRow.BeginLocks(KeyColumnCount_);

        const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();

        // Migrate locks.
        {
            const auto* lock = locks;
            auto* migratedLock = migratedLocks;
            for (int index = 0; index < ColumnLockCount_; ++index, ++lock, ++migratedLock) {
                if (lock->Transaction == transaction) {
                    YASSERT(lock->RowIndex != TLockDescriptor::InvalidRowIndex);
                    YASSERT(lock->PrepareTimestamp != NullTimestamp);
                    YASSERT(!migratedLock->Transaction);
                    YASSERT(migratedLock->RowIndex == TLockDescriptor::InvalidRowIndex);
                    YASSERT(migratedLock->PrepareTimestamp == MaxTimestamp);
                    migratedLock->Transaction = lock->Transaction;
                    migratedLock->RowIndex = lock->RowIndex;
                    migratedLock->PrepareTimestamp = lock->PrepareTimestamp;
                    migratedLock->LastCommitTimestamp = std::max(migratedLock->LastCommitTimestamp, lock->LastCommitTimestamp);
                    migratedLock->Transaction->LockedRows()[migratedLock->RowIndex] = migratedRowRef;
                    if (index == TDynamicRow::PrimaryLockIndex) {
                        YASSERT(!migratedRow.GetDeleteLockFlag());
                        migratedRow.SetDeleteLockFlag(row.GetDeleteLockFlag());
                    }
                }
            }
        }

        // Migrate fixed values.
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            int lockIndex = columnIndexToLockIndex[columnIndex];
            if (locks[lockIndex].Transaction == transaction) {
                auto list = row.GetFixedValueList(columnIndex, KeyColumnCount_, ColumnLockCount_);
                if (list) {
                    const auto& srcValue = list.Back();
                    if (srcValue.Timestamp == UncommittedTimestamp) {
                        auto migratedList = TValueList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
                        migratedRow.SetFixedValueList(columnIndex, migratedList, KeyColumnCount_, ColumnLockCount_);
                        CaptureValue(migratedList.BeginPush(), srcValue);
                        migratedList.EndPush();
                        ++StoreValueCount_;
                    }
                }
            }
        }

        // Release locks.
        {
            auto* lock = locks;
            for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
                if (lock->Transaction == transaction) {
                    lock->Transaction = nullptr;
                    lock->RowIndex = TLockDescriptor::InvalidRowIndex;
                    lock->PrepareTimestamp = MaxTimestamp;
                    if (index == TDynamicRow::PrimaryLockIndex) {
                        row.SetDeleteLockFlag(false);
                    }
                }
            }
        }

        Lock();
        store->Unlock();
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
            auto columnIt = Tablet_->Schema().Columns().begin();
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
    int rowIndex = static_cast<int>(transaction->LockedRows().size());
    transaction->LockedRows().push_back(TDynamicRowRef(this, row));

    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                YASSERT(lock->RowIndex == TLockDescriptor::InvalidRowIndex);
                lock->RowIndex = rowIndex;
            }
        }
    }
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
    YASSERT(State_ == EStoreState::ActiveDynamic);

    auto commitTimestamp = transaction->GetCommitTimestamp();
    YASSERT(commitTimestamp != NullTimestamp);

    const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();
    auto* locks = row.BeginLocks(KeyColumnCount_);

    for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
        const auto& lock = locks[columnIndexToLockIndex[index]];
        if (lock.Transaction == transaction) {
            auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
            if (list) {
                auto& lastValue = list.Back();
                if (lastValue.Timestamp == UncommittedTimestamp) {
                    lastValue.Timestamp = commitTimestamp;
                }
            }
        }
    }

    if (row.GetDeleteLockFlag()) {
        AddTimestamp(row, commitTimestamp, ETimestampListKind::Delete);
    } else {
        AddTimestamp(row, commitTimestamp, ETimestampListKind::Write);
    }

    {
        auto* lock = locks;
        YASSERT(lock[TDynamicRow::PrimaryLockIndex].LastCommitTimestamp <= commitTimestamp);
        lock[TDynamicRow::PrimaryLockIndex].LastCommitTimestamp = commitTimestamp;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->Transaction = nullptr;
                lock->RowIndex = TLockDescriptor::InvalidRowIndex;
                lock->PrepareTimestamp = MaxTimestamp;
                YASSERT(lock->LastCommitTimestamp <= commitTimestamp);
                lock->LastCommitTimestamp = commitTimestamp;
            }
        }
    }

    row.SetDeleteLockFlag(false);

    Unlock();

    MinTimestamp_ = std::min(MinTimestamp_, commitTimestamp);
    MaxTimestamp_ = std::max(MaxTimestamp_, commitTimestamp);
}

void TDynamicMemoryStore::AbortRow(TTransaction* transaction, TDynamicRow row)
{
    DropUncommittedValues(row);

    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->Transaction = nullptr;
                lock->RowIndex = TLockDescriptor::InvalidRowIndex;
                lock->PrepareTimestamp = MaxTimestamp;
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
                << TErrorAttribute("tablet_id", Tablet_->GetId())
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
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("key", RowToKey(row))
                    << TErrorAttribute("lock", Tablet_->LockIndexToName()[index]);
            }
            if (lock->LastCommitTimestamp >= transaction->GetStartTimestamp()) {
                THROW_ERROR_EXCEPTION("Row lock conflict")
                    << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_commit_timestamp", lock->LastCommitTimestamp)
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("key", RowToKey(row))
                    << TErrorAttribute("lock", Tablet_->LockIndexToName()[index]);
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
    int rowIndex = TLockDescriptor::InvalidRowIndex;
    if (!prelock) {
        rowIndex = static_cast<int>(transaction->LockedRows().size());
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
                YASSERT(lock->RowIndex == TLockDescriptor::InvalidRowIndex);
                lock->RowIndex = rowIndex;
                YASSERT(lock->PrepareTimestamp == MaxTimestamp);
            }
        }
    }

    if (deleteFlag) {
        YASSERT(!row.GetDeleteLockFlag());
        row.SetDeleteLockFlag(true);
    }

    Lock();
}

void TDynamicMemoryStore::DropUncommittedValues(TDynamicRow row)
{
    // Fixed values.
    for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
        if (list) {
            const auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                if (list.Pop() == 0) {
                    row.SetFixedValueList(index, list.GetNext(), KeyColumnCount_, ColumnLockCount_);
                }
            }
        }
    }
}

void TDynamicMemoryStore::AddFixedValue(
    TDynamicRow row,
    int columnIndex,
    const TVersionedValue& value)
{
    auto list = row.GetFixedValueList(columnIndex, KeyColumnCount_, ColumnLockCount_);

    if (list) {
        auto& lastValue = list.Back();
        if (lastValue.Timestamp == UncommittedTimestamp) {
            CaptureValue(&lastValue, value);
            return;
        }

        if (AllocateListForPushIfNeeded(&list, RowBuffer_.GetAlignedPool())) {
            row.SetFixedValueList(columnIndex, list, KeyColumnCount_, ColumnLockCount_);
        }
    } else {
        list = TValueList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
        row.SetFixedValueList(columnIndex, list, KeyColumnCount_, ColumnLockCount_);
    }

    CaptureValue(list.BeginPush(), value);
    list.EndPush();
    ++StoreValueCount_;
}

void TDynamicMemoryStore::AddUncommittedFixedValue(
    TDynamicRow row,
    int columnIndex,
    const TUnversionedValue& value)
{
    AddFixedValue(row, columnIndex, MakeVersionedValue(value, UncommittedTimestamp));
}

void TDynamicMemoryStore::AddTimestamp(TDynamicRow row, TTimestamp timestamp, ETimestampListKind kind)
{
    auto timestampList = row.GetTimestampList(kind, KeyColumnCount_, ColumnLockCount_);
    if (timestampList) {
        if (AllocateListForPushIfNeeded(&timestampList, RowBuffer_.GetAlignedPool())) {
            row.SetTimestampList(timestampList, kind, KeyColumnCount_, ColumnLockCount_);
        }
    } else {
        timestampList = TTimestampList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
        row.SetTimestampList(timestampList, kind, KeyColumnCount_, ColumnLockCount_);
    }
    timestampList.Push(timestamp);
}

void TDynamicMemoryStore::SetKeys(TDynamicRow dst, TUnversionedRow src)
{
    ui32 nullKeyMask = 0;
    ui32 nullKeyBit = 1;
    auto* dstValue = dst.BeginKeys();
    auto columnIt = Tablet_->Schema().Columns().begin();
    for (int index = 0;
         index < KeyColumnCount_;
         ++index, nullKeyBit <<= 1, ++dstValue, ++columnIt)
    {
        const auto& srcValue = src[index];
        if (srcValue.Type == EValueType::Null) {
            nullKeyMask |= nullKeyBit;
        } else {
            if (IsStringLikeType(columnIt->Type)) {
                *dstValue = CaptureStringValue(srcValue);
            } else {
                *reinterpret_cast<TUnversionedValueData*>(dstValue) = srcValue.Data;
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
        memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
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

i64 TDynamicMemoryStore::GetDataSize() const
{
    // Ignore memory stores when deciding to compact.
    return 0;
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
    auto it = Rows_->FindEqualTo(key);
    if (!it.IsValid())
        return;

    auto row = it.GetCurrent();
    CheckRowLocks(row, transaction, lockMask);
}

void TDynamicMemoryStore::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, GetPersistentState());

    SmallVector<TValueList, TypicalEditListCount> valueLists;
    SmallVector<TTimestampList, TypicalEditListCount> timestampLists;

    // Rows
    Save(context, Rows_->GetSize());
    for (auto rowIt = Rows_->FindGreaterThanOrEqualTo(MinKey().Get()); rowIt.IsValid(); rowIt.MoveNext()) {
        auto row = rowIt.GetCurrent();

        // Keys.
        SaveRowKeys(context, row, Tablet_);

        // Values.
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            auto topList = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
            if (!topList) {
                Save(context, static_cast<int>(0));
                continue;
            }

            int valueCount = topList.GetSize() + topList.GetSuccessorsSize();
            if (topList && topList.Back().Timestamp == UncommittedTimestamp) {
                --valueCount;
            }
            Save(context, valueCount);

            EnumerateListsAndReverse(topList, &valueLists);
            for (auto list : valueLists) {
                for (const auto* valueIt = list.Begin(); valueIt != list.End(); ++valueIt) {
                    const auto& value = *valueIt;
                    if (value.Timestamp != UncommittedTimestamp) {
                        NVersionedTableClient::Save(context, *valueIt);
                    }
                }
            }
        }

        // Timestamps.
        auto saveTimestamps = [&] (ETimestampListKind kind) {
            auto topList = row.GetTimestampList(kind, KeyColumnCount_, ColumnLockCount_);
            if (!topList) {
                Save(context, static_cast<int>(0));
                return;
            }

            int timestampCount = topList.GetSize() + topList.GetSuccessorsSize();
            Save(context, timestampCount);

            EnumerateListsAndReverse(topList, &timestampLists);
            for (auto list : timestampLists) {
                for (const auto* timestampIt = list.Begin(); timestampIt != list.End(); ++timestampIt) {
                    auto timestamp = *timestampIt;
                    YASSERT(timestamp != UncommittedTimestamp);
                    Save(context, timestamp);
                }
            }
        };
        saveTimestamps(ETimestampListKind::Write);
        saveTimestamps(ETimestampListKind::Delete);
    }
}

void TDynamicMemoryStore::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);

    auto* slot = context.GetSlot();
    auto transactionManager = slot->GetTransactionManager();

    // Rows
    int rowCount = Load<int>(context);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto row = AllocateRow();

        // Keys.
        LoadRowKeys(context, row, Tablet_, RowBuffer_.GetAlignedPool());

        // Values.
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            int valueCount = Load<int>(context);
            for (int valueIndex = 0; valueIndex < valueCount; ++valueIndex) {
                TVersionedValue value;
                NVersionedTableClient::Load(context, value, RowBuffer_.GetUnalignedPool());
                AddFixedValue(row, columnIndex, value);
            }
        }

        // Timestamps.
        auto loadTimestamps = [&] (ETimestampListKind kind)  {
            int timestampCount = Load<int>(context);
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
    TUnversionedOwningRowBuilder builder;
    ui32 nullKeyBit = 1;
    ui32 nullKeyMask = row.GetNullKeyMask();
    const auto* srcKey = row.BeginKeys();
    auto columnIt = Tablet_->Schema().Columns().begin();
    for (int index = 0;
         index < KeyColumnCount_;
         ++index, nullKeyBit <<= 1, ++srcKey, ++columnIt)
    {
        TUnversionedValue dstKey;
        dstKey.Id = index;
        if (nullKeyMask & nullKeyBit) {
            dstKey.Type = EValueType::Null;
        } else {
            dstKey.Type = columnIt->Type;
            if (IsStringLikeType(EValueType(dstKey.Type))) {
                dstKey.Length = srcKey->String->Length;
                dstKey.Data.String = srcKey->String->Data;
            } else {
                *reinterpret_cast<TDynamicValueData*>(&dstKey.Data) = *srcKey;
            }
        }
        builder.AddValue(dstKey);
    }
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
