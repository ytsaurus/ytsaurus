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

        return left && timestampExtractor(*left) <= maxTimestamp
            ? left
            : nullptr;
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
        , KeyColumnCount_(Store_->Tablet_->GetKeyColumnCount())
        , SchemaColumnCount_(Store_->Tablet_->GetSchemaColumnCount())
        , Pool_(TDynamicMemoryStoreFetcherPoolTag(), TabletReaderPoolSize)
    {
        YCHECK(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.All);
    }

protected:
    TDynamicMemoryStorePtr Store_;
    TTimestamp Timestamp_;
    TColumnFilter ColumnFilter_;

    int KeyColumnCount_;
    int SchemaColumnCount_;

    TChunkedMemoryPool Pool_;
    

    void WaitOnRow(TDynamicRow dynamicRow)
    {
        WaitFor(
            BIND(&TFetcherBase::DoWaitOnRow, dynamicRow)
                .AsyncVia(Store_->Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read))
                .Run());
    }

    static void DoWaitOnRow(TDynamicRow dynamicRow)
    {
        auto* transaction = dynamicRow.GetTransaction();
        if (transaction) {
            WaitFor(transaction->GetFinished());
        }
    }


    TVersionedRow ProduceSingleRowVersion(TDynamicRow dynamicRow)
    {
        if (Timestamp_ != LastCommittedTimestamp && dynamicRow.GetPrepareTimestamp() < Timestamp_) {
            WaitOnRow(dynamicRow);
        }

        auto timestampList = dynamicRow.GetTimestampList(KeyColumnCount_);
        const auto* minTimestampPtr = SearchList(
            timestampList,
            Timestamp_,
            [] (TTimestamp value) {
                return value & TimestampValueMask;
            });

        if (!minTimestampPtr) {
            return TVersionedRow();
        }

        auto versionedRow = TVersionedRow::Allocate(&Pool_, KeyColumnCount_, SchemaColumnCount_ - KeyColumnCount_, 1);
        memcpy(versionedRow.BeginKeys(), dynamicRow.GetKeys(), KeyColumnCount_ * sizeof (TUnversionedValue));

        auto minTimestamp = *minTimestampPtr;
        if (minTimestamp & TombstoneTimestampMask) {
            versionedRow.GetHeader()->ValueCount = 0;
            versionedRow.BeginTimestamps()[0] = minTimestamp;
            return versionedRow;
        }

        auto* currentRowValue = versionedRow.BeginValues();
        auto fillValue = [&] (int index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_);
            const auto* value = SearchList(
                list,
                Timestamp_,
                [] (const TVersionedValue& value) {
                    return value.Timestamp;
                });
            if (value && value->Timestamp >= minTimestamp) {
                *currentRowValue++ = *value;
            }
        };

        if (ColumnFilter_.All) {
            for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
                fillValue(index);
            }
        } else {
            for (int index : ColumnFilter_.Indexes) {
                if (index >= KeyColumnCount_) {
                    fillValue(index - KeyColumnCount_);
                }
            }
        }

        versionedRow.GetHeader()->ValueCount = currentRowValue - versionedRow.BeginValues();

        versionedRow.BeginTimestamps()[0] =
            minTimestamp |
            (minTimestampPtr == timestampList.Begin() ? IncrementalTimestampMask : 0);

        return versionedRow;
    }

    TVersionedRow ProduceAllRowVersions(TDynamicRow dynamicRow)
    {
        auto timestampList = dynamicRow.GetTimestampList(KeyColumnCount_);
        if (!timestampList) {
            return TVersionedRow();
        }

        int timestampCount = timestampList.GetSize() + timestampList.GetSuccessorsSize();
        if ((timestampList.Back() & TimestampValueMask) == UncommittedTimestamp) {
            --timestampCount;
        }

        if (timestampCount == 0) {
            return TVersionedRow();
        }

        int valueCount = 0;
        for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_);
            if (list) {
                valueCount += list.GetSize() + list.GetSuccessorsSize();
                if ((list.Back().Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    --valueCount;
                }
            }
        }

        auto versionedRow = TVersionedRow::Allocate(&Pool_, KeyColumnCount_, valueCount, timestampCount);

        // Keys.
        memcpy(versionedRow.BeginKeys(), dynamicRow.GetKeys(), KeyColumnCount_ * sizeof (TUnversionedValue));

        // Fixed values.
        auto* currentValue = versionedRow.BeginValues();
        for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
            auto currentList = dynamicRow.GetFixedValueList(index, KeyColumnCount_);
            while (currentList) {
                for (const auto* it = &currentList.Back(); it >= &currentList.Front(); --it) {
                    const auto& value = *it;
                    if ((value.Timestamp & TimestampValueMask) != UncommittedTimestamp) {
                        *currentValue++ = value;
                    }
                }
                currentList = currentList.GetNext();
            }
        }
        YASSERT(currentValue == versionedRow.EndValues());

        // Timestamps.
        auto currentTimestampList = timestampList;
        auto* currentTimestamp = versionedRow.BeginTimestamps();
        while (currentTimestampList) {
            for (const auto* it = &currentTimestampList.Back(); it >= &currentTimestampList.Front(); --it) {
                auto timestamp = *it;
                if ((timestamp & TimestampValueMask) != UncommittedTimestamp) {
                    *currentTimestamp++ = timestamp;
                }
            }
            currentTimestampList = currentTimestampList.GetNext();
        }
        YASSERT(currentTimestamp == versionedRow.EndTimestamps());

        return versionedRow;
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
        Iterator_ = Store_->Rows_->FindGreaterThanOrEqualTo(LowerKey_);
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

        TKeyComparer keyComparer(KeyColumnCount_);

        while (Iterator_.IsValid() && rows->size() < rows->capacity()) {
            const auto* rowKeys = Iterator_.GetCurrent().GetKeys();
            if (CompareRows(rowKeys, rowKeys + KeyColumnCount_, UpperKey_.Begin(), UpperKey_.End()) >= 0)
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

    TSkipList<TDynamicRow, TKeyComparer>::TIterator Iterator_;

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
            static const auto NullRow = MakeFuture<TErrorOr<TVersionedRow>>(TVersionedRow());
            return NullRow;
        }

        auto dynamicRow = iterator.GetCurrent();
        auto versionedRow = ProduceSingleRowVersion(dynamicRow);
        return MakeFuture<TErrorOr<TVersionedRow>>(versionedRow);
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
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->GetSchemaColumnCount())
    , RowBuffer_(
        Config_->AlignedPoolChunkSize,
        Config_->UnalignedPoolChunkSize,
        Config_->MaxPoolSmallBlockRatio)
    , Rows_(new TSkipList<TDynamicRow, TKeyComparer>(
        RowBuffer_.GetAlignedPool(),
        TKeyComparer(KeyColumnCount_)))
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
    return LockCount_;
}

int TDynamicMemoryStore::Lock()
{
    int result = ++LockCount_;
    LOG_TRACE("Store locked (StoreId: %v, Count: %v)",
        Id_,
        result);
    return result;
}

int TDynamicMemoryStore::Unlock()
{
    YASSERT(LockCount_ > 0);
    int result = --LockCount_;
    LOG_TRACE("Store unlocked (StoreId: %v, Count: %v)",
        Id_,
        result);
    return result;
}

TDynamicRow TDynamicMemoryStore::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock)
{
    TDynamicRow result;

    auto addValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            AddUncommittedFixedValue(dynamicRow, value.Id - KeyColumnCount_, value);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        YASSERT(State_ == EStoreState::ActiveDynamic);
        auto dynamicRow = result = AllocateRow();
        YCHECK(LockRow(dynamicRow, transaction, ERowLockMode::Write, prelock));

        // Add timestamp.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp);

        // Copy keys.
        for (int id = 0; id < KeyColumnCount_; ++id) {
            auto& dstValue = dynamicRow.GetKeys()[id];
            CaptureValue(&dstValue, row[id]);
            dstValue.Id = id;
        }

        // Copy values.
        addValues(dynamicRow);

        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Check for lock conflicts and acquire the lock.
        if (LockRow(dynamicRow, transaction, ERowLockMode::Write, prelock)) {
            result = dynamicRow;
        }

        // Add timestamp, if needed.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp);

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
    YASSERT(State_ == EStoreState::ActiveDynamic);

    TDynamicRow result;

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        YCHECK(LockRow(dynamicRow, transaction, ERowLockMode::Delete, prelock));

        // Add tombstone.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp | TombstoneTimestampMask);

        // Copy keys.
        for (int id = 0; id < KeyColumnCount_; ++id) {
            auto& internedValue = dynamicRow.GetKeys()[id];
            CaptureValue(&internedValue, key[id]);
            internedValue.Id = id;
        }

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Check for lock conflicts and acquire the lock.
        if (LockRow(dynamicRow, transaction, ERowLockMode::Delete, prelock)) {
            result = dynamicRow;
        }

        // Add tombstone.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp | TombstoneTimestampMask);
    };

    Rows_->Insert(
        key,
        newKeyProvider,
        existingKeyConsumer);

    OnMemoryUsageUpdated();

    return result;
}

TDynamicRow TDynamicMemoryStore::MigrateRow(const TDynamicRowRef& rowRef)
{
    auto row = rowRef.Row;
    // NB: We may change rowRef if the latter references
    // an element from transaction->LockedRows().
    auto* store = rowRef.Store;

    TDynamicRow migratedRow;
    auto newKeyProvider = [&] () -> TDynamicRow {
        // Create migrated row.
        migratedRow = AllocateRow();

        // Migrate lock.
        Lock();
        auto* transaction = row.GetTransaction();
        int lockIndex = row.GetLockIndex();
        migratedRow.Lock(transaction, lockIndex, row.GetLockMode());
        migratedRow.SetPrepareTimestamp(row.GetPrepareTimestamp());
        if (lockIndex != TDynamicRow::InvalidLockIndex) {
            transaction->LockedRows()[lockIndex] = TDynamicRowRef(this, migratedRow);
        }

        // Migrate keys.
        const auto* srcKeys = row.GetKeys();
        auto* dstKeys = migratedRow.GetKeys();
        for (int index = 0; index < KeyColumnCount_; ++index) {
            CaptureValue(&dstKeys[index], srcKeys[index]);
        }

        // Migrate fixed values.
        for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
            auto list = row.GetFixedValueList(index, KeyColumnCount_);
            if (list) {
                const auto& srcValue = list.Back();
                if ((srcValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    auto migratedList = TValueList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
                    migratedRow.SetFixedValueList(index, migratedList, KeyColumnCount_);
                    migratedList.Push([&] (TVersionedValue* dstValue) {
                        CaptureValue(dstValue, srcValue);
                    });
                    ++ValueCount_;
                }
            }
        }

        // Migrate timestamps.
        auto timestampList = row.GetTimestampList(KeyColumnCount_);
        if (timestampList) {
            auto timestamp = timestampList.Back();
            if ((timestamp & TimestampValueMask) == UncommittedTimestamp) {
                auto migratedTimestampList = TTimestampList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
                migratedRow.SetTimestampList(migratedTimestampList, KeyColumnCount_);
                migratedTimestampList.Push(timestamp);
            }
        }

        store->Unlock();
        row.Unlock();

        return migratedRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow /*dynamicRow*/) {
        YUNREACHABLE();
    };

    Rows_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

    OnMemoryUsageUpdated();

    return migratedRow;
}

TDynamicRow TDynamicMemoryStore::FindRowAndCheckLocks(
    NVersionedTableClient::TKey key,
    TTransaction* transaction,
    ERowLockMode mode)
{
    auto it = Rows_->FindEqualTo(key);
    if (!it.IsValid()) {
        return TDynamicRow();
    }

    auto row = it.GetCurrent();
    CheckRowLock(row, transaction, mode);

    if (row.GetLockMode() == ERowLockMode::None) {
        return TDynamicRow();
    }

    return row;
}

void TDynamicMemoryStore::ConfirmRow(TDynamicRow row)
{
    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    int lockIndex = static_cast<int>(transaction->LockedRows().size());
    transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    row.SetLockIndex(lockIndex);
}

void TDynamicMemoryStore::PrepareRow(TDynamicRow row)
{
    auto* transaction = row.GetTransaction();
    YASSERT(transaction);

    auto prepareTimestamp = transaction->GetPrepareTimestamp();
    YASSERT(prepareTimestamp != NullTimestamp);

    row.SetPrepareTimestamp(prepareTimestamp);
}

void TDynamicMemoryStore::CommitRow(TDynamicRow row)
{
    YASSERT(State_ == EStoreState::ActiveDynamic);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);

    auto commitTimestamp = transaction->GetCommitTimestamp();
    YASSERT(commitTimestamp != NullTimestamp);

    // Edit timestamps.
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        auto& lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            lastTimestamp = (lastTimestamp & ~TimestampValueMask) | commitTimestamp;
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyColumnCount_);
        if (list) {
            auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                lastValue.Timestamp = commitTimestamp;
            }
        }
    }

    Unlock();
    row.Unlock();

    auto timestamp = transaction->GetCommitTimestamp();
    row.SetLastCommitTimestamp(timestamp);
    MinTimestamp_ = std::min(MinTimestamp_, timestamp);
    MaxTimestamp_ = std::max(MaxTimestamp_, timestamp);
}

void TDynamicMemoryStore::AbortRow(TDynamicRow row)
{
    DropUncommittedValues(row);
    Unlock();
    row.Unlock();
}

TDynamicRow TDynamicMemoryStore::AllocateRow()
{
    return TDynamicRow::Allocate(
        RowBuffer_.GetAlignedPool(),
        KeyColumnCount_,
        SchemaColumnCount_);
}

void TDynamicMemoryStore::CheckRowLock(
    TDynamicRow row,
    TTransaction* transaction,
    ERowLockMode mode)
{
    auto* existingTransaction = row.GetTransaction();
    if (existingTransaction) {
        if (existingTransaction == transaction) {
            if (row.GetLockMode() != mode) {
                THROW_ERROR_EXCEPTION("Cannot change row lock mode from %Qlv to %Qlv",
                    row.GetLockMode(),
                    mode);
            }
        } else {
            THROW_ERROR_EXCEPTION("Row lock conflict")
                << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
                << TErrorAttribute("winner_transaction_id", existingTransaction->GetId())
                << TErrorAttribute("tablet_id", Tablet_->GetId())
                << TErrorAttribute("key", RowToKey(row));
        }
    }

    if (row.GetLastCommitTimestamp() >= transaction->GetStartTimestamp()) {
        THROW_ERROR_EXCEPTION("Row lock conflict")
            << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
            << TErrorAttribute("winner_transaction_commit_timestamp", row.GetLastCommitTimestamp())
            << TErrorAttribute("tablet_id", Tablet_->GetId())
            << TErrorAttribute("key", RowToKey(row));
    }
}

bool TDynamicMemoryStore::LockRow(
    TDynamicRow row,
    TTransaction* transaction,
    ERowLockMode mode,
    bool prelock)
{
    CheckRowLock(row, transaction, mode);

    if (row.GetLockMode() != ERowLockMode::None) {
        YASSERT(row.GetTransaction() == transaction);
        return false;
    }

    int lockIndex = TDynamicRow::InvalidLockIndex;
    if (!prelock) {
        lockIndex = static_cast<int>(transaction->LockedRows().size());
        transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    }
    
    Lock();
    row.Lock(transaction, lockIndex, mode);

    return true;
}

void TDynamicMemoryStore::DropUncommittedValues(TDynamicRow row)
{
    // Timestamps.
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        auto lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            if (timestampList.Pop() == 0) {
                row.SetTimestampList(timestampList.GetNext(), KeyColumnCount_);
            }
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyColumnCount_);
        if (list) {
            const auto& lastValue = list.Back();
            if ((lastValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                if (list.Pop() == 0) {
                    row.SetFixedValueList(index, list.GetNext(), KeyColumnCount_);
                }
            }
        }
    }
}

void TDynamicMemoryStore::AddFixedValue(
    TDynamicRow row,
    int listIndex,
    const TVersionedValue& value)
{
    auto list = row.GetFixedValueList(listIndex, KeyColumnCount_);

    if (list) {
        auto& lastValue = list.Back();
        if ((lastValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
            CaptureValue(&lastValue, value);
            return;
        }

        if (AllocateListForPushIfNeeded(&list, RowBuffer_.GetAlignedPool())) {
            row.SetFixedValueList(listIndex, list, KeyColumnCount_);
        }
    } else {
        list = TValueList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
        row.SetFixedValueList(listIndex, list, KeyColumnCount_);
    }

    list.Push([&] (TVersionedValue* dstValue) {
        CaptureValue(dstValue, value);
    });

    ++ValueCount_;
}

void TDynamicMemoryStore::AddUncommittedFixedValue(
    TDynamicRow row,
    int listIndex,
    const TUnversionedValue& value)
{
    AddFixedValue(row, listIndex, MakeVersionedValue(value, UncommittedTimestamp));
}

void TDynamicMemoryStore::AddTimestamp(TDynamicRow row, TTimestamp timestamp)
{
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        if (AllocateListForPushIfNeeded(&timestampList, RowBuffer_.GetAlignedPool())) {
            row.SetTimestampList(timestampList, KeyColumnCount_);
        }
    } else {
        timestampList = TTimestampList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
        row.SetTimestampList(timestampList, KeyColumnCount_);
    }
    timestampList.Push(timestamp);
}

void TDynamicMemoryStore::AddUncommittedTimestamp(TDynamicRow row, TTimestamp timestamp)
{
    YASSERT((timestamp & TimestampValueMask) == UncommittedTimestamp);

    bool pushTimestamp = false;
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        auto lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            YASSERT((timestamp & TombstoneTimestampMask) == (lastTimestamp & TombstoneTimestampMask));
        } else {
            if ((lastTimestamp & TombstoneTimestampMask) != (timestamp & TombstoneTimestampMask)) {
                pushTimestamp = true;
            }
        }
    } else {
        timestampList = TTimestampList::Allocate(RowBuffer_.GetAlignedPool(), InitialEditListCapacity);
        row.SetTimestampList(timestampList, KeyColumnCount_);
        pushTimestamp = true;
    }

    if (pushTimestamp) {
        if (AllocateListForPushIfNeeded(&timestampList, RowBuffer_.GetAlignedPool())) {
            row.SetTimestampList(timestampList, KeyColumnCount_);
        }
        timestampList.Push(timestamp);
    }
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
    if (src.Type == EValueType::String || src.Type == EValueType::Any) {
        dst->Data.String = RowBuffer_.GetUnalignedPool()->AllocateUnaligned(src.Length);
        memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
    }
}

int TDynamicMemoryStore::GetValueCount() const
{
    return ValueCount_;
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

TTimestamp TDynamicMemoryStore::GetLatestCommitTimestamp(TKey key)
{
    auto it = Rows_->FindEqualTo(key);
    if (!it.IsValid()) {
        return NullTimestamp;
    }

    auto row = it.GetCurrent();
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (!timestampList) {
        return NullTimestamp;
    }

    auto latestTimestamp = timestampList.Back() & TimestampValueMask;
    if (latestTimestamp != UncommittedTimestamp) {
        return latestTimestamp;
    }

    timestampList = timestampList.GetNext();
    if (!timestampList) {
        return NullTimestamp;
    }

    latestTimestamp = timestampList.Back() & TimestampValueMask;
    YASSERT(latestTimestamp != UncommittedTimestamp);
    return latestTimestamp;
}

void TDynamicMemoryStore::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, GetPersistentState());

    SmallVector<TValueList, TypicalEditListCount> valueLists;
    SmallVector<TTimestampList, TypicalEditListCount> timestampLists;

    // Rows
    Save(context, Rows_->GetSize());
    for (auto rowIt = Rows_->FindGreaterThanOrEqualTo(MinKey()); rowIt.IsValid(); rowIt.MoveNext()) {
        auto row = rowIt.GetCurrent();

        const auto* keyBegin = row.GetKeys();
        const auto* keyEnd = keyBegin + KeyColumnCount_;
        for (const auto* keyIt = keyBegin; keyIt != keyEnd; ++keyIt) {
            NVersionedTableClient::Save(context, *keyIt);
        }

        for (int listIndex = 0; listIndex < SchemaColumnCount_ - KeyColumnCount_; ++listIndex) {
            auto topList = row.GetFixedValueList(listIndex, KeyColumnCount_);
            if (topList) {
                int valueCount = topList.GetSize() + topList.GetSuccessorsSize();
                if (topList && (topList.Back().Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    --valueCount;
                }
                Save(context, valueCount);

                EnumerateListsAndReverse(topList, &valueLists);
                for (auto list : valueLists) {
                    for (const auto* valueIt = list.Begin(); valueIt != list.End(); ++valueIt) {
                        const auto& value = *valueIt;
                        if ((value.Timestamp & TimestampValueMask) != UncommittedTimestamp) {
                            NVersionedTableClient::Save(context, *valueIt);
                        }
                    }
                }
            } else {
                Save(context, static_cast<int>(0));
            }
        }

        {
            auto topList = row.GetTimestampList(KeyColumnCount_);
            if (topList) {
                int timestampCount = topList.GetSize() + topList.GetSuccessorsSize();
                if (topList && (topList.Back() & TimestampValueMask) == UncommittedTimestamp) {
                    --timestampCount;
                }
                Save(context, timestampCount);

                EnumerateListsAndReverse(topList, &timestampLists);
                for (auto list : timestampLists) {
                    for (const auto* timestampIt = list.Begin(); timestampIt != list.End(); ++timestampIt) {
                        auto timestamp = *timestampIt;
                        if ((timestamp & TimestampValueMask) != UncommittedTimestamp) {
                            Save(context, timestamp);
                        }
                    }
                }
            } else {
                Save(context, static_cast<int>(0));
            }
        }
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

        auto* keyBegin = row.GetKeys();
        auto* keyEnd = keyBegin + KeyColumnCount_;
        for (auto* keyIt = keyBegin; keyIt != keyEnd; ++keyIt) {
            NVersionedTableClient::Load(context, *keyIt, RowBuffer_.GetUnalignedPool());
        }

        for (int listIndex = 0; listIndex < SchemaColumnCount_ - KeyColumnCount_; ++listIndex) {
            int valueCount = Load<int>(context);
            for (int valueIndex = 0; valueIndex < valueCount; ++valueIndex) {
                TVersionedValue value;
                NVersionedTableClient::Load(context, value, RowBuffer_.GetUnalignedPool());
                AddFixedValue(row, listIndex, value);
            }
        }

        {
            int timestampCount = Load<int>(context);
            for (int timestampIndex = 0; timestampIndex < timestampCount; ++timestampIndex) {
                auto timestamp = Load<TTimestamp>(context);
                AddTimestamp(row, timestamp);
            }
        }

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
    for (const auto* it = row.GetKeys(); it != row.GetKeys() + KeyColumnCount_; ++it) {
        builder.AddValue(*it);
    }
    return builder.GetRowAndReset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
