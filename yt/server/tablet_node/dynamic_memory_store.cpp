#include "stdafx.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"

#include <core/misc/small_vector.h>
#include <core/misc/skip_list.h>

#include <core/concurrency/scheduler.h>

#include <core/profiling/timing.h>

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/table_client/name_table.h>
#include <ytlib/table_client/versioned_row.h>
#include <ytlib/table_client/versioned_reader.h>
#include <ytlib/table_client/versioned_writer.h>
#include <ytlib/table_client/versioned_chunk_reader.h>
#include <ytlib/table_client/versioned_chunk_writer.h>
#include <ytlib/table_client/cached_versioned_chunk_meta.h>

#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const int InitialEditListCapacity = 2;
static const int EditListCapacityMultiplier = 2;
static const int MaxEditListCapacity = 256;
static const int TabletReaderPoolSize = 16 * 1024;
static const int SnapshotRowsPerRead = 1024;

struct TDynamicMemoryStoreReaderPoolTag
{ };

////////////////////////////////////////////////////////////////////////////////

namespace {

ui32 ExtractRevision(ui32 revision)
{
    return revision;
}

ui32 ExtractRevision(const TDynamicValue& value)
{
    return value.Revision;
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TReaderBase
{
public:
    explicit TReaderBase(
        TDynamicMemoryStorePtr store,
        TTimestamp timestamp,
        ui32 revision,
        const TColumnFilter& columnFilter)
        : Store_(std::move(store))
        , Timestamp_(timestamp)
        , Revision_(revision)
        , ColumnFilter_(columnFilter)
        , KeyColumnCount_(Store_->KeyColumnCount_)
        , SchemaColumnCount_(Store_->SchemaColumnCount_)
        , ColumnLockCount_(Store_->ColumnLockCount_)
        , Pool_(TDynamicMemoryStoreReaderPoolTag(), TabletReaderPoolSize)
    {
        YCHECK(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.All);

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
    const TDynamicMemoryStorePtr Store_;
    const TTimestamp Timestamp_;
    const ui32 Revision_;
    const TColumnFilter ColumnFilter_;

    int KeyColumnCount_;
    int SchemaColumnCount_;
    int ColumnLockCount_;

    TChunkedMemoryPool Pool_;

    std::vector<TTimestamp> DeleteTimestamps_;
    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TVersionedValue> VersionedValues_;

    ui32 LockMask_;


    TTimestamp GetLatestWriteTimestamp(TDynamicRow dynamicRow)
    {
        auto* lock = dynamicRow.BeginLocks(KeyColumnCount_);
        auto maxTimestamp = NullTimestamp;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            auto list = TDynamicRow::GetWriteRevisionList(*lock);
            const auto* revisionPtr = SearchByTimestamp(list, Timestamp_);
            if (revisionPtr) {
                auto timestamp = Store_->TimestampFromRevision(*revisionPtr);
                maxTimestamp = std::max(maxTimestamp, timestamp);
            }
        }
        return maxTimestamp;
    }

    TTimestamp GetLatestDeleteTimestamp(TDynamicRow dynamicRow)
    {
        auto list = dynamicRow.GetDeleteRevisionList(KeyColumnCount_, ColumnLockCount_);
        const auto* revisionPtr = SearchByTimestamp(list, Timestamp_);
        return  revisionPtr ? Store_->TimestampFromRevision(*revisionPtr) : NullTimestamp;
    }


    TVersionedRow ProduceSingleRowVersion(TDynamicRow dynamicRow)
    {
        Store_->WaitOnBlockedRow(dynamicRow, LockMask_, Timestamp_);

        auto latestWriteTimestamp = GetLatestWriteTimestamp(dynamicRow);
        auto latestDeleteTimestamp = GetLatestDeleteTimestamp(dynamicRow);

        if (latestWriteTimestamp == NullTimestamp && latestDeleteTimestamp == NullTimestamp) {
            return TVersionedRow();
        }

        int writeTimestampCount = 1;
        int deleteTimestampCount = 1;
        int valueCount = SchemaColumnCount_ - KeyColumnCount_; // an upper bound

        if (latestDeleteTimestamp == NullTimestamp) {
            deleteTimestampCount = 0;
        } else if (latestDeleteTimestamp > latestWriteTimestamp) {
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
            versionedRow.BeginWriteTimestamps()[0] = latestWriteTimestamp;
        }

        if (deleteTimestampCount > 0) {
            versionedRow.BeginDeleteTimestamps()[0] = latestDeleteTimestamp;
        }

        if (valueCount > 0) {
            auto* currentRowValue = versionedRow.BeginValues();
            auto fillValue = [&] (int index) {
                auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                // NB: Inserting a new item into value list and adding a new write revision cannot
                // be done atomically. We always append values before revisions but in the middle of these
                // two steps there might be "phantom" values present in the row.
                // To work this around, we cap the value lists by #latestWriteTimestamp to make sure that
                // no "phantom" value is listed.
                const auto* value = SearchByTimestamp(list, latestWriteTimestamp);
                if (value && Store_->TimestampFromRevision(value->Revision) > latestDeleteTimestamp) {
                    ProduceVersionedValue(currentRowValue, index, *value);
                    ++currentRowValue;
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
        // Prepare values and write timestamps.
        VersionedValues_.clear();
        WriteTimestamps_.clear();
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            for (auto list = dynamicRow.GetFixedValueList(columnIndex, KeyColumnCount_, ColumnLockCount_);
                 list;
                 list = list.GetSuccessor())
            {
                for (int itemIndex = list.GetSize() - 1; itemIndex >= 0; --itemIndex) {
                    const auto& value = list[itemIndex];
                    ui32 revision = value.Revision;
                    if (revision <= Revision_) {
                        VersionedValues_.push_back(TVersionedValue());
                        ProduceVersionedValue(&VersionedValues_.back(), columnIndex, value);
                        WriteTimestamps_.push_back(Store_->TimestampFromRevision(revision));
                    }
                }
            }
        }
        std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end(), std::greater<TTimestamp>());
        WriteTimestamps_.erase(
            std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
            WriteTimestamps_.end());

        // Prepare delete timestamps.
        DeleteTimestamps_.clear();
        for (auto list = dynamicRow.GetDeleteRevisionList(KeyColumnCount_, ColumnLockCount_);
             list;
             list = list.GetSuccessor())
        {
            for (int itemIndex = list.GetSize() - 1; itemIndex >= 0; --itemIndex) {
                ui32 revision = list[itemIndex];
                if (revision <= Revision_) {
                    DeleteTimestamps_.push_back(Store_->TimestampFromRevision(revision));
                    YASSERT(DeleteTimestamps_.size() == 1 ||
                            DeleteTimestamps_[DeleteTimestamps_.size() - 1] < DeleteTimestamps_[DeleteTimestamps_.size() - 2]);
                }
            }
        }

        if (WriteTimestamps_.empty() && DeleteTimestamps_.empty()) {
            return TVersionedRow();
        }

        auto versionedRow = TVersionedRow::Allocate(
            &Pool_,
            KeyColumnCount_,
            VersionedValues_.size(),
            WriteTimestamps_.size(),
            DeleteTimestamps_.size());

        // Keys.
        ProduceKeys(dynamicRow, versionedRow.BeginKeys());

        // Timestamps (sorted in descending order).
        ::memcpy(versionedRow.BeginWriteTimestamps(), WriteTimestamps_.data(), sizeof (TTimestamp) * WriteTimestamps_.size());
        ::memcpy(versionedRow.BeginDeleteTimestamps(), DeleteTimestamps_.data(), sizeof (TTimestamp) * DeleteTimestamps_.size());

        // Values.
        ::memcpy(versionedRow.BeginValues(), VersionedValues_.data(), sizeof (TVersionedValue) * VersionedValues_.size());

        return versionedRow;
    }

    void ProduceKeys(TDynamicRow dynamicRow, TUnversionedValue* dstKey)
    {
        ui32 nullKeyMask = dynamicRow.GetNullKeyMask();
        ui32 nullKeyBit = 1;
        const auto* srcKey = dynamicRow.BeginKeys();
        for (int index = 0;
             index < KeyColumnCount_;
             ++index, nullKeyBit <<= 1, ++srcKey, ++dstKey)
        {
            ProduceUnversionedValue(dstKey, index, *srcKey, (nullKeyMask & nullKeyBit) != 0);
        }
    }

    void ProduceUnversionedValue(TUnversionedValue* dstValue, int index, TDynamicValueData srcData, bool null)
    {
        dstValue->Id = index;
        if (null) {
            dstValue->Type = EValueType::Null;
        } else {
            dstValue->Type = Store_->Schema_.Columns()[index].Type;
            if (IsStringLikeType(dstValue->Type)) {
                dstValue->Length = srcData.String->Length;
                dstValue->Data.String = srcData.String->Data;
            } else {
                ::memcpy(&dstValue->Data, &srcData, sizeof(TDynamicValueData));
            }
        }
    }

    void ProduceVersionedValue(TVersionedValue* dstValue, int index, const TDynamicValue& srcValue)
    {
        ProduceUnversionedValue(dstValue, index, srcValue.Data, srcValue.Null);
        dstValue->Timestamp = Store_->TimestampFromRevision(srcValue.Revision);
    }


    template <class T>
    T* SearchByTimestamp(TEditList<T> list, TTimestamp maxTimestamp)
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
                if (!list.IsEmpty() && Store_->TimestampFromRevision(ExtractRevision(list[0])) <= maxTimestamp) {
                    break;
                }
                list = list.GetSuccessor();
            }

            if (!list) {
                return nullptr;
            }

            auto* left = list.Begin();
            auto* right = list.End();
            if (left == right) {
                return nullptr;
            }

            while (right - left > 1) {
                auto* mid = left + (right - left) / 2;
                if (Store_->TimestampFromRevision(ExtractRevision(*mid)) <= maxTimestamp) {
                    left = mid;
                } else {
                    right = mid;
                }
            }

            YASSERT(Store_->TimestampFromRevision(ExtractRevision(*left)) <= maxTimestamp);
            return left;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TRangeReader
    : public TReaderBase
    , public IVersionedReader
{
public:
    TRangeReader(
        TDynamicMemoryStorePtr store,
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        ui32 revision,
        const TColumnFilter& columnFilter)
        : TReaderBase(
            std::move(store),
            timestamp,
            revision,
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

        const auto& keyComparer = Store_->GetRowKeyComparer();

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

        Store_->PerformanceCounters_->DynamicMemoryRowReadCount += rows->size();

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

private:
    const TOwningKey LowerKey_;
    const TOwningKey UpperKey_;

    TSkipList<TDynamicRow, TDynamicRowKeyComparer>::TIterator Iterator_;

    bool Finished_ = false;


    TVersionedRow ProduceRow()
    {
        auto dynamicRow = Iterator_.GetCurrent();
        return Timestamp_ == AllCommittedTimestamp
            ? ProduceAllRowVersions(dynamicRow)
            : ProduceSingleRowVersion(dynamicRow);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TLookupReader
    : public TReaderBase
    , public IVersionedReader
{
public:
    TLookupReader(
        TDynamicMemoryStorePtr store,
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : TReaderBase(
            std::move(store),
            timestamp,
            MaxRevision,
            columnFilter)
        , Keys_(keys)
    { }

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        YASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        if (Finished_) {
            return false;
        }

        while (rows->size() < rows->capacity()) {
            if (RowCount_ == Keys_.Size())
                break;

            auto iterator = Store_->Rows_->FindEqualTo(TKeyWrapper{Keys_[RowCount_]});
            if (iterator.IsValid()) {
                auto row = ProduceSingleRowVersion(iterator.GetCurrent());
                rows->push_back(row);
            } else {
                rows->push_back(TVersionedRow());
            }

            ++RowCount_;
        }

        if (rows->empty()) {
            Finished_ = true;
            return false;
        }

        Store_->PerformanceCounters_->DynamicMemoryRowLookupCount += rows->size();

        return true;
    }

private:
    const TSharedRange<TKey> Keys_;
    i64 RowCount_  = 0;
    bool Finished_ = false;

};

////////////////////////////////////////////////////////////////////////////////

TDynamicMemoryStore::TDynamicMemoryStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(
        id,
        tablet)
    , FlushState_(EStoreFlushState::None)
    , Config_(config)
    , RowKeyComparer_(tablet->GetRowKeyComparer())
    , RowBuffer_(New<TRowBuffer>(
        Config_->PoolChunkSize,
        Config_->MaxPoolSmallBlockRatio))
    , Rows_(new TSkipList<TDynamicRow, TDynamicRowKeyComparer>(
        RowBuffer_->GetPool(),
        RowKeyComparer_))
{
    StoreState_ = EStoreState::ActiveDynamic;

    // Reserve the vector to prevent reallocations and thus enable accessing
    // it from arbitrary threads.
    RevisionToTimestamp_.ReserveChunks(MaxRevisionChunks);
    RevisionToTimestamp_.PushBack(UncommittedTimestamp);
    YCHECK(TimestampFromRevision(UncommittedRevision) == UncommittedTimestamp);

    LOG_DEBUG("Dynamic memory store created (TabletId: %v)",
        TabletId_);
}

TDynamicMemoryStore::~TDynamicMemoryStore()
{
    LOG_DEBUG("Dynamic memory store destroyed");
}

IVersionedReaderPtr TDynamicMemoryStore::CreateFlushReader()
{
    YCHECK(StoreState_ == EStoreState::PassiveDynamic);
    return New<TRangeReader>(
        this,
        MinKey(),
        MaxKey(),
        AllCommittedTimestamp,
        FlushRevision_,
        TColumnFilter());
}

const TDynamicRowKeyComparer& TDynamicMemoryStore::GetRowKeyComparer() const
{
    return RowKeyComparer_;
}

int TDynamicMemoryStore::GetLockCount() const
{
    return StoreLockCount_;
}

int TDynamicMemoryStore::Lock()
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

    int result = ++StoreLockCount_;
    LOG_TRACE("Store locked (Count: %v)",
        result);
    return result;
}

int TDynamicMemoryStore::Unlock()
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);
    YASSERT(StoreLockCount_ > 0);

    int result = --StoreLockCount_;
    LOG_TRACE("Store unlocked (Count: %v)",
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
    if (Tablet_->GetAtomicity() == EAtomicity::None)
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

TDynamicRow TDynamicMemoryStore::WriteRowAtomic(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock,
    ui32 lockMask)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);
    YASSERT(lockMask != 0);

    TDynamicRow result;

    auto addValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            auto list = PrepareFixedValue(dynamicRow, value.Id);
            auto& uncommittedValue = list.GetUncommitted();
            uncommittedValue.Revision = UncommittedRevision;
            CaptureUnversionedValue(&uncommittedValue, value);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row.Begin());

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

    ++PerformanceCounters_->DynamicMemoryRowWriteCount;

    return result;
}

TDynamicRow TDynamicMemoryStore::WriteRowNonAtomic(
    TUnversionedRow row,
    TTimestamp commitTimestamp)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::None);

    TDynamicRow result;

    ui32 commitRevision = RegisterRevision(commitTimestamp);

    auto addValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            auto list = PrepareFixedValue(dynamicRow, value.Id);
            auto& uncommittedValue = list.GetUncommitted();
            uncommittedValue.Revision = commitRevision;
            CaptureUnversionedValue(&uncommittedValue, value);
            list.Commit();
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row.Begin());

        // Copy values.
        addValues(dynamicRow);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Copy values.
        addValues(dynamicRow);

        result = dynamicRow;
    };

    Rows_->Insert(TRowWrapper{row}, newKeyProvider, existingKeyConsumer);

    AddWriteRevisionNonAtomic(result, commitTimestamp, commitRevision);

    OnMemoryUsageUpdated();

    ++PerformanceCounters_->DynamicMemoryRowWriteCount;

    return result;
}

TDynamicRow TDynamicMemoryStore::DeleteRowAtomic(
    TTransaction* transaction,
    NTableClient::TKey key,
    bool prelock)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

    TDynamicRow result;

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, key.Begin());

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

    ++PerformanceCounters_->DynamicMemoryRowDeleteCount;

    return result;
}

TDynamicRow TDynamicMemoryStore::DeleteRowNonAtomic(
    NTableClient::TKey key,
    TTimestamp commitTimestamp)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::None);

    ui32 commitRevision = RegisterRevision(commitTimestamp);

    TDynamicRow result;

    auto newKeyProvider = [&] () -> TDynamicRow {
        YASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, key.Begin());

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        result = dynamicRow;
    };

    Rows_->Insert(TRowWrapper{key}, newKeyProvider, existingKeyConsumer);

    AddDeleteRevisionNonAtomic(result, commitTimestamp, commitRevision);

    UpdateTimestampRange(commitTimestamp);

    OnMemoryUsageUpdated();

    ++PerformanceCounters_->DynamicMemoryRowDeleteCount;

    return result;
}

TDynamicRow TDynamicMemoryStore::MigrateRow(TTransaction* transaction, TDynamicRow row)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

    auto migrateLocksAndValues = [&] (TDynamicRow migratedRow) {
        auto* locks = row.BeginLocks(KeyColumnCount_);
        auto* migratedLocks = migratedRow.BeginLocks(KeyColumnCount_);

        // Migrate locks.
        {
            const auto* lock = locks;
            auto* migratedLock = migratedLocks;
            for (int index = 0; index < ColumnLockCount_; ++index, ++lock, ++migratedLock) {
                if (lock->Transaction == transaction) {
                    // Validate the original lock's sanity.
                    // NB: For simple transaction transacton may not go through preparation stage
                    // during recovery.
                    YASSERT(
                        transaction->GetPrepareTimestamp() == NullTimestamp ||
                        lock->PrepareTimestamp == transaction->GetPrepareTimestamp());

                    // Validate the mirgated lock's sanity.
                    YASSERT(!migratedLock->Transaction);
                    YASSERT(migratedLock->PrepareTimestamp == NotPreparedTimestamp);

                    migratedLock->Transaction = lock->Transaction;
                    migratedLock->PrepareTimestamp = lock->PrepareTimestamp;
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
                    auto migratedList = PrepareFixedValue(migratedRow, columnIndex);
                    CaptureUncommittedValue(&migratedList.GetUncommitted(), list.GetUncommitted(), columnIndex);
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
        SetKeys(migratedRow, row);

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
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

    transaction->LockedRows().push_back(TDynamicRowRef(this, row));
}

void TDynamicMemoryStore::PrepareRow(TTransaction* transaction, TDynamicRow row)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

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
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

    auto commitTimestamp = transaction->GetCommitTimestamp();
    ui32 commitRevision = RegisterRevision(commitTimestamp);

    auto* locks = row.BeginLocks(KeyColumnCount_);

    if (row.GetDeleteLockFlag()) {
        AddDeleteRevision(row, commitRevision);
    } else {
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            auto& lock = locks[ColumnIndexToLockIndex_[index]];
            if (lock.Transaction == transaction) {
                auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                if (list.HasUncommitted()) {
                    list.GetUncommitted().Revision = commitRevision;
                    list.Commit();
                }
            }
        }
    }

    // NB: Add write timestamp _after_ the values are committed.
    // See remarks in TReaderBase.
    {
        auto* lock = locks;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->Transaction = nullptr;
                lock->PrepareTimestamp = NotPreparedTimestamp;
                if (!row.GetDeleteLockFlag()) {
                    AddWriteRevision(*lock, commitRevision);
                }
            }
        }
    }

    row.SetDeleteLockFlag(false);

    Unlock();

    UpdateTimestampRange(commitTimestamp);
}

void TDynamicMemoryStore::AbortRow(TTransaction* transaction, TDynamicRow row)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

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

TDynamicRow TDynamicMemoryStore::FindRow(TKey key)
{
    auto it = Rows_->FindEqualTo(TKeyWrapper{key});
    return it.IsValid() ? it.GetCurrent() : TDynamicRow();
}

std::vector<TDynamicRow> TDynamicMemoryStore::GetAllRows()
{
    std::vector<TDynamicRow> rows;
    for (auto it = Rows_->FindGreaterThanOrEqualTo(TKeyWrapper{MinKey().Get()});
         it.IsValid();
         it.MoveNext())
    {
        rows.push_back(it.GetCurrent());
    }
    return rows;
}

TDynamicRow TDynamicMemoryStore::AllocateRow()
{
    return TDynamicRow::Allocate(
        RowBuffer_->GetPool(),
        KeyColumnCount_,
        ColumnLockCount_,
        SchemaColumnCount_);
}

int TDynamicMemoryStore::GetBlockingLockIndex(
    TDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

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

TTimestamp TDynamicMemoryStore::GetLastCommitTimestamp(
    TDynamicRow row,
    int lockIndex)
{
    auto timestamp = MinTimestamp;
    auto& lock = row.BeginLocks(KeyColumnCount_)[lockIndex];
    auto writeRevisionList = TDynamicRow::GetWriteRevisionList(lock);
    if (writeRevisionList) {
        int size = writeRevisionList.GetSize();
        if (size > 0) {
            timestamp = TimestampFromRevision(writeRevisionList[size - 1]);
        }
    }
    if (lockIndex == TDynamicRow::PrimaryLockIndex) {
        auto deleteRevisionList = row.GetDeleteRevisionList(KeyColumnCount_, ColumnLockCount_);
        if (deleteRevisionList) {
            int size = deleteRevisionList.GetSize();
            if (size > 0) {
                timestamp = std::max(timestamp, TimestampFromRevision(deleteRevisionList[size - 1]));
            }
        }
    }
    return timestamp;
}

void TDynamicMemoryStore::CheckRowLocks(
    TDynamicRow row,
    TTransaction* transaction,
    ui32 lockMask)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

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
            auto lastCommitTimestamp = GetLastCommitTimestamp(row, index);
            if (lastCommitTimestamp > transaction->GetStartTimestamp()) {
                THROW_ERROR_EXCEPTION("Row lock conflict")
                    << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_commit_timestamp", lastCommitTimestamp)
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
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::Full);

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

TValueList TDynamicMemoryStore::PrepareFixedValue(TDynamicRow row, int index)
{
    YASSERT(index >= KeyColumnCount_ && index < SchemaColumnCount_);

    auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        row.SetFixedValueList(index, list, KeyColumnCount_, ColumnLockCount_);
    }
    ++StoreValueCount_;
    list.Prepare();
    return list;
}

void TDynamicMemoryStore::AddDeleteRevision(TDynamicRow row, ui32 revision)
{
    auto list = row.GetDeleteRevisionList(KeyColumnCount_, ColumnLockCount_);
    YASSERT(!list || TimestampFromRevision(list.Back()) < TimestampFromRevision(revision));
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        row.SetDeleteRevisionList(list, KeyColumnCount_, ColumnLockCount_);
    }
    list.Push(revision);
}

void TDynamicMemoryStore::AddWriteRevision(TLockDescriptor& lock, ui32 revision)
{
    auto list = TDynamicRow::GetWriteRevisionList(lock);
    YASSERT(!list || TimestampFromRevision(list.Back()) < TimestampFromRevision(revision));
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        TDynamicRow::SetWriteRevisionList(lock, list);
    }
    list.Push(revision);
}

void TDynamicMemoryStore::AddDeleteRevisionNonAtomic(
    TDynamicRow row,
    TTimestamp commitTimestamp,
    ui32 commitRevision)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::None);

    AddDeleteRevision(row, commitRevision);
    UpdateTimestampRange(commitTimestamp);
}

void TDynamicMemoryStore::AddWriteRevisionNonAtomic(
    TDynamicRow row,
    TTimestamp commitTimestamp,
    ui32 commitRevision)
{
    YASSERT(Tablet_->GetAtomicity() == EAtomicity::None);

    auto& lock = row.BeginLocks(KeyColumnCount_)[TDynamicRow::PrimaryLockIndex];
    AddWriteRevision(lock, commitRevision);
    UpdateTimestampRange(commitTimestamp);
}

void TDynamicMemoryStore::SetKeys(TDynamicRow dstRow, TUnversionedValue* srcKeys)
{
    ui32 nullKeyMask = 0;
    ui32 nullKeyBit = 1;
    auto* dstValue = dstRow.BeginKeys();
    auto columnIt = Schema_.Columns().begin();
    for (int index = 0;
         index < KeyColumnCount_;
         ++index, nullKeyBit <<= 1, ++dstValue, ++columnIt)
    {
        const auto& srcValue = srcKeys[index];
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
    dstRow.SetNullKeyMask(nullKeyMask);
}

void TDynamicMemoryStore::SetKeys(TDynamicRow dstRow, TDynamicRow srcRow)
{
    ui32 nullKeyMask = srcRow.GetNullKeyMask();
    dstRow.SetNullKeyMask(nullKeyMask);
    ui32 nullKeyBit = 1;
    const auto* srcKeys = srcRow.BeginKeys();
    auto* dstKeys = dstRow.BeginKeys();
    auto columnIt = Schema_.Columns().begin();
    for (int index = 0;
         index < KeyColumnCount_;
         ++index, nullKeyBit <<= 1, ++srcKeys, ++dstKeys, ++columnIt)
    {
        if (!(nullKeyMask & nullKeyBit) && IsStringLikeType(columnIt->Type)) {
            *dstKeys = CaptureStringValue(*srcKeys);
        } else {
            *dstKeys = *srcKeys;
        }
    }
}

void TDynamicMemoryStore::LoadRow(
    TVersionedRow row,
    TLoadScratchData* scratchData)
{
    YASSERT(row.GetKeyCount() == KeyColumnCount_);

    auto dynamicRow = AllocateRow();

    SetKeys(dynamicRow, row.BeginKeys());

    for (auto& revisions : scratchData->WriteRevisions) {
        revisions.clear();
    }

    const auto* currentValue = row.BeginValues();
    while (currentValue != row.EndValues()) {
        const auto* beginValue = currentValue;
        const auto* endValue = beginValue;
        int index = beginValue->Id;
        while (endValue != row.EndValues() && endValue->Id == index) {
            ++endValue;
        }

        int lockIndex = ColumnIndexToLockIndex_[index];
        // Values are ordered by descending timestamps but we need ascending ones here.
        for (const auto* value = endValue - 1; value >= beginValue; --value) {
            auto list = PrepareFixedValue(dynamicRow, index);
            ui32 revision = CaptureVersionedValue(&list.GetUncommitted(), *value, scratchData);
            list.Commit();
            scratchData->WriteRevisions[lockIndex].push_back(revision);
        }

        currentValue = endValue;
    }

    auto* locks = dynamicRow.BeginLocks(KeyColumnCount_);
    const auto& primaryRevisions = scratchData->WriteRevisions[TDynamicRow::PrimaryLockIndex];
    for (int lockIndex = 0; lockIndex < ColumnLockCount_; ++lockIndex) {
        auto& lock = locks[lockIndex];
        auto& revisions = scratchData->WriteRevisions[lockIndex];
        // NB: Taking the primary lock implies taking all other locks.
        if (lockIndex != TDynamicRow::PrimaryLockIndex) {
            revisions.insert(
                revisions.end(),
                primaryRevisions.begin(),
                primaryRevisions.end());
        }
        if (!revisions.empty()) {
            std::sort(
                revisions.begin(),
                revisions.end(),
                [&] (ui32 lhs, ui32 rhs) {
                    return TimestampFromRevision(lhs) < TimestampFromRevision(rhs);
                });
            revisions.erase(
                std::unique(revisions.begin(), revisions.end()),
                revisions.end());
            for (ui32 revision : revisions) {
                AddWriteRevision(lock, revision);
            }
        }
    }

    // Delete timestamps are also in descending order.
    if (row.BeginDeleteTimestamps() != row.EndDeleteTimestamps()) {
        for (const auto* currentTimestamp = row.EndDeleteTimestamps() - 1;
             currentTimestamp >= row.BeginDeleteTimestamps();
             --currentTimestamp)
        {
            ui32 revision = CaptureTimestamp(*currentTimestamp, scratchData);
            AddDeleteRevision(dynamicRow, revision);
        }
    }

    Rows_->Insert(dynamicRow);
}

ui32 TDynamicMemoryStore::CaptureTimestamp(
    TTimestamp timestamp,
    TLoadScratchData* scratchData)
{
    auto& timestampToRevision = scratchData->TimestampToRevision;
    auto it = timestampToRevision.find(timestamp);
    if (it == timestampToRevision.end()) {
        ui32 revision = RegisterRevision(timestamp);
        YCHECK(timestampToRevision.insert(std::make_pair(timestamp, revision)).second);
        return revision;
    } else {
        return it->second;
    }
}

ui32 TDynamicMemoryStore::CaptureVersionedValue(
    TDynamicValue* dst,
    const TVersionedValue& src,
    TLoadScratchData* scratchData)
{
    YASSERT(src.Type == EValueType::Null || src.Type == Schema_.Columns()[src.Id].Type);
    ui32 revision = CaptureTimestamp(src.Timestamp, scratchData);
    dst->Revision = revision;
    CaptureUnversionedValue(dst, src);
    return revision;
}

void TDynamicMemoryStore::CaptureUncommittedValue(TDynamicValue* dst, const TDynamicValue& src, int index)
{
    YASSERT(index >= KeyColumnCount_ && index < SchemaColumnCount_);
    YASSERT(src.Revision == UncommittedRevision);

    *dst = src;
    if (!src.Null && IsStringLikeType(Schema_.Columns()[index].Type)) {
        dst->Data = CaptureStringValue(src.Data);
    }
}

void TDynamicMemoryStore::CaptureUnversionedValue(
    TDynamicValue* dst,
    const TUnversionedValue& src)
{
    YASSERT(src.Type == EValueType::Null || src.Type == Schema_.Columns()[src.Id].Type);

    if (src.Type == EValueType::Null) {
        dst->Null = true;
        return;
    }

    dst->Null = false;

    if (IsStringLikeType(src.Type)) {
        dst->Data = CaptureStringValue(src);
    } else {
        ::memcpy(&dst->Data, &src.Data, sizeof(TDynamicValueData));
    }
}

TDynamicValueData TDynamicMemoryStore::CaptureStringValue(TDynamicValueData src)
{
    ui32 length = src.String->Length;
    TDynamicValueData dst;
    dst.String = reinterpret_cast<TDynamicString*>(RowBuffer_->GetPool()->AllocateAligned(
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
    dst.String = reinterpret_cast<TDynamicString*>(RowBuffer_->GetPool()->AllocateAligned(
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

i64 TDynamicMemoryStore::GetPoolSize() const
{
    return RowBuffer_->GetSize();
}

i64 TDynamicMemoryStore::GetPoolCapacity() const
{
    return RowBuffer_->GetCapacity();
}

EStoreType TDynamicMemoryStore::GetType() const
{
    return EStoreType::DynamicMemory;
}

void TDynamicMemoryStore::SetStoreState(EStoreState state)
{
    if (StoreState_ == EStoreState::ActiveDynamic && state == EStoreState::PassiveDynamic) {
        YCHECK(FlushRevision_ == InvalidRevision);
        FlushRevision_ = GetLatestRevision();
    }
    TStoreBase::SetStoreState(state);
}

i64 TDynamicMemoryStore::GetUncompressedDataSize() const
{
    return GetPoolCapacity();
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
    YCHECK(timestamp != AllCommittedTimestamp);
    return New<TRangeReader>(
        this,
        std::move(lowerKey),
        std::move(upperKey),
        timestamp,
        timestamp == AllCommittedTimestamp ? GetLatestRevision() : MaxRevision,
        columnFilter);
}

IVersionedReaderPtr TDynamicMemoryStore::CreateReader(
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    YCHECK(timestamp != AllCommittedTimestamp);
    return New<TLookupReader>(
        this,
        keys,
        timestamp,
        columnFilter);
}

void TDynamicMemoryStore::CheckRowLocks(
    TUnversionedRow row,
    TTransaction* transaction,
    ui32 lockMask)
{
    auto it = Rows_->FindEqualTo(TRowWrapper{row});
    if (!it.IsValid())
        return;

    auto dynamicRow = it.GetCurrent();
    CheckRowLocks(dynamicRow, transaction, lockMask);
}

void TDynamicMemoryStore::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);

    using NYT::Save;
    Save(context, FlushRevision_);
    Save(context, MinTimestamp_);
    Save(context, MaxTimestamp_);
}

void TDynamicMemoryStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);

    using NYT::Load;
    Load(context, FlushRevision_);
    Load(context, MinTimestamp_);
    Load(context, MaxTimestamp_);
}

TCallback<void(TSaveContext& context)> TDynamicMemoryStore::AsyncSave()
{
    auto tableReader = CreateReader(
        MinKey(),
        MaxKey(),
        AllCommittedTimestamp,
        TColumnFilter());

    return BIND([=, this_ = MakeStrong(this)] (TSaveContext& context) {
        WaitFor(tableReader->Open())
            .ThrowOnError();

        auto chunkWriter = New<TMemoryWriter>();
        WaitFor(chunkWriter->Open())
            .ThrowOnError();

        auto tableWriterConfig = New<TChunkWriterConfig>();
        auto tableWriterOptions = New<TTabletWriterOptions>();
        auto tableWriter = CreateVersionedChunkWriter(
            tableWriterConfig,
            tableWriterOptions,
            Schema_,
            KeyColumns_,
            chunkWriter);
        WaitFor(tableWriter->Open())
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(SnapshotRowsPerRead);

        while (tableReader->Read(&rows)) {
            if (rows.empty()) {
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            if (!tableWriter->Write(rows)) {
                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        using NYT::Save;

        // pushsin@ forbids empty chunks.
        if (tableWriter->GetRowCount() == 0) {
            Save(context, false);
        }  else {
            Save(context, true);

            // NB: This also closes chunkWriter.
            WaitFor(tableWriter->Close())
                .ThrowOnError();

            Save(context, chunkWriter->GetChunkMeta());
            Save(context, chunkWriter->GetBlocks());
        }
    });
}

void TDynamicMemoryStore::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    if (Load<bool>(context)) {
        auto chunkMeta = Load<TChunkMeta>(context);
        auto blocks = Load<std::vector<TSharedRef>>(context);

        auto chunkReader = CreateMemoryReader(chunkMeta, blocks);

        auto asyncCachedMeta = TCachedVersionedChunkMeta::Load(chunkReader, Schema_, KeyColumns_);
        auto cachedMeta = WaitFor(asyncCachedMeta)
            .ValueOrThrow();

        auto tableReaderConfig = New<TTabletChunkReaderConfig>();
        auto tableReader = CreateVersionedChunkReader(
            tableReaderConfig,
            chunkReader,
            GetNullBlockCache(),
            cachedMeta,
            NChunkClient::TReadLimit(),
            NChunkClient::TReadLimit(),
            TColumnFilter(),
            New<TChunkReaderPerformanceCounters>(),
            AllCommittedTimestamp);
        WaitFor(tableReader->Open())
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(SnapshotRowsPerRead);

        TLoadScratchData scratchData;
        scratchData.WriteRevisions.resize(ColumnLockCount_);

        while (tableReader->Read(&rows)) {
            if (rows.empty()) {
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto row : rows) {
                LoadRow(row, &scratchData);
            }
        }
    }

    OnMemoryUsageUpdated();
}

void TDynamicMemoryStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    TStoreBase::BuildOrchidYson(consumer);

    BuildYsonMapFluently(consumer)
        .Item("flush_state").Value(FlushState_)
        .Item("key_count").Value(GetKeyCount())
        .Item("lock_count").Value(GetLockCount())
        .Item("value_count").Value(GetValueCount())
        .Item("pool_size").Value(GetPoolSize())
        .Item("pool_capacity").Value(GetPoolCapacity());
}

ui32 TDynamicMemoryStore::GetLatestRevision() const
{
    YASSERT(!RevisionToTimestamp_.Empty());
    return RevisionToTimestamp_.Size() - 1;
}

ui32 TDynamicMemoryStore::RegisterRevision(TTimestamp timestamp)
{
    YASSERT(timestamp >= MinTimestamp && timestamp <= MaxTimestamp);
    YASSERT(RevisionToTimestamp_.Size() < HardRevisionsPerDynamicMemoryStoreLimit);
    RevisionToTimestamp_.PushBack(timestamp);
    return GetLatestRevision();
}

TTimestamp TDynamicMemoryStore::TimestampFromRevision(ui32 revision)
{
    return RevisionToTimestamp_[revision];
}

void TDynamicMemoryStore::UpdateTimestampRange(TTimestamp commitTimestamp)
{
    // NB: Don't update min/max timestamps for passive stores since
    // others are relying on these values to remain constant.
    // See, e.g., TStoreManager::MaxTimestampToStore_.
    if (StoreState_ == EStoreState::ActiveDynamic) {
        MinTimestamp_ = std::min(MinTimestamp_, commitTimestamp);
        MaxTimestamp_ = std::max(MaxTimestamp_, commitTimestamp);
    }
}

void TDynamicMemoryStore::OnMemoryUsageUpdated()
{
    SetMemoryUsage(GetUncompressedDataSize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
