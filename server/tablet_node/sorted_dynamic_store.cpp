#include "sorted_dynamic_store.h"
#include "config.h"
#include "tablet.h"
#include "transaction.h"
#include "automaton.h"

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/versioned_writer.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/skip_list.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/linear_probe.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/ytree/fluent.h>

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

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

static const size_t ReaderPoolSize = (size_t) 16 * 1024;
static const int SnapshotRowsPerRead = 1024;

struct TSortedDynamicStoreReaderPoolTag
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
        Y_ASSERT(!list->HasUncommitted());
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

class TSortedDynamicStore::TLookupHashTable
{
public:
    TLookupHashTable(
        int size,
        TSortedDynamicRowKeyComparer keyComparer,
        int keyColumnCount)
        : HashTable_(size)
        , KeyComparer_(std::move(keyComparer))
        , KeyColumnCount_(keyColumnCount)
        , Size_(size)
    { }

    void Insert(const TUnversionedValue* keyBegin, TSortedDynamicRow dynamicRow)
    {
        auto fingerprint = GetFarmFingerprint(keyBegin, keyBegin + KeyColumnCount_);
        auto value = reinterpret_cast<ui64>(dynamicRow.GetHeader());
        YCHECK(HashTable_.Insert(fingerprint, value));
    }

    void Insert(TUnversionedRow row, TSortedDynamicRow dynamicRow)
    {
        Insert(row.Begin(), dynamicRow);
    }

    TSortedDynamicRow Find(TKey key) const
    {
        auto fingerprint = GetFarmFingerprint(key);
        SmallVector<ui64, 1> items;
        HashTable_.Find(fingerprint, &items);
        for (auto item : items) {
            auto dynamicRow = TSortedDynamicRow(reinterpret_cast<TSortedDynamicRowHeader*>(item));
            if (KeyComparer_(dynamicRow, TKeyWrapper{key}) == 0) {
                return dynamicRow;
            }
        }
        return TSortedDynamicRow();
    }

    size_t GetByteSize() const
    {
        return HashTable_.GetByteSize();
    }

    int GetSize() const
    {
        return Size_;
    }

private:
    TLinearProbeHashTable HashTable_;
    const TSortedDynamicRowKeyComparer KeyComparer_;
    const int KeyColumnCount_;
    const int Size_;
};

////////////////////////////////////////////////////////////////////////////////

class TSortedDynamicStore::TReaderBase
{
public:
    explicit TReaderBase(
        TSortedDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        ui32 revision,
        const TColumnFilter& columnFilter)
        : Store_(std::move(store))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , Revision_(revision)
        , ColumnFilter_(columnFilter)
        , KeyColumnCount_(Store_->KeyColumnCount_)
        , SchemaColumnCount_(Store_->SchemaColumnCount_)
        , ColumnLockCount_(Store_->ColumnLockCount_)
        , Pool_(TSortedDynamicStoreReaderPoolTag(), ReaderPoolSize)
    {
        YCHECK(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.All);

        if (columnFilter.All) {
            LockMask_ = TSortedDynamicRow::AllLocksMask;
        } else {
            LockMask_ = TSortedDynamicRow::PrimaryLockMask;
            for (int columnIndex : columnFilter.Indexes) {
                int lockIndex = Store_->ColumnIndexToLockIndex_[columnIndex];
                LockMask_ |= (1 << lockIndex);
            }
        }
    }

protected:
    const TSortedDynamicStorePtr Store_;
    const TTabletSnapshotPtr TabletSnapshot_;
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


    TTimestamp GetLatestWriteTimestamp(TSortedDynamicRow dynamicRow)
    {
        auto* lock = dynamicRow.BeginLocks(KeyColumnCount_);
        auto maxTimestamp = NullTimestamp;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            auto list = TSortedDynamicRow::GetWriteRevisionList(*lock);
            const auto* revisionPtr = SearchByTimestamp(list, Timestamp_);
            if (revisionPtr) {
                auto timestamp = Store_->TimestampFromRevision(*revisionPtr);
                maxTimestamp = std::max(maxTimestamp, timestamp);
            }
        }
        return maxTimestamp;
    }

    TTimestamp GetLatestDeleteTimestamp(TSortedDynamicRow dynamicRow)
    {
        auto list = dynamicRow.GetDeleteRevisionList(KeyColumnCount_, ColumnLockCount_);
        const auto* revisionPtr = SearchByTimestamp(list, Timestamp_);
        return  revisionPtr ? Store_->TimestampFromRevision(*revisionPtr) : NullTimestamp;
    }


    TVersionedRow ProduceSingleRowVersion(TSortedDynamicRow dynamicRow)
    {
        Store_->WaitOnBlockedRow(dynamicRow, LockMask_, Timestamp_);

        // Prepare timestamps.
        auto latestWriteTimestamp = GetLatestWriteTimestamp(dynamicRow);
        auto latestDeleteTimestamp = GetLatestDeleteTimestamp(dynamicRow);

        if (latestWriteTimestamp == NullTimestamp && latestDeleteTimestamp == NullTimestamp) {
            return TVersionedRow();
        }

        int writeTimestampCount = 1;
        int deleteTimestampCount = 1;

        if (latestDeleteTimestamp == NullTimestamp) {
            deleteTimestampCount = 0;
        } else if (latestDeleteTimestamp > latestWriteTimestamp) {
            writeTimestampCount = 0;
        }

        // Prepare values.
        VersionedValues_.clear();

        const auto& schemaColumns = TabletSnapshot_->PhysicalSchema.Columns();

        auto fillValue = [&] (int index) {
            // NB: Inserting a new item into value list and adding a new write revision cannot
            // be done atomically. We always append values before revisions but in the middle of these
            // two steps there might be "phantom" values present in the row.
            // To work this around, we cap the value lists by #latestWriteTimestamp to make sure that
            // no "phantom" value is listed.
            auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
            if (schemaColumns[index].Aggregate) {
                ExtractByTimestamp(
                    list,
                    latestDeleteTimestamp,
                    latestWriteTimestamp,
                        [&] (const TDynamicValue& value) {
                        VersionedValues_.push_back(TVersionedValue());
                        ProduceVersionedValue(&VersionedValues_.back(), index, value);
                    });
            } else {
                const auto* value = SearchByTimestamp(list, latestWriteTimestamp);
                if (value && Store_->TimestampFromRevision(value->Revision) > latestDeleteTimestamp) {
                    VersionedValues_.push_back(TVersionedValue());
                    ProduceVersionedValue(&VersionedValues_.back(), index, *value);
                }
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

        auto versionedRow = TMutableVersionedRow::Allocate(
            &Pool_,
            KeyColumnCount_,
            VersionedValues_.size(),
            writeTimestampCount,
            deleteTimestampCount);

        // Keys.
        ProduceKeys(dynamicRow, versionedRow.BeginKeys());

        // Timestamps.
        if (writeTimestampCount > 0) {
            versionedRow.BeginWriteTimestamps()[0] = latestWriteTimestamp;
        }
        if (deleteTimestampCount > 0) {
            versionedRow.BeginDeleteTimestamps()[0] = latestDeleteTimestamp;
        }

        // Values.
        ::memcpy(versionedRow.BeginValues(), VersionedValues_.data(), sizeof (TVersionedValue) * VersionedValues_.size());

        return versionedRow;
    }

    TVersionedRow ProduceAllRowVersions(TSortedDynamicRow dynamicRow)
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
                    Y_ASSERT(DeleteTimestamps_.size() == 1 ||
                            DeleteTimestamps_.back() < DeleteTimestamps_[DeleteTimestamps_.size() - 2]);
                }
            }
        }

        if (WriteTimestamps_.empty() && DeleteTimestamps_.empty()) {
            return TVersionedRow();
        }

        auto versionedRow = TMutableVersionedRow::Allocate(
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

    void ProduceKeys(TSortedDynamicRow dynamicRow, TUnversionedValue* dstKey)
    {
        ui32 nullKeyMask = dynamicRow.GetNullKeyMask();
        ui32 nullKeyBit = 1;
        const auto* srcKey = dynamicRow.BeginKeys();
        for (int index = 0;
             index < KeyColumnCount_;
             ++index, nullKeyBit <<= 1, ++srcKey, ++dstKey)
        {
            ProduceUnversionedValue(dstKey, index, *srcKey, (nullKeyMask & nullKeyBit) != 0, false);
        }
    }

    void ProduceUnversionedValue(TUnversionedValue* dstValue, int index, TDynamicValueData srcData, bool null, bool aggregate)
    {
        dstValue->Id = index;
        dstValue->Aggregate = aggregate;
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
        ProduceUnversionedValue(dstValue, index, srcValue.Data, srcValue.Null, srcValue.Aggregate);
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

            Y_ASSERT(!list.IsEmpty());

            auto* value = std::lower_bound(
                list.Begin(),
                list.End(),
                maxTimestamp,
                [&] (const T& element, TTimestamp timestamp) {
                    return Store_->TimestampFromRevision(ExtractRevision(element)) <= timestamp;
                }) - 1;

            Y_ASSERT(value >= list.Begin() || Store_->TimestampFromRevision(ExtractRevision(*value)) <= maxTimestamp);
            return value;
        }
    }

    template <class T, class TValueExtractor>
    void ExtractByTimestamp(
        TEditList<T> list,
        TTimestamp minTimestamp,
        TTimestamp maxTimestamp,
        const TValueExtractor& valueExtractor)
    {
        while (list) {
            if (list.GetSize() > 0 && Store_->TimestampFromRevision(ExtractRevision(list[0])) <= maxTimestamp) {
                break;
            }
            list = list.GetSuccessor();
        }

        if (!list) {
            return;
        }

        while (list) {
            if (list.GetSize() > 0) {
                if (Store_->TimestampFromRevision(ExtractRevision(list[list.GetSize() - 1])) <= minTimestamp) {
                    return;
                }

                auto* begin = list.Begin();
                auto* end = list.End();

                if (Store_->TimestampFromRevision(ExtractRevision(*begin)) <= minTimestamp) {
                    begin = std::lower_bound(
                        begin,
                        end,
                        minTimestamp,
                        [&] (const T& element, TTimestamp value) {
                            return Store_->TimestampFromRevision(ExtractRevision(element)) <= value;
                        });
                }

                if (end - begin > 0 && Store_->TimestampFromRevision(ExtractRevision(*(end - 1))) > maxTimestamp) {
                    end = std::lower_bound(
                        begin,
                        end,
                        maxTimestamp,
                        [&] (const T& element, TTimestamp value) {
                            return Store_->TimestampFromRevision(ExtractRevision(element)) <= value;
                        });
                }

                while (begin < end) {
                    --end;
                    valueExtractor(*end);
                }

            }

            list = list.GetSuccessor();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSortedDynamicStore::TRangeReader
    : public TReaderBase
    , public IVersionedReader
{
public:
    TRangeReader(
        TSortedDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        ui32 revision,
        const TColumnFilter& columnFilter)
        : TReaderBase(
            std::move(store),
            std::move(tabletSnapshot),
            timestamp,
            revision,
            columnFilter)
        , LowerKey_(std::move(lowerKey))
        , UpperKey_(std::move(upperKey))
    { }

    virtual TFuture<void> Open() override
    {
        Iterator_ = Store_->Rows_->FindGreaterThanOrEqualTo(TKeyWrapper{LowerKey_});
        return VoidFuture;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        Y_ASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        const auto& keyComparer = Store_->GetRowKeyComparer();

        while (Iterator_.IsValid() && rows->size() < rows->capacity()) {
            if (keyComparer(Iterator_.GetCurrent(), TKeyWrapper{UpperKey_}) >= 0)
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

        Store_->PerformanceCounters_->DynamicRowReadCount += rows->size();

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return true;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return std::vector<TChunkId>();
    }

private:
    const TOwningKey LowerKey_;
    const TOwningKey UpperKey_;

    TSkipList<TSortedDynamicRow, TSortedDynamicRowKeyComparer>::TIterator Iterator_;

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

class TSortedDynamicStore::TLookupReader
    : public TReaderBase
    , public IVersionedReader
{
public:
    TLookupReader(
        TSortedDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : TReaderBase(
            std::move(store),
            std::move(tabletSnapshot),
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
        Y_ASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        if (Finished_) {
            return false;
        }

        while (rows->size() < rows->capacity()) {
            if (RowCount_ == Keys_.Size())
                break;

            TVersionedRow row;
            if (Y_LIKELY(Store_->LookupHashTable_)) {
                auto dynamicRow = Store_->LookupHashTable_->Find(Keys_[RowCount_]);
                if (dynamicRow) {
                    row = ProduceSingleRowVersion(dynamicRow);
                }
            } else {
                auto iterator = Store_->Rows_->FindEqualTo(TKeyWrapper{Keys_[RowCount_]});
                if (iterator.IsValid()) {
                    row = ProduceSingleRowVersion(iterator.GetCurrent());
                }
            }
            rows->push_back(row);

            ++RowCount_;
        }

        if (rows->empty()) {
            Finished_ = true;
            return false;
        }

        Store_->PerformanceCounters_->DynamicRowLookupCount += rows->size();

        return true;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return true;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return std::vector<TChunkId>();
    }

private:
    const TSharedRange<TKey> Keys_;
    i64 RowCount_  = 0;
    bool Finished_ = false;

};

////////////////////////////////////////////////////////////////////////////////

TSortedDynamicStore::TSortedDynamicStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(config, id, tablet)
    , TDynamicStoreBase(config, id, tablet)
    , TSortedStoreBase(config, id, tablet)
    , RowKeyComparer_(Tablet_->GetRowKeyComparer())
    , Rows_(new TSkipList<TSortedDynamicRow, TSortedDynamicRowKeyComparer>(
        RowBuffer_->GetPool(),
        RowKeyComparer_))
{
    // Reserve the vector to prevent reallocations and thus enable accessing
    // it from arbitrary threads.
    RevisionToTimestamp_.ReserveChunks(MaxRevisionChunks);
    RevisionToTimestamp_.PushBack(UncommittedTimestamp);
    YCHECK(TimestampFromRevision(UncommittedRevision) == UncommittedTimestamp);

    if (Tablet_->GetHashTableSize() > 0) {
        LookupHashTable_ = std::make_unique<TLookupHashTable>(
            Tablet_->GetHashTableSize(),
            RowKeyComparer_,
            Tablet_->PhysicalSchema().GetKeyColumnCount());
    }

    LOG_DEBUG("Sorted dynamic store created (LookupHashTable: %v)",
        static_cast<bool>(LookupHashTable_));
}

TSortedDynamicStore::~TSortedDynamicStore()
{
    LOG_DEBUG("Sorted dynamic memory store destroyed");
}

IVersionedReaderPtr TSortedDynamicStore::CreateFlushReader()
{
    YCHECK(FlushRevision_ != InvalidRevision);
    return New<TRangeReader>(
        this,
        nullptr,
        MinKey(),
        MaxKey(),
        AllCommittedTimestamp,
        FlushRevision_,
        TColumnFilter());
}

IVersionedReaderPtr TSortedDynamicStore::CreateSnapshotReader()
{
    return New<TRangeReader>(
        this,
        nullptr,
        MinKey(),
        MaxKey(),
        AllCommittedTimestamp,
        GetLatestRevision(),
        TColumnFilter());
}

const TSortedDynamicRowKeyComparer& TSortedDynamicStore::GetRowKeyComparer() const
{
    return RowKeyComparer_;
}

void TSortedDynamicStore::SetRowBlockedHandler(TRowBlockedHandler handler)
{
    TWriterGuard guard(RowBlockedLock_);
    RowBlockedHandler_ = std::move(handler);
}

void TSortedDynamicStore::ResetRowBlockedHandler()
{
    TWriterGuard guard(RowBlockedLock_);
    RowBlockedHandler_.Reset();
}

void TSortedDynamicStore::WaitOnBlockedRow(
    TSortedDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    if (timestamp == AsyncLastCommittedTimestamp)
        return;
    if (Atomicity_ == EAtomicity::None)
        return;

    auto now = NProfiling::GetCpuInstant();
    auto deadline = now + NProfiling::DurationToCpuDuration(Config_->MaxBlockedRowWaitTime);

    while (true) {
        int lockIndex = GetBlockingLockIndex(row, lockMask, timestamp);
        if (lockIndex < 0) {
            break;
        }

        auto throwError = [&] (const Stroka& message) {
            THROW_ERROR_EXCEPTION(message)
                << TErrorAttribute("lock", LockIndexToName_[lockIndex])
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("key", RowToKey(row))
                << TErrorAttribute("timeout", Config_->MaxBlockedRowWaitTime);
        };

        auto handler = GetRowBlockedHandler();
        if (!handler) {
            throwError("Row is blocked");
        }

        handler.Run(row, lockIndex);

        if (NProfiling::GetCpuInstant() > deadline) {
            throwError("Timed out waiting on blocked row");
        }
    }
}

TSortedDynamicRow TSortedDynamicStore::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    TTimestamp commitTimestamp,
    ui32 lockMask)
{
    Y_ASSERT(FlushRevision_ != MaxRevision);

    TSortedDynamicRow result;

    ui32 revision = commitTimestamp == NullTimestamp
        ? UncommittedRevision
        : RegisterRevision(commitTimestamp);

    auto addValues = [&] (TSortedDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            auto list = PrepareFixedValue(dynamicRow, value.Id);
            auto& uncommittedValue = list.GetUncommitted();
            uncommittedValue.Revision = revision;
            CaptureUnversionedValue(&uncommittedValue, value);
            if (commitTimestamp != NullTimestamp) {
                list.Commit();
            }
        }
    };

    auto newKeyProvider = [&] () -> TSortedDynamicRow {
        Y_ASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row.Begin());

        if (commitTimestamp == NullTimestamp) {
            // Acquire the lock.
            AcquireRowLocks(dynamicRow, transaction, lockMask, false);
        }

        // Copy values.
        addValues(dynamicRow);

        InsertIntoLookupHashTable(row.Begin(), dynamicRow);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TSortedDynamicRow dynamicRow) {
        if (commitTimestamp == NullTimestamp) {
            // Make sure the row is not blocked.
            ValidateRowNotBlocked(dynamicRow, lockMask, transaction->GetStartTimestamp());

            // Check for lock conflicts and acquire the lock.
            CheckRowLocks(dynamicRow, transaction, lockMask);
            AcquireRowLocks(dynamicRow, transaction, lockMask, false);
        }

        // Copy values.
        addValues(dynamicRow);

        result = dynamicRow;
    };

    Rows_->Insert(TRowWrapper{row}, newKeyProvider, existingKeyConsumer);

    if (commitTimestamp != NullTimestamp) {
        auto& primaryLock = result.BeginLocks(KeyColumnCount_)[TSortedDynamicRow::PrimaryLockIndex];
        AddWriteRevision(primaryLock, revision);
        UpdateTimestampRange(commitTimestamp);
    }

    OnMemoryUsageUpdated();

    ++PerformanceCounters_->DynamicRowWriteCount;

    return result;
}

TSortedDynamicRow TSortedDynamicStore::DeleteRow(
    TTransaction* transaction,
    NTableClient::TKey key,
    TTimestamp commitTimestamp)
{
    Y_ASSERT(FlushRevision_ != MaxRevision);

    ui32 revision = commitTimestamp == NullTimestamp
        ? UncommittedRevision
        : RegisterRevision(commitTimestamp);

    TSortedDynamicRow result;

    auto newKeyProvider = [&] () -> TSortedDynamicRow {
        Y_ASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, key.Begin());

        if (commitTimestamp == NullTimestamp) {
            // Acquire the lock.
            AcquireRowLocks(dynamicRow, transaction, TSortedDynamicRow::PrimaryLockMask, true);
        }

        // Insert row in hash table.
        InsertIntoLookupHashTable(key.Begin(), dynamicRow);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TSortedDynamicRow dynamicRow) {
        if (commitTimestamp == NullTimestamp) {
            // Make sure the row is not blocked.
            ValidateRowNotBlocked(dynamicRow, TSortedDynamicRow::PrimaryLockMask, transaction->GetStartTimestamp());

            // Check for lock conflicts and acquire the lock.
            CheckRowLocks(dynamicRow, transaction, TSortedDynamicRow::PrimaryLockMask);
            AcquireRowLocks(dynamicRow, transaction, TSortedDynamicRow::PrimaryLockMask, true);
        }

        result = dynamicRow;
    };

    Rows_->Insert(TRowWrapper{key}, newKeyProvider, existingKeyConsumer);

    if (commitTimestamp != NullTimestamp) {
        AddDeleteRevision(result, revision);
        UpdateTimestampRange(commitTimestamp);
    }

    OnMemoryUsageUpdated();

    ++PerformanceCounters_->DynamicRowDeleteCount;

    return result;
}

TSortedDynamicRow TSortedDynamicStore::MigrateRow(TTransaction* transaction, TSortedDynamicRow row)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);
    Y_ASSERT(FlushRevision_ != MaxRevision);

    auto migrateLocksAndValues = [&] (TSortedDynamicRow migratedRow) {
        auto* locks = row.BeginLocks(KeyColumnCount_);
        auto* migratedLocks = migratedRow.BeginLocks(KeyColumnCount_);

        // Migrate locks.
        {
            const auto* lock = locks;
            auto* migratedLock = migratedLocks;
            for (int index = 0; index < ColumnLockCount_; ++index, ++lock, ++migratedLock) {
                if (lock->Transaction == transaction) {
                    // Validate the original lock's sanity.
                    // NB: For simple commit, transaction may not go through preparation stage
                    // during recovery.
                    Y_ASSERT(
                        transaction->GetPrepareTimestamp() == NullTimestamp ||
                        lock->PrepareTimestamp == transaction->GetPrepareTimestamp());

                    // Validate the migrated lock's sanity.
                    Y_ASSERT(!migratedLock->Transaction);
                    Y_ASSERT(migratedLock->PrepareTimestamp == NotPreparedTimestamp);

                    migratedLock->Transaction = lock->Transaction;
                    migratedLock->PrepareTimestamp = lock->PrepareTimestamp.load();
                    if (index == TSortedDynamicRow::PrimaryLockIndex) {
                        Y_ASSERT(!migratedRow.GetDeleteLockFlag());
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

    TSortedDynamicRow result;
    auto newKeyProvider = [&] () -> TSortedDynamicRow {
        // Create migrated row.
        auto migratedRow = result = AllocateRow();

        // Migrate keys.
        SetKeys(migratedRow, row);

        migrateLocksAndValues(migratedRow);

        InsertIntoLookupHashTable(RowToKey(row).Begin(), migratedRow);

        return migratedRow;
    };

    auto existingKeyConsumer = [&] (TSortedDynamicRow migratedRow) {
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

void TSortedDynamicStore::PrepareRow(TTransaction* transaction, TSortedDynamicRow row)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);
    Y_ASSERT(FlushRevision_ != MaxRevision);

    auto prepareTimestamp = transaction->GetPrepareTimestamp();
    Y_ASSERT(prepareTimestamp != NullTimestamp);

    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->Transaction == transaction) {
                lock->PrepareTimestamp = prepareTimestamp;
            }
        }
    }
}

void TSortedDynamicStore::CommitRow(TTransaction* transaction, TSortedDynamicRow row)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);
    Y_ASSERT(FlushRevision_ != MaxRevision);

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
                if (!row.GetDeleteLockFlag()) {
                    AddWriteRevision(*lock, commitRevision);
                }
                lock->Transaction = nullptr;
                lock->PrepareTimestamp = NotPreparedTimestamp;
            }
        }
    }

    row.SetDeleteLockFlag(false);

    Unlock();

    UpdateTimestampRange(commitTimestamp);
}

void TSortedDynamicStore::AbortRow(TTransaction* transaction, TSortedDynamicRow row)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);
    Y_ASSERT(FlushRevision_ != MaxRevision);

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

TSortedDynamicRow TSortedDynamicStore::FindRow(TKey key)
{
    auto it = Rows_->FindEqualTo(TKeyWrapper{key});
    return it.IsValid() ? it.GetCurrent() : TSortedDynamicRow();
}

std::vector<TSortedDynamicRow> TSortedDynamicStore::GetAllRows()
{
    std::vector<TSortedDynamicRow> rows;
    for (auto it = Rows_->FindGreaterThanOrEqualTo(TKeyWrapper{MinKey()});
         it.IsValid();
         it.MoveNext())
    {
        rows.push_back(it.GetCurrent());
    }
    return rows;
}

void TSortedDynamicStore::OnSetPassive()
{
    YCHECK(FlushRevision_ == InvalidRevision);
    FlushRevision_ = GetLatestRevision();
}

TSortedDynamicRow TSortedDynamicStore::AllocateRow()
{
    return TSortedDynamicRow::Allocate(
        RowBuffer_->GetPool(),
        KeyColumnCount_,
        ColumnLockCount_,
        SchemaColumnCount_);
}

TSortedDynamicStore::TRowBlockedHandler TSortedDynamicStore::GetRowBlockedHandler()
{
    TReaderGuard guard(RowBlockedLock_);
    return RowBlockedHandler_;
}

int TSortedDynamicStore::GetBlockingLockIndex(
    TSortedDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);

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

void TSortedDynamicStore::ValidateRowNotBlocked(
    TSortedDynamicRow row,
    ui32 lockMask,
    TTimestamp timestamp)
{
    int lockIndex = GetBlockingLockIndex(row, lockMask, timestamp);
    if (lockIndex >= 0) {
        throw TRowBlockedException(this, row, lockMask, timestamp);
    }
}

TTimestamp TSortedDynamicStore::GetLastCommitTimestamp(
    TSortedDynamicRow row,
    int lockIndex)
{
    auto timestamp = MinTimestamp;
    auto& lock = row.BeginLocks(KeyColumnCount_)[lockIndex];
    auto writeRevisionList = TSortedDynamicRow::GetWriteRevisionList(lock);
    if (writeRevisionList) {
        int size = writeRevisionList.GetSize();
        if (size > 0) {
            timestamp = TimestampFromRevision(writeRevisionList[size - 1]);
        }
    }
    if (lockIndex == TSortedDynamicRow::PrimaryLockIndex) {
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

void TSortedDynamicStore::CheckRowLocks(
    TSortedDynamicRow row,
    TTransaction* transaction,
    ui32 lockMask)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);

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
            (lockMask & TSortedDynamicRow::PrimaryLockMask) ||
            (index == TSortedDynamicRow::PrimaryLockIndex))
        {
            if (lock->Transaction) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::TransactionLockConflict,
                    "Row lock conflict")
                    << TErrorAttribute("loser_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_id", lock->Transaction->GetId())
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("key", RowToKey(row))
                    << TErrorAttribute("lock", LockIndexToName_[index]);
            }
            auto lastCommitTimestamp = GetLastCommitTimestamp(row, index);
            if (lastCommitTimestamp > transaction->GetStartTimestamp()) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::TransactionLockConflict,
                    "Row lock conflict")
                    << TErrorAttribute("loser_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_commit_timestamp", lastCommitTimestamp)
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("key", RowToKey(row))
                    << TErrorAttribute("lock", LockIndexToName_[index]);
            }
        }
    }
}

void TSortedDynamicStore::AcquireRowLocks(
    TSortedDynamicRow row,
    TTransaction* transaction,
    ui32 lockMask,
    bool deleteFlag)
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);

    // Acquire locks requested in #lockMask with the following exceptions:
    // * if primary lock is requested then all locks are acquired
    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        ui32 lockMaskBit = 1;
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock, lockMaskBit <<= 1) {
            if ((lockMask & lockMaskBit) || (lockMask & TSortedDynamicRow::PrimaryLockMask)) {
                Y_ASSERT(!lock->Transaction);
                lock->Transaction = transaction;
                Y_ASSERT(lock->PrepareTimestamp == NotPreparedTimestamp);
            }
        }
    }

    if (deleteFlag) {
        Y_ASSERT(!row.GetDeleteLockFlag());
        row.SetDeleteLockFlag(true);
    }

    Lock();
}

TValueList TSortedDynamicStore::PrepareFixedValue(TSortedDynamicRow row, int index)
{
    Y_ASSERT(index >= KeyColumnCount_ && index < SchemaColumnCount_);

    auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        row.SetFixedValueList(index, list, KeyColumnCount_, ColumnLockCount_);
    }
    ++StoreValueCount_;
    list.Prepare();
    return list;
}

void TSortedDynamicStore::AddDeleteRevision(TSortedDynamicRow row, ui32 revision)
{
    auto list = row.GetDeleteRevisionList(KeyColumnCount_, ColumnLockCount_);
    Y_ASSERT(!list || TimestampFromRevision(list.Back()) < TimestampFromRevision(revision));
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        row.SetDeleteRevisionList(list, KeyColumnCount_, ColumnLockCount_);
    }
    list.Push(revision);
}

void TSortedDynamicStore::AddWriteRevision(TLockDescriptor& lock, ui32 revision)
{
    auto list = TSortedDynamicRow::GetWriteRevisionList(lock);
    Y_ASSERT(!list || TimestampFromRevision(list.Back()) < TimestampFromRevision(revision));
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        TSortedDynamicRow::SetWriteRevisionList(lock, list);
    }
    list.Push(revision);
}

void TSortedDynamicStore::SetKeys(TSortedDynamicRow dstRow, const TUnversionedValue* srcKeys)
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
        Y_ASSERT(srcValue.Id == index);
        if (srcValue.Type == EValueType::Null) {
            nullKeyMask |= nullKeyBit;
        } else {
            Y_ASSERT(srcValue.Type == columnIt->Type);
            if (IsStringLikeType(columnIt->Type)) {
                *dstValue = CaptureStringValue(srcValue);
            } else {
                ::memcpy(dstValue, &srcValue.Data, sizeof(TDynamicValueData));
            }
        }
    }
    dstRow.SetNullKeyMask(nullKeyMask);
}

void TSortedDynamicStore::SetKeys(TSortedDynamicRow dstRow, TSortedDynamicRow srcRow)
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

void TSortedDynamicStore::LoadRow(
    TVersionedRow row,
    TLoadScratchData* scratchData)
{
    Y_ASSERT(row.GetKeyCount() == KeyColumnCount_);

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
    const auto& primaryRevisions = scratchData->WriteRevisions[TSortedDynamicRow::PrimaryLockIndex];
    for (int lockIndex = 0; lockIndex < ColumnLockCount_; ++lockIndex) {
        auto& lock = locks[lockIndex];
        auto& revisions = scratchData->WriteRevisions[lockIndex];
        // NB: Taking the primary lock implies taking all other locks.
        if (lockIndex != TSortedDynamicRow::PrimaryLockIndex) {
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

    InsertIntoLookupHashTable(row.BeginKeys(), dynamicRow);
}

ui32 TSortedDynamicStore::CaptureTimestamp(
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

ui32 TSortedDynamicStore::CaptureVersionedValue(
    TDynamicValue* dst,
    const TVersionedValue& src,
    TLoadScratchData* scratchData)
{
    Y_ASSERT(src.Type == EValueType::Null || src.Type == Schema_.Columns()[src.Id].Type);
    ui32 revision = CaptureTimestamp(src.Timestamp, scratchData);
    dst->Revision = revision;
    CaptureUnversionedValue(dst, src);
    return revision;
}

void TSortedDynamicStore::CaptureUncommittedValue(TDynamicValue* dst, const TDynamicValue& src, int index)
{
    Y_ASSERT(index >= KeyColumnCount_ && index < SchemaColumnCount_);
    Y_ASSERT(src.Revision == UncommittedRevision);

    *dst = src;
    if (!src.Null && IsStringLikeType(Schema_.Columns()[index].Type)) {
        dst->Data = CaptureStringValue(src.Data);
    }
}

void TSortedDynamicStore::CaptureUnversionedValue(
    TDynamicValue* dst,
    const TUnversionedValue& src)
{
    Y_ASSERT(src.Type == EValueType::Null || src.Type == Schema_.Columns()[src.Id].Type);

    dst->Aggregate = src.Aggregate;

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

TDynamicValueData TSortedDynamicStore::CaptureStringValue(TDynamicValueData src)
{
    ui32 length = src.String->Length;
    TDynamicValueData dst;
    dst.String = reinterpret_cast<TDynamicString*>(RowBuffer_->GetPool()->AllocateAligned(
        sizeof(ui32) + length,
        sizeof(ui32)));
    ::memcpy(dst.String, src.String, sizeof(ui32) + length);
    return dst;
}

TDynamicValueData TSortedDynamicStore::CaptureStringValue(const TUnversionedValue& src)
{
    Y_ASSERT(IsStringLikeType(EValueType(src.Type)));
    ui32 length = src.Length;
    TDynamicValueData dst;
    dst.String = reinterpret_cast<TDynamicString*>(RowBuffer_->GetPool()->AllocateAligned(
        sizeof(ui32) + length,
        sizeof(ui32)));
    dst.String->Length = length;
    ::memcpy(dst.String->Data, src.Data.String, length);
    return dst;
}

EStoreType TSortedDynamicStore::GetType() const
{
    return EStoreType::SortedDynamic;
}

i64 TSortedDynamicStore::GetRowCount() const
{
    return Rows_->GetSize();
}

TOwningKey TSortedDynamicStore::GetMinKey() const
{
    return MinKey();
}

TOwningKey TSortedDynamicStore::GetMaxKey() const
{
    return MaxKey();
}

IVersionedReaderPtr TSortedDynamicStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& /*workloadDescriptor*/)
{
    YCHECK(timestamp != AllCommittedTimestamp);
    return New<TRangeReader>(
        this,
        tabletSnapshot,
        std::move(lowerKey),
        std::move(upperKey),
        timestamp,
        MaxRevision,
        columnFilter);
}

IVersionedReaderPtr TSortedDynamicStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& /*workloadDescriptor*/)
{
    YCHECK(timestamp != AllCommittedTimestamp);
    return New<TLookupReader>(
        this,
        tabletSnapshot,
        keys,
        timestamp,
        columnFilter);
}

void TSortedDynamicStore::CheckRowLocks(
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

void TSortedDynamicStore::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);

    using NYT::Save;
    Save(context, MinTimestamp_);
    Save(context, MaxTimestamp_);
}

void TSortedDynamicStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);

    using NYT::Load;
    Load(context, MinTimestamp_);
    Load(context, MaxTimestamp_);
}

TCallback<void(TSaveContext& context)> TSortedDynamicStore::AsyncSave()
{
    using NYT::Save;

    auto tableReader = CreateSnapshotReader();

    return BIND([=, this_ = MakeStrong(this)] (TSaveContext& context) {
        WaitFor(tableReader->Open())
            .ThrowOnError();

        auto chunkWriter = New<TMemoryWriter>();
        auto tableWriterConfig = New<TChunkWriterConfig>();
        auto tableWriterOptions = New<TTabletWriterOptions>();
        tableWriterOptions->OptimizeFor = EOptimizeFor::Scan;
        auto tableWriter = CreateVersionedChunkWriter(
            tableWriterConfig,
            tableWriterOptions,
            Schema_,
            chunkWriter);
        WaitFor(tableWriter->Open())
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(SnapshotRowsPerRead);

        i64 rowCount = 0;
        while (tableReader->Read(&rows)) {
            if (rows.empty()) {
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            rowCount += rows.size();
            if (!tableWriter->Write(rows)) {
                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        // pushsin@ forbids empty chunks.
        if (rowCount == 0) {
            Save(context, false);
            return;
        }

        Save(context, true);

        // NB: This also closes chunkWriter.
        WaitFor(tableWriter->Close())
            .ThrowOnError();

        Save(context, chunkWriter->GetChunkMeta());
        Save(context, chunkWriter->GetBlocks());
    });
}

void TSortedDynamicStore::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    if (Load<bool>(context)) {
        auto chunkMeta = Load<TChunkMeta>(context);
        auto blocks = Load<std::vector<TSharedRef>>(context);

        auto chunkReader = CreateMemoryReader(chunkMeta, blocks);

        auto asyncCachedMeta = TCachedVersionedChunkMeta::Load(chunkReader, TWorkloadDescriptor(), Schema_);
        auto cachedMeta = WaitFor(asyncCachedMeta)
            .ValueOrThrow();

        auto tableReaderConfig = New<TTabletChunkReaderConfig>();
        auto tableReader = CreateVersionedChunkReader(
            tableReaderConfig,
            chunkReader,
            GetNullBlockCache(),
            cachedMeta,
            MinKey(),
            MaxKey(),
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

    // Cf. YT-4534
    if (StoreState_ == EStoreState::PassiveDynamic ||
        StoreState_ == EStoreState::RemovePrepared)
    {
        // NB: No more changes are possible after load.
        YCHECK(FlushRevision_ == InvalidRevision);
        FlushRevision_ = MaxRevision;
    }

    OnMemoryUsageUpdated();
}

TSortedDynamicStorePtr TSortedDynamicStore::AsSortedDynamic()
{
    return this;
}

ui32 TSortedDynamicStore::GetLatestRevision() const
{
    Y_ASSERT(!RevisionToTimestamp_.Empty());
    return RevisionToTimestamp_.Size() - 1;
}

ui32 TSortedDynamicStore::RegisterRevision(TTimestamp timestamp)
{
    YCHECK(timestamp >= MinTimestamp && timestamp <= MaxTimestamp);
    YCHECK(RevisionToTimestamp_.Size() < HardRevisionsPerDynamicStoreLimit);
    RevisionToTimestamp_.PushBack(timestamp);
    return GetLatestRevision();
}

void TSortedDynamicStore::OnMemoryUsageUpdated()
{
    auto hashTableSize = LookupHashTable_ ? LookupHashTable_->GetByteSize() : 0;
    SetMemoryUsage(GetUncompressedDataSize() + hashTableSize);
}

void TSortedDynamicStore::InsertIntoLookupHashTable(
    const TUnversionedValue* keyBegin,
    TSortedDynamicRow dynamicRow)
{
    if (LookupHashTable_) {
        if (GetRowCount() >= LookupHashTable_->GetSize()) {
            LookupHashTable_.reset();
        } else {
            LookupHashTable_->Insert(keyBegin, dynamicRow);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
