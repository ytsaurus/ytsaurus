#include "sorted_dynamic_store.h"
#include "tablet.h"
#include "transaction.h"
#include "automaton.h"

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/versioned_writer.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/skip_list.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/linear_probe.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

static const size_t ReaderPoolSize = 16_KB;
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
        YT_ASSERT(!list->HasUncommitted());
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
        YT_VERIFY(HashTable_.Insert(fingerprint, value));
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
    TReaderBase(
        TSortedDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        bool produceAllVersions,
        ui32 revision,
        const TColumnFilter& columnFilter)
        : Store_(std::move(store))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , Revision_(revision)
        , ColumnFilter_(columnFilter)
        , KeyColumnCount_(Store_->KeyColumnCount_)
        , SchemaColumnCount_(Store_->SchemaColumnCount_)
        , ColumnLockCount_(Store_->ColumnLockCount_)
        , Pool_(TSortedDynamicStoreReaderPoolTag(), ReaderPoolSize)
    {
        YT_VERIFY(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.IsUniversal());

        LockMask_.Set(PrimaryLockIndex, ELockType::SharedWeak);
        if (columnFilter.IsUniversal()) {
            LockMask_.Enrich(ColumnLockCount_);
        } else {
            for (int columnIndex : columnFilter.GetIndexes()) {
                int lockIndex = Store_->ColumnIndexToLockIndex_[columnIndex];
                LockMask_.Set(lockIndex, ELockType::SharedWeak);
            }
        }
    }

protected:
    const TSortedDynamicStorePtr Store_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const ui32 Revision_;
    const TColumnFilter ColumnFilter_;

    int KeyColumnCount_;
    int SchemaColumnCount_;
    int ColumnLockCount_;

    TChunkedMemoryPool Pool_;

    std::vector<TTimestamp> DeleteTimestamps_;
    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TVersionedValue> VersionedValues_;

    TLockMask LockMask_;


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
            if (schemaColumns[index].Aggregate()) {
                ExtractByTimestamp(
                    list,
                    latestDeleteTimestamp,
                    latestWriteTimestamp,
                    [&] (const TDynamicValue& value) {
                        ProduceVersionedValue(&VersionedValues_.emplace_back(), index, value);
                    });
            } else {
                const auto* value = SearchByTimestamp(list, latestWriteTimestamp);
                if (value && Store_->TimestampFromRevision(value->Revision) > latestDeleteTimestamp) {
                    ProduceVersionedValue(&VersionedValues_.emplace_back(), index, *value);
                }
            }
        };

        if (ColumnFilter_.IsUniversal()) {
            for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
                fillValue(index);
            }
        } else {
            for (int index : ColumnFilter_.GetIndexes()) {
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
        Store_->WaitOnBlockedRow(dynamicRow, LockMask_, Timestamp_);

        // Prepare values and write timestamps.
        VersionedValues_.clear();
        WriteTimestamps_.clear();
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            for (auto list = dynamicRow.GetFixedValueList(columnIndex, KeyColumnCount_, ColumnLockCount_);
                 list;
                 list = list.GetSuccessor())
            {
                for (int itemIndex = UpperBoundByTimestamp(list, Timestamp_) - 1; itemIndex >= 0; --itemIndex) {
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
            for (int itemIndex = UpperBoundByTimestamp(list, Timestamp_) - 1; itemIndex >= 0; --itemIndex) {
                ui32 revision = list[itemIndex];
                if (revision <= Revision_) {
                    DeleteTimestamps_.push_back(Store_->TimestampFromRevision(revision));
                    YT_ASSERT(DeleteTimestamps_.size() == 1 ||
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
            dstValue->Type = Store_->Schema_.Columns()[index].GetPhysicalType();
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

            YT_ASSERT(!list.IsEmpty());

            auto* value = std::lower_bound(
                list.Begin(),
                list.End(),
                maxTimestamp,
                [&] (const T& element, TTimestamp timestamp) {
                    return Store_->TimestampFromRevision(ExtractRevision(element)) <= timestamp;
                }) - 1;

            YT_ASSERT(value >= list.Begin() || Store_->TimestampFromRevision(ExtractRevision(*value)) <= maxTimestamp);
            return value;
        }
    }

    template<class T>
    int UpperBoundByTimestamp(TEditList<T> list, TTimestamp maxTimestamp)
    {
        if (!list) {
            return 0;
        }

        if (maxTimestamp == SyncLastCommittedTimestamp || maxTimestamp == AsyncLastCommittedTimestamp) {
            return list.GetSize();
        }

        return std::lower_bound(
            list.Begin(),
            list.End(),
            maxTimestamp,
            [&] (const T& element, TTimestamp timestamp) {
                return Store_->TimestampFromRevision(ExtractRevision(element)) <= timestamp;
            }) - list.Begin();
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
        TSharedRange<TRowRange> ranges,
        TTimestamp timestamp,
        bool produceAllVersions,
        ui32 revision,
        const TColumnFilter& columnFilter)
        : TReaderBase(
            std::move(store),
            std::move(tabletSnapshot),
            timestamp,
            produceAllVersions,
            revision,
            columnFilter)
        , Ranges_(std::move(ranges))
    { }

    virtual TFuture<void> Open() override
    {
        UpdateLimits();
        return VoidFuture;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        YT_ASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        if (!Iterator_.IsValid()) {
            return false;
        }

        const auto& keyComparer = Store_->GetRowKeyComparer();

        i64 dataWeight = 0;
        while (Iterator_.IsValid() && rows->size() < rows->capacity()) {
            if (keyComparer(Iterator_.GetCurrent(), TKeyWrapper{UpperBound_}) >= 0) {
                UpdateLimits();
                if (!Iterator_.IsValid()) {
                    break;
                }
            }

            auto row = ProduceRow(Iterator_.GetCurrent());
            if (row) {
                rows->push_back(row);
                dataWeight += GetDataWeight(row);
            }

            Iterator_.MoveNext();
        }

        i64 rowCount = rows->size();

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        Store_->PerformanceCounters_->DynamicRowReadCount += rowCount;
        Store_->PerformanceCounters_->DynamicRowReadDataWeightCount += dataWeight;

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return TCodecStatistics();
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
    TKey LowerBound_;
    TKey UpperBound_;
    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;
    i64 RowCount_  = 0;
    i64 DataWeight_ = 0;

    typedef TSkipList<TSortedDynamicRow, TSortedDynamicRowKeyComparer>::TIterator TIterator;
    TIterator Iterator_;

    void UpdateLimits()
    {
        const auto& keyComparer = Store_->GetRowKeyComparer();

        while (RangeIndex_ < Ranges_.Size()) {
            LowerBound_ = Ranges_[RangeIndex_].first;
            UpperBound_ = Ranges_[RangeIndex_].second;

            Iterator_ = Store_->Rows_->FindGreaterThanOrEqualTo(TKeyWrapper{LowerBound_});

            if (Iterator_.IsValid() && keyComparer(Iterator_.GetCurrent(), TKeyWrapper{UpperBound_}) >= 0) {
                auto newBoundIt = std::upper_bound(
                    Ranges_.begin() + RangeIndex_,
                    Ranges_.end(),
                    Iterator_.GetCurrent(),
                    [&] (const TSortedDynamicRow& lhs, const TRowRange& rhs) {
                        return keyComparer(lhs, TKeyWrapper{rhs.second}) < 0;
                    });

                RangeIndex_ = std::distance(Ranges_.begin(), newBoundIt);
                continue;
            }

            ++RangeIndex_;
            return;
        }
        Iterator_ = TIterator();
    }

    TVersionedRow ProduceRow(const TSortedDynamicRow& dynamicRow)
    {
        return ProduceAllVersions_
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
        bool produceAllVersions,
        const TColumnFilter& columnFilter)
        : TReaderBase(
            std::move(store),
            std::move(tabletSnapshot),
            timestamp,
            produceAllVersions,
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
        YT_ASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        if (Finished_) {
            return false;
        }

        i64 dataWeight = 0;

        while (rows->size() < rows->capacity()) {
            if (RowCount_ == Keys_.Size())
                break;

            TVersionedRow row;
            if (Y_LIKELY(Store_->LookupHashTable_)) {
                auto dynamicRow = Store_->LookupHashTable_->Find(Keys_[RowCount_]);
                if (dynamicRow) {
                    row = ProduceRow(dynamicRow);
                }
            } else {
                auto iterator = Store_->Rows_->FindEqualTo(TKeyWrapper{Keys_[RowCount_]});
                if (iterator.IsValid()) {
                    row = ProduceRow(iterator.GetCurrent());
                }
            }
            rows->push_back(row);

            ++RowCount_;
            dataWeight += GetDataWeight(row);
        }

        if (rows->empty()) {
            Finished_ = true;
            return false;
        }

        DataWeight_ += dataWeight;
        Store_->PerformanceCounters_->DynamicRowLookupCount += rows->size();
        Store_->PerformanceCounters_->DynamicRowLookupDataWeightCount += dataWeight;

        return true;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return TCodecStatistics();
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
    i64 DataWeight_ = 0;
    bool Finished_ = false;

    TVersionedRow ProduceRow(const TSortedDynamicRow& dynamicRow)
    {
        return ProduceAllVersions_
            ? ProduceAllRowVersions(dynamicRow)
            : ProduceSingleRowVersion(dynamicRow);
    }
};

////////////////////////////////////////////////////////////////////////////////

TSortedDynamicStore::TSortedDynamicStore(
    TTabletManagerConfigPtr config,
    TStoreId id,
    TTablet* tablet)
    : TDynamicStoreBase(config, id, tablet)
    , RowKeyComparer_(Tablet_->GetRowKeyComparer())
    , Rows_(new TSkipList<TSortedDynamicRow, TSortedDynamicRowKeyComparer>(
        RowBuffer_->GetPool(),
        RowKeyComparer_))
{
    // Reserve the vector to prevent reallocations and thus enable accessing
    // it from arbitrary threads.
    RevisionToTimestamp_.ReserveChunks(MaxRevisionChunks);
    RevisionToTimestamp_.PushBack(UncommittedTimestamp);
    YT_VERIFY(TimestampFromRevision(UncommittedRevision) == UncommittedTimestamp);

    if (Tablet_->GetHashTableSize() > 0) {
        LookupHashTable_ = std::make_unique<TLookupHashTable>(
            Tablet_->GetHashTableSize(),
            RowKeyComparer_,
            Tablet_->PhysicalSchema().GetKeyColumnCount());
    }

    YT_LOG_DEBUG("Sorted dynamic store created (LookupHashTable: %v)",
        static_cast<bool>(LookupHashTable_));
}

TSortedDynamicStore::~TSortedDynamicStore()
{ }

IVersionedReaderPtr TSortedDynamicStore::CreateFlushReader()
{
    YT_VERIFY(FlushRevision_ != InvalidRevision);
    return New<TRangeReader>(
        this,
        nullptr,
        MakeSingletonRowRange(MinKey(), MaxKey()),
        AllCommittedTimestamp,
        true,
        FlushRevision_,
        TColumnFilter());
}

IVersionedReaderPtr TSortedDynamicStore::CreateSnapshotReader()
{
    return New<TRangeReader>(
        this,
        nullptr,
        MakeSingletonRowRange(MinKey(), MaxKey()),
        AllCommittedTimestamp,
        true,
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
    TLockMask lockMask,
    TTimestamp timestamp)
{
    if (timestamp == AsyncLastCommittedTimestamp ||
        timestamp == AllCommittedTimestamp ||
        Atomicity_ == EAtomicity::None)
    {
        return;
    }

    auto now = NProfiling::GetCpuInstant();
    auto deadline = now + NProfiling::DurationToCpuDuration(Config_->MaxBlockedRowWaitTime);

    while (true) {
        int lockIndex = GetBlockingLockIndex(row, lockMask, timestamp);
        if (lockIndex < 0) {
            break;
        }

        auto throwError = [&] (NTabletClient::EErrorCode errorCode, const TString& message) {
            THROW_ERROR_EXCEPTION(errorCode, message)
                << TErrorAttribute("lock", LockIndexToName_[lockIndex])
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("table_path", TablePath_)
                << TErrorAttribute("key", RowToKey(row))
                << TErrorAttribute("timeout", Config_->MaxBlockedRowWaitTime);
        };

        auto handler = GetRowBlockedHandler();
        if (!handler) {
            throwError(NTabletClient::EErrorCode::RowIsBlocked, "Row is blocked");
        }

        handler.Run(row, lockIndex);

        if (NProfiling::GetCpuInstant() > deadline) {
            throwError(NTabletClient::EErrorCode::BlockedRowWaitTimeout, "Timed out waiting on blocked row");
        }
    }
}

TSortedDynamicRow TSortedDynamicStore::ModifyRow(
    TUnversionedRow row,
    TLockMask lockMask,
    bool isDelete,
    TWriteContext* context)
{
    YT_ASSERT(FlushRevision_ != MaxRevision);

    TSortedDynamicRow result;

    auto commitTimestamp = context->CommitTimestamp;

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
                CommitValue(dynamicRow, list, value.Id);
            }
        }
    };

    auto newKeyProvider = [&] () -> TSortedDynamicRow {
        YT_ASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row.Begin());

        if (context->Phase == EWritePhase::Prelock || context->Phase == EWritePhase::Lock) {
            // Acquire the lock.
            AcquireRowLocks(dynamicRow, lockMask, isDelete, context);
        }

         // Copy values.
        addValues(dynamicRow);

        InsertIntoLookupHashTable(row.Begin(), dynamicRow);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TSortedDynamicRow dynamicRow) {
        if (context->Phase == EWritePhase::Prelock) {
            // Make sure the row is not blocked.
            if (!CheckRowBlocking(dynamicRow, lockMask, context)) {
                return;
            }

            // Check for lock conflicts and acquire the lock.
            auto error = CheckRowLocks(dynamicRow, context->Transaction, lockMask);
            if (!error.IsOK()) {
                context->Error = error;
                return;
            }
        }

        if (context->Phase == EWritePhase::Prelock || context->Phase == EWritePhase::Lock) {
            // Acquire the lock.
            AcquireRowLocks(dynamicRow, lockMask, isDelete, context);
        }

        // Copy values.
        addValues(dynamicRow);

        result = dynamicRow;
    };

    Rows_->Insert(TUnversionedRowWrapper{row}, newKeyProvider, existingKeyConsumer);

    if (!result) {
        return TSortedDynamicRow();
    }

    if (commitTimestamp != NullTimestamp) {
        if (isDelete) {
            AddDeleteRevision(result, revision);
        } else {
            auto& primaryLock = result.BeginLocks(KeyColumnCount_)[PrimaryLockIndex];
            AddWriteRevision(primaryLock, revision);
        }
        UpdateTimestampRange(commitTimestamp);
    }

    OnDynamicMemoryUsageUpdated();

    auto dataWeight = GetDataWeight(row);
    ++PerformanceCounters_->DynamicRowWriteCount;
    PerformanceCounters_->DynamicRowWriteDataWeightCount += dataWeight;
    ++context->RowCount;
    context->DataWeight += dataWeight;

    return result;
}

TSortedDynamicRow TSortedDynamicStore::ModifyRow(TVersionedRow row, TWriteContext* context)
{
    YT_ASSERT(FlushRevision_ != MaxRevision);

    auto* timestampToRevision = &context->TimestampToRevision;
    TSortedDynamicRow result;

    auto newKeyProvider = [&] () -> TSortedDynamicRow {
        YT_ASSERT(StoreState_ == EStoreState::ActiveDynamic);

        auto dynamicRow = AllocateRow();

        // Copy keys.
        SetKeys(dynamicRow, row.BeginKeys());

        InsertIntoLookupHashTable(row.BeginKeys(), dynamicRow);

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TSortedDynamicRow dynamicRow) {
        result = dynamicRow;
    };

    Rows_->Insert(TVersionedRowWrapper{row}, newKeyProvider, existingKeyConsumer);

    WriteRevisions_.clear();
    for (const auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
        auto revision = CaptureTimestamp(value->Timestamp, timestampToRevision);
        WriteRevisions_.push_back(revision);

        TDynamicValue dynamicValue;
        CaptureUnversionedValue(&dynamicValue, *value);
        dynamicValue.Revision = revision;

        int index = value->Id;
        auto currentList = result.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
        if (currentList && currentList.HasUncommitted()) {
            // It is possible for a replication transaction to be committing at a time when
            // a lock is being held by another transaction (performing an unversioned write into this table and
            // regarding it as a synchronous replica). In this case there's no actual conflict and
            // the replicated values must come first in the edit list.
            // See YT-10304.
            auto oldUncommittedValue = currentList.GetUncommitted();
            currentList.GetUncommitted() = dynamicValue;
            CommitValue(result, currentList, index);

            auto newList = PrepareFixedValue(result, index);
            newList.GetUncommitted() = oldUncommittedValue;
        } else {
            auto newList = PrepareFixedValue(result, index);
            newList.GetUncommitted() = dynamicValue;
            CommitValue(result, newList, index);
        }
    }

    std::sort(
        WriteRevisions_.begin(),
        WriteRevisions_.end(),
        [&] (ui32 lhs, ui32 rhs) {
            return TimestampFromRevision(lhs) < TimestampFromRevision(rhs);
        });
    WriteRevisions_.erase(std::unique(
        WriteRevisions_.begin(),
        WriteRevisions_.end(),
        [&] (ui32 lhs, ui32 rhs) {
            return TimestampFromRevision(lhs) == TimestampFromRevision(rhs);
        }),
        WriteRevisions_.end());
    auto& primaryLock = result.BeginLocks(KeyColumnCount_)[PrimaryLockIndex];
    for (auto revision : WriteRevisions_) {
        AddWriteRevision(primaryLock, revision);
        UpdateTimestampRange(TimestampFromRevision(revision));
    }

    for (const auto* timestamp = row.EndDeleteTimestamps() - 1; timestamp >= row.BeginDeleteTimestamps(); --timestamp) {
        auto revision = CaptureTimestamp(*timestamp, timestampToRevision);
        AddDeleteRevision(result, revision);
        UpdateTimestampRange(TimestampFromRevision(revision));
    }

    OnDynamicMemoryUsageUpdated();

    auto dataWeight = GetDataWeight(row);
    ++PerformanceCounters_->DynamicRowWriteCount;
    PerformanceCounters_->DynamicRowWriteDataWeightCount += dataWeight;
    ++context->RowCount;
    context->DataWeight += dataWeight;

    return result;
}

TSortedDynamicRow TSortedDynamicStore::MigrateRow(
    TTransaction* transaction,
    TSortedDynamicRow row,
    TLockMask lockMask)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);
    YT_ASSERT(FlushRevision_ != MaxRevision);

    auto migrateLocksAndValues = [&] (TSortedDynamicRow migratedRow) {
        auto* locks = row.BeginLocks(KeyColumnCount_);
        auto* migratedLocks = migratedRow.BeginLocks(KeyColumnCount_);

        // Migrate locks.
        {
            auto* lock = locks;
            auto* lockEnd = locks + ColumnLockCount_;
            auto* migratedLock = migratedLocks;

            for (int index = 0; lock < lockEnd; ++lock, ++migratedLock, ++index) {
                auto lockType = lockMask.Get(index);
                if (lock->WriteTransaction == transaction) {
                    // Write Lock
                    YT_ASSERT(lockType == ELockType::Exclusive);

                    // Validate the original lock's sanity.
                    // NB: For simple commit, transaction may not go through preparation stage
                    // during recovery.
                    YT_ASSERT(
                        transaction->GetPrepareTimestamp() == NullTimestamp ||
                        lock->PrepareTimestamp == transaction->GetPrepareTimestamp());

                    // Validate the migrated lock's sanity.
                    YT_ASSERT(!migratedLock->WriteTransaction);
                    YT_ASSERT(migratedLock->ReadLockCount == 0);
                    YT_ASSERT(migratedLock->PrepareTimestamp == NotPreparedTimestamp);

                    migratedLock->WriteTransaction = lock->WriteTransaction;
                    migratedLock->PrepareTimestamp = lock->PrepareTimestamp.load();
                    if (index == PrimaryLockIndex) {
                        YT_ASSERT(!migratedRow.GetDeleteLockFlag());
                        migratedRow.SetDeleteLockFlag(row.GetDeleteLockFlag());
                    }
                } else if (lockType == ELockType::SharedWeak || lockType == ELockType::SharedStrong) {
                    // Read Lock
                    ++migratedLock->ReadLockCount;
                }
            }
        }

        // Migrate fixed values.
        for (int columnIndex = KeyColumnCount_; columnIndex < SchemaColumnCount_; ++columnIndex) {
            int lockIndex = ColumnIndexToLockIndex_[columnIndex];
            if (locks[lockIndex].WriteTransaction == transaction) {
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

    OnDynamicMemoryUsageUpdated();

    return result;
}

void TSortedDynamicStore::PrepareRow(TTransaction* transaction, TSortedDynamicRow row)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);
    YT_ASSERT(FlushRevision_ != MaxRevision);

    auto prepareTimestamp = transaction->GetPrepareTimestamp();
    YT_ASSERT(prepareTimestamp != NullTimestamp);

    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            if (lock->WriteTransaction == transaction) {
                lock->PrepareTimestamp = prepareTimestamp;
            }
        }
    }
}

void TSortedDynamicStore::CommitRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask lockMask)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);
    YT_ASSERT(FlushRevision_ != MaxRevision);

    auto commitTimestamp = transaction->GetCommitTimestamp();
    ui32 commitRevision = RegisterRevision(commitTimestamp);

    auto* locks = row.BeginLocks(KeyColumnCount_);

    if (row.GetDeleteLockFlag()) {
        AddDeleteRevision(row, commitRevision);
    } else {
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            auto& lock = locks[ColumnIndexToLockIndex_[index]];
            if (lock.WriteTransaction == transaction) {
                auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                if (list.HasUncommitted()) {
                    list.GetUncommitted().Revision = commitRevision;
                    CommitValue(row, list, index);
                }
            }
        }
    }

    // NB: Add write timestamp _after_ the values are committed.
    // See remarks in TReaderBase.
    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        auto* lockEnd = lock + ColumnLockCount_;

        for (int index = 0; lock < lockEnd; ++lock, ++index) {
            auto lockType = lockMask.Get(index);
            if (lock->WriteTransaction == transaction) {
                // Write Lock
                YT_ASSERT(lockType == ELockType::Exclusive);
                if (!row.GetDeleteLockFlag()) {
                    AddWriteRevision(*lock, commitRevision);
                }
                lock->WriteTransaction = nullptr;
                lock->PrepareTimestamp = NotPreparedTimestamp;
            } else if (lockType == ELockType::SharedWeak) {
                YT_ASSERT(lock->ReadLockCount > 0);
                --lock->ReadLockCount;
            } else if (lockType == ELockType::SharedStrong) {
                YT_ASSERT(lock->ReadLockCount > 0);
                --lock->ReadLockCount;
                lock->LastReadLockTimestamp = std::max(lock->LastReadLockTimestamp, commitTimestamp);
            }
        }
    }

    row.SetDeleteLockFlag(false);

    Unlock();

    UpdateTimestampRange(commitTimestamp);
}

void TSortedDynamicStore::AbortRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask lockMask)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);
    YT_ASSERT(FlushRevision_ != MaxRevision);

    auto* locks = row.BeginLocks(KeyColumnCount_);

    if (!row.GetDeleteLockFlag()) {
        // Fixed values.
        for (int index = KeyColumnCount_; index < SchemaColumnCount_; ++index) {
            const auto& lock = locks[ColumnIndexToLockIndex_[index]];
            if (lock.WriteTransaction == transaction) {
                auto list = row.GetFixedValueList(index, KeyColumnCount_, ColumnLockCount_);
                if (list.HasUncommitted()) {
                    list.Abort();
                }
            }
        }
    }

    auto* lock = row.BeginLocks(KeyColumnCount_);
    auto* lockEnd = lock + ColumnLockCount_;

    for (int index = 0; lock < lockEnd; ++lock, ++index) {
        auto lockType = lockMask.Get(index);
        if (lock->WriteTransaction == transaction) {
            // Write Lock
            YT_ASSERT(lockType == ELockType::Exclusive);
            lock->WriteTransaction = nullptr;
            lock->PrepareTimestamp = NotPreparedTimestamp;
        } else if (lockType == ELockType::SharedWeak || lockType == ELockType::SharedStrong) {
            // Read Lock
            YT_ASSERT(lock->ReadLockCount > 0);
            --lock->ReadLockCount;
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
    YT_VERIFY(FlushRevision_ == InvalidRevision);
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
    TLockMask lockMask,
    TTimestamp timestamp)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);

    lockMask.Enrich(ColumnLockCount_);

    const auto* lock = row.BeginLocks(KeyColumnCount_);
    for (int index = 0;
         index < ColumnLockCount_;
         ++index, ++lock)
    {
        auto lockType = lockMask.Get(index);

        if (lockType != ELockType::None && lock->PrepareTimestamp < timestamp) {
            return index;
        }
    }
    return -1;
}

bool TSortedDynamicStore::CheckRowBlocking(
    TSortedDynamicRow row,
    TLockMask lockMask,
    TWriteContext* context)
{
    auto timestamp = context->Transaction->GetStartTimestamp();
    int lockIndex = GetBlockingLockIndex(row, lockMask, timestamp);
    if (lockIndex < 0) {
        return true;
    }

    context->BlockedStore = this;
    context->BlockedRow = row;
    context->BlockedLockMask = lockMask;
    context->BlockedTimestamp = timestamp;
    return false;
}

TTimestamp TSortedDynamicStore::GetLastWriteTimestamp(TSortedDynamicRow row, int lockIndex)
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

    if (lockIndex == PrimaryLockIndex) {
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

TTimestamp TSortedDynamicStore::GetLastReadTimestamp(TSortedDynamicRow row, int lockIndex)
{
    auto& lock = row.BeginLocks(KeyColumnCount_)[lockIndex];
    return lock.LastReadLockTimestamp;
}

TError TSortedDynamicStore::CheckRowLocks(
    TSortedDynamicRow row,
    TTransaction* transaction,
    TLockMask lockMask)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);

    // Check locks requested in #lockMask with the following exceptions:
    // * if primary lock is requested then all locks are checked
    // * primary lock is always checked

    // Enrich lock mask
    lockMask.Enrich(ColumnLockCount_);

    TError error;

    const auto* lock = row.BeginLocks(KeyColumnCount_);
    for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
        auto lockType = lockMask.Get(index);

        YT_VERIFY(lock->WriteTransaction != transaction);

        if (lockType == ELockType::Exclusive) {
            auto lastCommitTimestamp = GetLastReadTimestamp(row, index);
            if (lastCommitTimestamp > transaction->GetStartTimestamp()) {
                error = TError(
                    NTabletClient::EErrorCode::TransactionLockConflict,
                    "Write failed due to concurrent read lock")
                    << TErrorAttribute("winner_transaction_commit_timestamp", lastCommitTimestamp);
            }

            if (lock->ReadLockCount > 0) {
                YT_VERIFY(!lock->WriteTransaction);
                error = TError(
                     NTabletClient::EErrorCode::TransactionLockConflict,
                     "Write failed due to concurrent read lock");
            }
        }

        if (lockType != ELockType::None) {
            auto lastCommitTimestamp = GetLastWriteTimestamp(row, index);
            if (lastCommitTimestamp > transaction->GetStartTimestamp()) {
                error = TError(
                    NTabletClient::EErrorCode::TransactionLockConflict,
                    "Row lock conflict due to concurrent write")
                    << TErrorAttribute("winner_transaction_commit_timestamp", lastCommitTimestamp);
            }

            if (lock->WriteTransaction) {
                error = TError(
                    NTabletClient::EErrorCode::TransactionLockConflict,
                    "Row lock conflict due to concurrent write")
                    << TErrorAttribute("winner_transaction_id", lock->WriteTransaction->GetId());
            }
        }

        if (!error.IsOK()) {
            error = std::move(error)
                << TErrorAttribute("loser_transaction_id", transaction->GetId())
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("table_path", TablePath_)
                << TErrorAttribute("key", RowToKey(row))
                << TErrorAttribute("lock", LockIndexToName_[index]);
            break;
        }
    }
    return error;
}

void TSortedDynamicStore::AcquireRowLocks(
    TSortedDynamicRow row,
    TLockMask lockMask,
    bool isDelete,
    TWriteContext* context)
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);

    // Acquire locks requested in #lockMask with the following exceptions:
    // * if primary lock is requested then all locks are acquired
    {
        auto* lock = row.BeginLocks(KeyColumnCount_);
        for (int index = 0; index < ColumnLockCount_; ++index, ++lock) {
            auto lockType = lockMask.Get(index);

            if (lockType != ELockType::None) {
                YT_VERIFY(!lock->WriteTransaction);
                YT_ASSERT(lock->PrepareTimestamp == NotPreparedTimestamp);
            }

            if (lockType == ELockType::Exclusive) {
                lock->WriteTransaction = context->Transaction;
                YT_ASSERT(lock->ReadLockCount == 0);
            } else if (lockType != ELockType::None) {
                ++lock->ReadLockCount;
            }
        }
    }

    if (isDelete) {
        YT_ASSERT(!row.GetDeleteLockFlag());
        row.SetDeleteLockFlag(true);
    }

    Lock();
}

TValueList TSortedDynamicStore::PrepareFixedValue(TSortedDynamicRow row, int index)
{
    YT_ASSERT(index >= KeyColumnCount_ && index < SchemaColumnCount_);

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
    YT_ASSERT(!list || TimestampFromRevision(list.Back()) < TimestampFromRevision(revision));
    if (AllocateListForPushIfNeeded(&list, RowBuffer_->GetPool())) {
        row.SetDeleteRevisionList(list, KeyColumnCount_, ColumnLockCount_);
    }
    list.Push(revision);
}

void TSortedDynamicStore::AddWriteRevision(TLockDescriptor& lock, ui32 revision)
{
    auto list = TSortedDynamicRow::GetWriteRevisionList(lock);
    YT_ASSERT(!list || TimestampFromRevision(list.Back()) < TimestampFromRevision(revision));
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
        YT_ASSERT(srcValue.Id == index);
        dstRow.GetDataWeight() += GetDataWeight(srcValue);
        if (srcValue.Type == EValueType::Null) {
            nullKeyMask |= nullKeyBit;
        } else {
            YT_ASSERT(srcValue.Type == columnIt->GetPhysicalType());
            if (IsStringLikeType(columnIt->GetPhysicalType())) {
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
        bool isNull = nullKeyMask & nullKeyBit;
        dstRow.GetDataWeight() += GetDataWeight(columnIt->GetPhysicalType(), isNull, *srcKeys);
        if (!isNull) {
            if(IsStringLikeType(columnIt->GetPhysicalType())) {
                *dstKeys = CaptureStringValue(*srcKeys);
            } else {
                *dstKeys = *srcKeys;
            }
        }
    }
}

void TSortedDynamicStore::CommitValue(TSortedDynamicRow row, TValueList list, int index)
{
    row.GetDataWeight() += GetDataWeight(Schema_.Columns()[index].GetPhysicalType(), list.GetUncommitted());
    list.Commit();

    if (row.GetDataWeight() > MaxDataWeight_) {
        MaxDataWeight_ = row.GetDataWeight();
        MaxDataWeightWitness_ = row;
    }
}

void TSortedDynamicStore::LoadRow(
    TVersionedRow row,
    TLoadScratchData* scratchData)
{
    YT_ASSERT(row.GetKeyCount() == KeyColumnCount_);

    auto* timestampToRevision = &scratchData->TimestampToRevision;
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
            ui32 revision = CaptureVersionedValue(&list.GetUncommitted(), *value, timestampToRevision);
            CommitValue(dynamicRow, list, index);
            scratchData->WriteRevisions[lockIndex].push_back(revision);
        }

        currentValue = endValue;
    }

    auto* locks = dynamicRow.BeginLocks(KeyColumnCount_);
    for (int lockIndex = 0; lockIndex < ColumnLockCount_; ++lockIndex) {
        auto& lock = locks[lockIndex];
        auto& revisions = scratchData->WriteRevisions[lockIndex];
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
            ui32 revision = CaptureTimestamp(*currentTimestamp, timestampToRevision);
            AddDeleteRevision(dynamicRow, revision);
        }
    }

    Rows_->Insert(dynamicRow);

    InsertIntoLookupHashTable(row.BeginKeys(), dynamicRow);
}

ui32 TSortedDynamicStore::CaptureTimestamp(
    TTimestamp timestamp,
    TTimestampToRevisionMap* timestampToRevision)
{
    auto it = timestampToRevision->find(timestamp);
    if (it == timestampToRevision->end()) {
        ui32 revision = RegisterRevision(timestamp);
        YT_VERIFY(timestampToRevision->insert(std::make_pair(timestamp, revision)).second);
        return revision;
    } else {
        return it->second;
    }
}

ui32 TSortedDynamicStore::CaptureVersionedValue(
    TDynamicValue* dst,
    const TVersionedValue& src,
    TTimestampToRevisionMap* timestampToRevision)
{
    YT_ASSERT(src.Type == EValueType::Null || src.Type == Schema_.Columns()[src.Id].GetPhysicalType());
    ui32 revision = CaptureTimestamp(src.Timestamp, timestampToRevision);
    dst->Revision = revision;
    CaptureUnversionedValue(dst, src);
    return revision;
}

void TSortedDynamicStore::CaptureUncommittedValue(TDynamicValue* dst, const TDynamicValue& src, int index)
{
    YT_ASSERT(index >= KeyColumnCount_ && index < SchemaColumnCount_);
    YT_ASSERT(src.Revision == UncommittedRevision);

    *dst = src;
    if (!src.Null && IsStringLikeType(Schema_.Columns()[index].GetPhysicalType())) {
        dst->Data = CaptureStringValue(src.Data);
    }
}

void TSortedDynamicStore::CaptureUnversionedValue(
    TDynamicValue* dst,
    const TUnversionedValue& src)
{
    YT_ASSERT(src.Type == EValueType::Null || src.Type == Schema_.Columns()[src.Id].GetPhysicalType());

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
    YT_ASSERT(IsStringLikeType(EValueType(src.Type)));
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

i64 TSortedDynamicStore::GetTimestampCount() const
{
    return RevisionToTimestamp_.Size();
}

TOwningKey TSortedDynamicStore::GetMinKey() const
{
    return MinKey();
}

TOwningKey TSortedDynamicStore::GetUpperBoundKey() const
{
    return MaxKey();
}

IVersionedReaderPtr TSortedDynamicStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const NChunkClient::TClientBlockReadOptions& /*blockReadOptions*/,
    IThroughputThrottlerPtr /*throttler*/)
{
    return New<TRangeReader>(
        this,
        tabletSnapshot,
        std::move(ranges),
        timestamp,
        produceAllVersions,
        MaxRevision,
        columnFilter);
}

IVersionedReaderPtr TSortedDynamicStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const NChunkClient::TClientBlockReadOptions&,
    IThroughputThrottlerPtr /*throttler*/)
{
    return New<TLookupReader>(
        this,
        tabletSnapshot,
        keys,
        timestamp,
        produceAllVersions,
        columnFilter);
}

bool TSortedDynamicStore::CheckRowLocks(
    TUnversionedRow row,
    TLockMask lockMask,
    TWriteContext* context)
{
    auto it = Rows_->FindEqualTo(TUnversionedRowWrapper{row});
    if (!it.IsValid()) {
        return true;
    }

    auto dynamicRow = it.GetCurrent();

    if (context->Phase == EWritePhase::Prelock && !CheckRowBlocking(dynamicRow, lockMask, context)) {
        return false;
    }

    auto error = CheckRowLocks(dynamicRow, context->Transaction, lockMask);
    if (!error.IsOK()) {
        context->Error = error;
        return false;
    }

    return true;
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
        YT_LOG_DEBUG("Store snapshot serialization started");

        YT_LOG_DEBUG("Opening table reader");
        WaitFor(tableReader->Open())
            .ThrowOnError();

        auto chunkWriter = New<TMemoryWriter>();

        auto tableWriterConfig = New<TChunkWriterConfig>();
        tableWriterConfig->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletRecovery);

        auto tableWriterOptions = New<TTabletWriterOptions>();
        tableWriterOptions->OptimizeFor = EOptimizeFor::Scan;

        auto tableWriter = CreateVersionedChunkWriter(
            tableWriterConfig,
            tableWriterOptions,
            Schema_,
            chunkWriter);

        std::vector<TVersionedRow> rows;
        rows.reserve(SnapshotRowsPerRead);

        YT_LOG_DEBUG("Serializing store snapshot");

        i64 rowCount = 0;
        while (tableReader->Read(&rows)) {
            if (rows.empty()) {
                YT_LOG_DEBUG("Waiting for table reader");
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            rowCount += rows.size();
            if (!tableWriter->Write(rows)) {
                YT_LOG_DEBUG("Waiting for table writer");
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
        YT_LOG_DEBUG("Closing table writer");
        WaitFor(tableWriter->Close())
            .ThrowOnError();

        Save(context, *chunkWriter->GetChunkMeta());

        auto blocks = TBlock::Unwrap(chunkWriter->GetBlocks());
        YT_LOG_DEBUG("Writing store blocks (RowCount: %v, BlockCount: %v)",
            rowCount,
            blocks.size());

        Save(context, blocks);

        YT_LOG_DEBUG("Store snapshot serialization complete");
    });
}

void TSortedDynamicStore::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    if (Load<bool>(context)) {
        auto chunkMeta = New<TRefCountedChunkMeta>(Load<TChunkMeta>(context));
        auto blocks = Load<std::vector<TSharedRef>>(context);

        auto chunkReader = CreateMemoryReader(
            std::move(chunkMeta),
            TBlock::Wrap(blocks));

        auto asyncCachedMeta = TCachedVersionedChunkMeta::Load(
            chunkReader,
            TClientBlockReadOptions(),
            Schema_,
            {} /* ColumnRenameDescriptors */,
            MemoryTracker_);
        auto cachedMeta = WaitFor(asyncCachedMeta)
            .ValueOrThrow();
        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), StoreId_);
        auto chunkState = New<TChunkState>(
            GetNullBlockCache(),
            chunkSpec,
            nullptr,
            NullTimestamp,
            nullptr,
            New<TChunkReaderPerformanceCounters>(),
            nullptr);

        auto tableReaderConfig = New<TTabletChunkReaderConfig>();
        auto tableReader = CreateVersionedChunkReader(
            tableReaderConfig,
            chunkReader,
            std::move(chunkState),
            std::move(cachedMeta),
            TClientBlockReadOptions(),
            MinKey(),
            MaxKey(),
            TColumnFilter(),
            AllCommittedTimestamp,
            true);
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
        YT_VERIFY(FlushRevision_ == InvalidRevision);
        FlushRevision_ = MaxRevision;
    }

    OnDynamicMemoryUsageUpdated();
}

TSortedDynamicStorePtr TSortedDynamicStore::AsSortedDynamic()
{
    return this;
}

ui32 TSortedDynamicStore::GetLatestRevision() const
{
    YT_ASSERT(!RevisionToTimestamp_.Empty());
    return RevisionToTimestamp_.Size() - 1;
}

ui32 TSortedDynamicStore::RegisterRevision(TTimestamp timestamp)
{
    YT_VERIFY(timestamp >= MinTimestamp && timestamp <= MaxTimestamp);

    auto latestRevision = GetLatestRevision();
    if (TimestampFromRevision(latestRevision) == timestamp) {
        return latestRevision;
    }

    YT_VERIFY(RevisionToTimestamp_.Size() < HardRevisionsPerDynamicStoreLimit);
    RevisionToTimestamp_.PushBack(timestamp);
    return GetLatestRevision();
}

void TSortedDynamicStore::OnDynamicMemoryUsageUpdated()
{
    auto hashTableSize = LookupHashTable_ ? LookupHashTable_->GetByteSize() : 0;
    SetDynamicMemoryUsage(GetUncompressedDataSize() + hashTableSize);
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

i64 TSortedDynamicStore::GetMaxDataWeight() const
{
    return MaxDataWeight_;
}

TOwningKey TSortedDynamicStore::GetMaxDataWeightWitnessKey() const
{
    return RowToKey(MaxDataWeightWitness_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
