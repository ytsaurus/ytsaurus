#include "stdafx.h"
#include "static_memory_store.h"
#include "config.h"
#include "tablet.h"

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const int RowsPerSegment = 10240;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TIterator, class TPivot, class TComparer>
TIterator BinarySearch(
    TIterator begin,
    TIterator end,
    TPivot pivot,
    TComparer comparer)
{
    auto left = begin;
    auto right = end;
    while (true) {
        auto dist = right - left;
        if (dist <= 1) {
            return left;
        }
        auto mid = left + dist / 2;
        int result = comparer(mid, pivot);
        if (result == 0) {
            return mid;
        }
        if (result < 0) {
            left = mid;
        } else {
            right = mid;
        }
    }
}

template <class T, class TTimestampExtractor>
T* FindVersionedValue(
    T* begin,
    T* end,
    TTimestamp maxTimestamp,
    TTimestampExtractor timestampExtractor)
{
    if (begin == end) {
        return nullptr;
    }

    if (maxTimestamp == LastCommittedTimestamp) {
        return end - 1;
    } else {
        auto* left = begin;
        auto* right = end;
        while (right - left > 1) {
            auto* mid = left + (right - left) / 2;
            if (timestampExtractor(*mid) <= maxTimestamp) {
                left = mid;
            }
            else {
                right = mid;
            }
        }

        return left && timestampExtractor(*left) <= maxTimestamp ? left : nullptr;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStaticMemoryStoreBuilder::TStaticMemoryStoreBuilder(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(config)
    , Tablet_(tablet)
{
    KeyCount_ = static_cast<int>(Tablet_->KeyColumns().size());
    SchemaColumnCount_ = static_cast<int>(Tablet_->Schema().Columns().size());

    CurrentRow_ = TStaticRow();

    Data_.reset(new TData());
    Data_->RowSize = TStaticRow::GetSize(
        Tablet_->KeyColumns().size(),
        Tablet_->Schema().Columns().size());
    Data_->AlignedPool.reset(new TChunkedMemoryPool(Config_->AlignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio));
    Data_->UnalignedPool.reset(new TChunkedMemoryPool(Config_->UnalignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio));
}

void TStaticMemoryStoreBuilder::BeginRow()
{
    YASSERT(!CurrentRow_);

    if (Data_->Segments.empty() || Data_->Segments.back().RowCount == RowsPerSegment) {
        struct TRowsTag { };
        TSegment segment;
        segment.Rows = TSharedRef::Allocate<TRowsTag>(Data_->RowSize * RowsPerSegment);
        segment.RowCount = 0;
        Data_->Segments.push_back(std::move(segment));
    }

    auto& segment = Data_->Segments.back();
    CurrentRow_ = TStaticRow(reinterpret_cast<TStaticRowHeader*>(
        segment.Rows.Begin() +
        segment.RowCount * Data_->RowSize));
    ++segment.RowCount;
}

TTimestamp* TStaticMemoryStoreBuilder::AllocateTimestamps(int count)
{
    YASSERT(CurrentRow_);
    YASSERT(count >= 0);

    if (count == 0) {
        return nullptr;
    }

    auto* result = reinterpret_cast<TTimestamp*>(Data_->AlignedPool->Allocate(sizeof(TTimestamp) * count));
    CurrentRow_.SetTimestamps(KeyCount_, result);
    CurrentRow_.SetTimestampCount(KeyCount_, SchemaColumnCount_, count);
    return result;
}

TUnversionedValue* TStaticMemoryStoreBuilder::AllocateKeys()
{
    YASSERT(CurrentRow_);

    // No allocation, actually.
    return CurrentRow_.GetKeys();
}

TVersionedValue* TStaticMemoryStoreBuilder::AllocateFixedValues(int index, int count)
{
    YASSERT(CurrentRow_);
    YASSERT(count >= 0);

    if (count == 0) {
        return nullptr;
    }

    auto* result = reinterpret_cast<TVersionedValue*>(Data_->AlignedPool->Allocate(sizeof(TVersionedValue) * count));
    CurrentRow_.SetFixedValues(KeyCount_, index, result);
    CurrentRow_.SetFixedValueCount(index, count, KeyCount_, SchemaColumnCount_);
    return result;
}

void TStaticMemoryStoreBuilder::EndRow(TTimestamp lastCommitTimestamp)
{
    YASSERT(CurrentRow_);

    CurrentRow_.SetLastCommitTimestamp(lastCommitTimestamp);

    // Copy keys.
    auto* keys = CurrentRow_.GetKeys();
    for (int columnIndex = 0; columnIndex < KeyCount_; ++columnIndex) {
        CopyValueIfNeeded(keys + columnIndex);
    }

    // Copy fixed values.
    for (int columnIndex = 0; columnIndex < SchemaColumnCount_ - KeyCount_; ++columnIndex) {
        auto* values = CurrentRow_.GetFixedValues(columnIndex, KeyCount_);
        int count = CurrentRow_.GetFixedValueCount(columnIndex, KeyCount_, SchemaColumnCount_);
        for (int valueIndex = 0; valueIndex < count; ++valueIndex) {
            CopyValueIfNeeded(values + valueIndex);
        }
    }

    CurrentRow_ = TStaticRow();
}

TStaticMemoryStorePtr TStaticMemoryStoreBuilder::Finish()
{
    YASSERT(!CurrentRow_);

    return New<TStaticMemoryStore>(
        Config_,
        Tablet_,
        std::move(Data_));
}

void TStaticMemoryStoreBuilder::CopyValueIfNeeded(TUnversionedValue* value)
{
    if (value->Type == EValueType::String || value->Type == EValueType::Any) {
        char* newString = Data_->UnalignedPool->AllocateUnaligned(value->Length);
        memcpy(newString, value->Data.String, value->Length);
        value->Data.String = newString;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TStaticMemoryStore::TScanner
    : public IStoreScanner
{
public:
    explicit TScanner(TStaticMemoryStorePtr store)
        : Store_(std::move(store))
        , KeyCount_(Store_->Tablet_->KeyColumns().size())
        , SchemaValueCount_(Store_->Tablet_->Schema().Columns().size())
        , RowSize_(Store_->Data_->RowSize)
        , Comparer_(KeyCount_)
    {
        ResetCurrentRow();
    }


    virtual TTimestamp Find(TKey key, TTimestamp timestamp) override
    {
        return DoBeginScan(key, timestamp, true);
    }

    virtual TTimestamp BeginScan(NVersionedTableClient::TKey key, TTimestamp timestamp) override
    {
        return DoBeginScan(key, timestamp, false);
    }

    virtual TTimestamp Advance() override
    {
        YASSERT(CurrentRow_);

        auto segmentIt = CurrentSegmentIt_;
        int rowIndex = CurrentRowIndex_;

        if (++rowIndex == segmentIt->RowCount) {
            if (++segmentIt == Store_->Data_->Segments.end()) {
                return ResetCurrentRow();
            }
        }

        return SetCurrentRow(segmentIt, rowIndex, CurrentMaxTimestamp_);
    }

    virtual void EndScan() override
    {
        ResetCurrentRow();
    }

    virtual const TUnversionedValue& GetKey(int index) const override
    {
        YASSERT(CurrentRow_);
        YASSERT(index >= 0 && index < KeyCount_);

        return CurrentRow_.GetKeys()[index];
    }

    virtual const NVersionedTableClient::TVersionedValue* GetFixedValue(int index) const override
    {
        YASSERT(CurrentRow_);
        YASSERT(index >= 0 && index < SchemaValueCount_ - KeyCount_);

        auto* begin = CurrentRow_.GetFixedValues(index, KeyCount_);
        auto* end = begin + CurrentRow_.GetFixedValueCount(index, KeyCount_, SchemaValueCount_);
        return FindVersionedValueVersionedValue(
            begin,
            end,
            CurrentMinTimestamp_,
            CurrentMaxTimestamp_);
    }

    virtual void GetFixedValues(
        int index,
        int maxVersions,
        std::vector<TVersionedValue>* values) const override
    {
        YASSERT(CurrentRow_);
        YASSERT(index >= 0 && index < SchemaValueCount_ - KeyCount_);

        values->clear();

        auto* begin = CurrentRow_.GetFixedValues(index, KeyCount_);
        auto* end = begin + CurrentRow_.GetFixedValueCount(index, KeyCount_, SchemaValueCount_);
        auto* value = FindVersionedValue(
            begin,
            end,
            CurrentMinTimestamp_,
            [] (const TVersionedValue& value) {
                return value.Timestamp;
            });

        if (!value || value->Timestamp < CurrentMinTimestamp_)
            return;

        while (value != end && values->size() < maxVersions) {
            values->push_back(*value++);
        }
    }

    virtual void GetTimestamps(std::vector<TTimestamp>* timestamps) const override
    {
        YASSERT(CurrentRow_);

        timestamps->clear();

        auto* begin = CurrentRow_.GetTimestamps(KeyCount_);
        auto* end = begin + CurrentRow_.GetTimestampCount(KeyCount_, SchemaValueCount_);
        auto* timestamp = FindVersionedValue(
            begin,
            end,
            CurrentMinTimestamp_,
            [] (TTimestamp value) {
                return value & TimestampValueMask;
            });

        if (!timestamp || *timestamp < CurrentMinTimestamp_)
            return;

        while (timestamp != end) {
            timestamps->push_back(*timestamp++);
        }
    }

private:
    TStaticMemoryStorePtr Store_;

    int KeyCount_;
    int SchemaValueCount_;
    size_t RowSize_;
    TKeyPrefixComparer Comparer_;

    TSegmentIt CurrentSegmentIt_;
    int CurrentRowIndex_;
    TStaticRow CurrentRow_;
    TTimestamp CurrentMaxTimestamp_;
    TTimestamp CurrentMinTimestamp_;


    const TTimestamp* FindVersionedValueMinTimestamp(
        const TTimestamp* begin,
        const TTimestamp* end,
        TTimestamp maxTimestamp)
    {
        return FindVersionedValue(
            begin,
            end,
            maxTimestamp,
            [] (TTimestamp timestamp) {
                return timestamp & TimestampValueMask;
            });
    }

    static const TVersionedValue* FindVersionedValueVersionedValue(
        const TVersionedValue* begin,
        const TVersionedValue* end,
        TTimestamp minTimestamp,
        TTimestamp maxTimestamp)
    {
        auto* result = FindVersionedValue(
            begin,
            end,
            maxTimestamp,
            [] (const TVersionedValue& value) {
                return value.Timestamp;
            });
        return result && result->Timestamp >= minTimestamp ? result : nullptr;
    }


    TTimestamp DoBeginScan(TKey key, TTimestamp timestamp, bool exact)
    {
        auto& segments = Store_->Data_->Segments;
        auto segmentIt = BinarySearch(
            segments.begin(),
            segments.end(),
            key,
            [&] (TSegmentIt segmentIt, TKey key) {
                return Comparer_(GetRow(segmentIt, 0), key);
            });

        if (segmentIt == segments.end()) {
            return NullTimestamp;
        }

        const auto& segment = *segmentIt;
        int rowIndex = BinarySearch(
            0,
            segment.RowCount,
            key,
            [&] (int index, TKey key) {
                return Comparer_(GetRow(segmentIt, index), key);
            });

        if (rowIndex == segment.RowCount) {
            return NullTimestamp;
        }

        if (exact && Comparer_(GetRow(segmentIt, rowIndex), key) != 0) {
            return NullTimestamp;
        }

        return SetCurrentRow(segmentIt, rowIndex, timestamp);
    }

    TStaticRow GetRow(TSegmentIt segmentIt, int rowIndex)
    {
        return TStaticRow(reinterpret_cast<TStaticRowHeader*>(
            const_cast<char*>(segmentIt->Rows.Begin()) +
            RowSize_ * rowIndex));
    }

    TTimestamp SetCurrentRow(TSegmentIt segmentIt, int rowIndex, TTimestamp timestamp)
    {
        auto row = GetRow(segmentIt, rowIndex);
        const auto* timestampBegin = row.GetTimestamps(KeyCount_);
        const auto* timestampEnd = timestampBegin + row.GetTimestampCount(KeyCount_, SchemaValueCount_);
        const auto* timestampMin = FindVersionedValueMinTimestamp(timestampBegin, timestampEnd, timestamp);
        
        if (!timestampMin) {
            return NullTimestamp;
        }

        if (*timestampMin & TombstoneTimestampMask) {
            return *timestampMin;
        }

        CurrentSegmentIt_ = segmentIt;
        CurrentRowIndex_ = rowIndex;
        CurrentRow_ = row;
        CurrentMinTimestamp_ = *timestampMin;
        CurrentMaxTimestamp_ = timestamp;

        auto result = *timestampMin;
        if (timestampMin == timestampBegin) {
            result |= IncrementalTimestampMask;
        }
        return result;
    }

    TTimestamp ResetCurrentRow()
    {
        CurrentSegmentIt_ = TSegmentIt();
        CurrentRowIndex_ = -1;
        CurrentRow_ = TStaticRow();
        CurrentMaxTimestamp_ = NullTimestamp;
        CurrentMinTimestamp_ = NullTimestamp;
        return NullTimestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

TStaticMemoryStore::TStaticMemoryStore(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    std::unique_ptr<TData> data)
    : Config_(config)
    , Tablet_(tablet)
    , Data_(std::move(data))
{ }

std::unique_ptr<IStoreScanner> TStaticMemoryStore::CreateScanner()
{
    return std::unique_ptr<IStoreScanner>(new TScanner(this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
