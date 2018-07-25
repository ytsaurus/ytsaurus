#include "integer_column_writer.h"
#include "helpers.h"
#include "column_writer_detail.h"
#include "compressed_integer_vector.h"

#include <yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/core/misc/zigzag.h>
#include <yt/core/misc/bitmap.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

const int MaxValueCount = 128 * 1024;

////////////////////////////////////////////////////////////////////////////////

inline ui64 GetEncodedValue(i64 value)
{
    return ZigZagEncode64(value);
}

inline ui64 GetEncodedValue(ui64 value)
{
    return value;
}

////////////////////////////////////////////////////////////////////////////////

class TIntegerColumnWriterBase
{
protected:
    std::vector<ui64> Values_;

    ui64 MaxValue_;
    ui64 MinValue_;
    THashMap<ui64, int> DistinctValues_;

    void Reset()
    {
        Values_.clear();

        MaxValue_ = 0;
        MinValue_ = std::numeric_limits<ui64>::max();

        DistinctValues_.clear();
    }

    void UpdateStatistics(ui64 value)
    {
        MaxValue_ = std::max(MaxValue_, value);
        MinValue_ = std::min(MinValue_, value);
        DistinctValues_.insert(std::make_pair(value, DistinctValues_.size() + 1));
    }

    void DumpDirectValues(TSegmentInfo* segmentInfo, TAppendOnlyBitmap<ui64>& nullBitmap)
    {
        for (i64 index = 0; index < Values_.size(); ++index) {
            if (!nullBitmap[index]) {
                Values_[index] -= MinValue_;
            }
        }

        // 1. Direct values.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(Values_), MaxValue_ - MinValue_));

        // 2. Null bitmap.
        segmentInfo->Data.push_back(nullBitmap.Flush<TSegmentWriterTag>());
    }

    void DumpDictionaryValues(TSegmentInfo* segmentInfo, TAppendOnlyBitmap<ui64>& nullBitmap)
    {
        std::vector<ui64> dictionary;
        dictionary.reserve(DistinctValues_.size());

        for (i64 index = 0; index < Values_.size(); ++index) {
            if (nullBitmap[index]) {
                Y_ASSERT(Values_[index] == 0);
            } else {
                auto dictionaryIndex = DistinctValues_[Values_[index]];
                Y_ASSERT(dictionaryIndex <= dictionary.size() + 1);

                if (dictionaryIndex > dictionary.size()) {
                    dictionary.push_back(Values_[index] - MinValue_);
                }

                Values_[index] = dictionaryIndex;
            }
        }

        // 1. Dictionary - compressed vector of values.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(dictionary), MaxValue_ - MinValue_));

        // 2. Compressed vector of value ids.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(Values_), dictionary.size() + 1));
    }
};

////////////////////////////////////////////////////////////////////////////////

// TValue - i64 or ui64.
template <class TValue>
class TVersionedIntegerColumnWriter
    : public TVersionedColumnWriterBase
    , private TIntegerColumnWriterBase
{
public:
    TVersionedIntegerColumnWriter(int columnId, bool aggregate, TDataBlockWriter* blockWriter)
        : TVersionedColumnWriterBase(columnId, aggregate, blockWriter)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        AddPendingValues(
            rows,
            [&] (const TVersionedValue& value) {
                ui64 data = 0;
                if (value.Type != EValueType::Null) {
                    data = GetEncodedValue(GetValue<TValue>(value));
                    UpdateStatistics(data);
                }
                Values_.push_back(data);
            });

        if (Values_.size() > MaxValueCount) {
            FinishCurrentSegment();
        }
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (TVersionedColumnWriterBase::ValuesPerRow_.empty()) {
            return 0;
        } else {
            return std::min(GetDirectSize(), GetDictionarySize()) +
                   TVersionedColumnWriterBase::GetCurrentSegmentSize();
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (!TVersionedColumnWriterBase::ValuesPerRow_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        TIntegerColumnWriterBase::Reset();
    }

    size_t GetDictionarySize() const
    {
        return
            CompressedUnsignedVectorSizeInBytes(MaxValue_ - MinValue_, DistinctValues_.size()) +
            CompressedUnsignedVectorSizeInBytes(DistinctValues_.size() + 1, Values_.size());
    }

    size_t GetDirectSize() const
    {
        return
            CompressedUnsignedVectorSizeInBytes(MaxValue_ - MinValue_, Values_.size()) +
            NullBitmap_.Size();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_version(0);

        auto* meta = segmentInfo.SegmentMeta.MutableExtension(TIntegerSegmentMeta::integer_segment_meta);
        meta->set_min_value(MinValue_);

        DumpVersionedData(&segmentInfo);

        ui64 dictionarySize = GetDictionarySize();
        ui64 directSize = GetDirectSize();
        if (dictionarySize < directSize) {
            DumpDictionaryValues(&segmentInfo, NullBitmap_);

            segmentInfo.SegmentMeta.set_type(static_cast<int>(segmentInfo.Dense
                ? EVersionedIntegerSegmentType::DictionaryDense
                : EVersionedIntegerSegmentType::DictionarySparse));

        } else {
            DumpDirectValues(&segmentInfo, NullBitmap_);

            segmentInfo.SegmentMeta.set_type(static_cast<int>(segmentInfo.Dense
                ? EVersionedIntegerSegmentType::DirectDense
                : EVersionedIntegerSegmentType::DirectSparse));
        }

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedInt64ColumnWriter(
    int columnId,
    bool aggregate,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TVersionedIntegerColumnWriter<i64>>(
        columnId,
        aggregate,
        dataBlockWriter);
}

std::unique_ptr<IValueColumnWriter> CreateVersionedUint64ColumnWriter(
    int columnId,
    bool aggregate,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TVersionedIntegerColumnWriter<ui64>>(
        columnId,
        aggregate,
        dataBlockWriter);
}

////////////////////////////////////////////////////////////////////////////////

// TValue - i64 or ui64.
template <class TValue>
class TUnversionedIntegerColumnWriter
    : public TColumnWriterBase
    , private TIntegerColumnWriterBase
{
public:
    TUnversionedIntegerColumnWriter(int columnIndex, TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
        , ColumnIndex_(columnIndex)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    virtual void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (Values_.empty()) {
            return 0;
        } else {
            auto sizes = GetSegmentSizeVector();
            auto minElement = std::min_element(sizes.begin(), sizes.end());
            return *minElement;
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (Values_.size() > 0) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;
    i64 RunCount_;

    TAppendOnlyBitmap<ui64> NullBitmap_;

    void Reset()
    {
        TIntegerColumnWriterBase::Reset();

        NullBitmap_ = TAppendOnlyBitmap<ui64>();
        RunCount_ = 0;
    }

    i32 GetSegmentSize(EUnversionedIntegerSegmentType type) const
    {
        switch (type) {
            case EUnversionedIntegerSegmentType::DictionaryRle:
                return
                    CompressedUnsignedVectorSizeInBytes(MaxValue_ - MinValue_, DistinctValues_.size()) +
                    CompressedUnsignedVectorSizeInBytes(DistinctValues_.size() + 1, RunCount_) +
                    CompressedUnsignedVectorSizeInBytes(RowCount_, RunCount_);

            case EUnversionedIntegerSegmentType::DirectRle:
                return
                    CompressedUnsignedVectorSizeInBytes(MaxValue_ - MinValue_, RunCount_) +
                    CompressedUnsignedVectorSizeInBytes(RowCount_, RunCount_) +
                    RunCount_ / 8; // Null bitmap.

            case EUnversionedIntegerSegmentType::DictionaryDense:
                return
                    CompressedUnsignedVectorSizeInBytes(MaxValue_ - MinValue_, DistinctValues_.size()) +
                    CompressedUnsignedVectorSizeInBytes(DistinctValues_.size() + 1, Values_.size());

            case EUnversionedIntegerSegmentType::DirectDense:
                return
                    CompressedUnsignedVectorSizeInBytes(MaxValue_ - MinValue_, Values_.size()) +
                    Values_.size() / 8; // Null bitmap.

            default:
                Y_UNREACHABLE();
        }
    }

    TEnumIndexedVector<i32, EUnversionedIntegerSegmentType> GetSegmentSizeVector() const
    {
        TEnumIndexedVector<i32, EUnversionedIntegerSegmentType> sizes;

        for (auto type : TEnumTraits<EUnversionedIntegerSegmentType>::GetDomainValues()) {
            sizes[type] = GetSegmentSize(type);
        }
        return sizes;
    }

    void DumpDirectRleValues(TSegmentInfo* segmentInfo)
    {
        TAppendOnlyBitmap<ui64> rleNullBitmap(RunCount_);
        std::vector<ui64> rowIndexes;
        rowIndexes.reserve(RunCount_);

        ui64 runIndex = 0;
        ui64 runBegin = 0;

        while (runBegin < Values_.size()) {
            ui64 runEnd = runBegin + 1;
            while (runEnd < Values_.size() &&
                Values_[runBegin] == Values_[runEnd] &&
                NullBitmap_[runBegin] == NullBitmap_[runEnd])
            {
                ++runEnd;
            }

            // For null values store data as 0.
            Values_[runIndex] = NullBitmap_[runBegin] ? 0 : Values_[runBegin] - MinValue_;
            rowIndexes.push_back(runBegin);
            rleNullBitmap.Append(NullBitmap_[runBegin]);

            ++runIndex;
            runBegin = runEnd;
        }

        YCHECK(runIndex == RunCount_);
        Values_.resize(RunCount_);

        // 1. Compressed vector of values.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(Values_), MaxValue_ - MinValue_));

        // 2. Null bitmap of values.
        segmentInfo->Data.push_back(rleNullBitmap.Flush<TSegmentWriterTag>());

        // 3. Compressed vector of row indexes.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(rowIndexes), rowIndexes.back()));
    }

    void DumpDictionaryRleValues(TSegmentInfo* segmentInfo)
    {
        std::vector<ui64> dictionary;
        dictionary.reserve(DistinctValues_.size());

        std::vector<ui64> rowIndexes;
        rowIndexes.reserve(RunCount_);

        ui64 runIndex = 0;
        ui64 runBegin = 0;
        while (runBegin < Values_.size()) {
            ui64 runEnd = runBegin + 1;
            while (runEnd < Values_.size() &&
                Values_[runBegin] == Values_[runEnd] &&
                NullBitmap_[runBegin] == NullBitmap_[runEnd])
            {
                ++runEnd;
            }

            if (NullBitmap_[runBegin]) {
                Values_[runIndex] = 0;
            } else {
                auto dictionaryIndex = DistinctValues_[Values_[runBegin]];
                Y_ASSERT(dictionaryIndex <= dictionary.size() + 1);

                if (dictionaryIndex > dictionary.size()) {
                    dictionary.push_back(Values_[runBegin] - MinValue_);
                }

                Values_[runIndex] = dictionaryIndex;
            }

            rowIndexes.push_back(runBegin);

            ++runIndex;
            runBegin = runEnd;
        }

        YCHECK(runIndex == RunCount_);
        Values_.resize(RunCount_);

        // 1. Dictionary - compressed vector of values.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(dictionary), MaxValue_ - MinValue_));

        // 2. Compressed vector of value ids.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(Values_), dictionary.size() + 1));

        // 3. Compressed vector of row indexes.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(rowIndexes), rowIndexes.back()));
    }

    void DumpSegment()
    {
        auto sizes = GetSegmentSizeVector();

        auto minElement = std::min_element(sizes.begin(), sizes.end());
        auto type = EUnversionedIntegerSegmentType(std::distance(sizes.begin(), minElement));

        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(static_cast<int>(type));
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Values_.size());

        auto* meta = segmentInfo.SegmentMeta.MutableExtension(TIntegerSegmentMeta::integer_segment_meta);
        meta->set_min_value(MinValue_);

        switch (type) {
            case EUnversionedIntegerSegmentType::DirectRle:
                DumpDirectRleValues(&segmentInfo);
                break;

            case EUnversionedIntegerSegmentType::DictionaryRle:
                DumpDictionaryRleValues(&segmentInfo);
                break;

            case EUnversionedIntegerSegmentType::DirectDense:
                DumpDirectValues(&segmentInfo, NullBitmap_);
                break;

            case EUnversionedIntegerSegmentType::DictionaryDense:
                DumpDictionaryValues(&segmentInfo, NullBitmap_);
                break;

            default:
                Y_UNREACHABLE();
        }

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }

    template <class TRow>
    void DoWriteValues(TRange<TRow> rows)
    {
        AddPendingValues(rows);
        if (Values_.size() > MaxValueCount) {
            FinishCurrentSegment();
        }
    }

    template <class TRow>
    void AddPendingValues(const TRange<TRow> rows)
    {
        for (auto row : rows) {
            const auto& value = GetUnversionedValue(row, ColumnIndex_);
            bool isNull = value.Type == EValueType::Null;
            ui64 data = 0;
            if (!isNull) {
                data = GetEncodedValue(GetValue<TValue>(value));
                UpdateStatistics(data);
            }

            if (Values_.empty() ||
                NullBitmap_[Values_.size() - 1] != isNull ||
                Values_.back() != data)
            {
                ++RunCount_;
            }

            Values_.push_back(data);
            NullBitmap_.Append(isNull);
        }

        RowCount_ += rows.Size();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedInt64ColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TUnversionedIntegerColumnWriter<i64>>(
        columnIndex,
        dataBlockWriter);
}

std::unique_ptr<IValueColumnWriter> CreateUnversionedUint64ColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TUnversionedIntegerColumnWriter<ui64>>(
        columnIndex,
        dataBlockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
