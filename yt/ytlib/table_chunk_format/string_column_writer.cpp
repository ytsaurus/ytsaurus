#include "string_column_writer.h"
#include "column_writer_detail.h"
#include "helpers.h"
#include "compressed_integer_vector.h"

#include <yt/ytlib/table_client/versioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

const int MaxValueCount = 128 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TStringColumnWriterBase
{
protected:
    std::unique_ptr<TChunkedOutputStream> DirectBuffer_;
    char* CurrentPreallocated_;

    std::vector<TStringBuf> Values_;

    ui32 MaxValueLength_;

    i64 DictionarySize_;
    yhash_map<TStringBuf, ui32> Dictionary_;


    void Reset()
    {
        DictionarySize_ = 0;

        Values_.clear();
        Dictionary_.clear();

        DirectBuffer_ = std::make_unique<TChunkedOutputStream>();
        CurrentPreallocated_ = nullptr;

        MaxValueLength_ = 0;
        Values_.clear();
    }

    void EnsureCapacity(size_t size)
    {
        CurrentPreallocated_ = DirectBuffer_->Preallocate(size);
    }

    std::vector<ui32> GetDirectDenseOffsets() const
    {
        std::vector<ui32> offsets;
        offsets.reserve(Values_.size());

        ui32 offset = 0;
        for (const auto& value : Values_) {
            offset += value.length();
            offsets.push_back(offset);
        }
        return offsets;
    }



    bool EqualValues(const TStringBuf& lhs, const TStringBuf& rhs) const
    {
        if (lhs.Data() == nullptr && rhs.Data() == nullptr) {
            // Both are null.
            return true;
        } else if (lhs.Data() == nullptr || rhs.Data() == nullptr) {
            // One is null, and the other is not.
            return false;
        } else {
            // Compare as strings.
            return lhs == rhs;
        }
    }

    size_t GetDictionarySize() const
    {
        return
            DictionarySize_ +
            CompressedUnsignedVectorSizeInBytes(MaxValueLength_, Dictionary_.size()) +
            CompressedUnsignedVectorSizeInBytes(Dictionary_.size() + 1, Values_.size());
    }

    size_t GetDirectSize() const
    {
        return
            DirectBuffer_->GetSize() +
            CompressedUnsignedVectorSizeInBytes(MaxValueLength_, Values_.size()) +
            Values_.size() / 8;
    }

    TStringBuf UpdateStatistics(const TUnversionedValue& unversionedValue)
    {
        bool isNull = unversionedValue.Type == EValueType::Null;

        TStringBuf value = TStringBuf(nullptr, nullptr);

        if (!isNull) {
            value = TStringBuf(CurrentPreallocated_, unversionedValue.Length);
            std::memcpy(
                CurrentPreallocated_,
                unversionedValue.Data.String,
                unversionedValue.Length);
            CurrentPreallocated_ += unversionedValue.Length;
            DirectBuffer_->Advance(unversionedValue.Length);

            auto pair = Dictionary_.insert(std::make_pair(value, Dictionary_.size() + 1));

            if (pair.second) {
                DictionarySize_ += unversionedValue.Length;
                MaxValueLength_ = std::max(MaxValueLength_, unversionedValue.Length);
            }
        }

        return value;
    }

    void DumpDictionaryValues(TSegmentInfo* segmentInfo)
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DictionarySize_, false);
        std::vector<ui32> dictionaryOffsets;
        dictionaryOffsets.reserve(Dictionary_.size());

        std::vector<ui32> ids;
        ids.reserve(Values_.size());

        int dictionarySize = 0;
        ui32 dictionaryOffset = 0;
        for (const auto& value : Values_) {
            if (value.Data() == nullptr) {
                ids.push_back(0);
            } else {
                auto it = Dictionary_.find(value);
                YCHECK(it != Dictionary_.end());
                ids.push_back(it->second);

                if (it->second > dictionarySize) {
                    std::memcpy(
                        dictionaryData.Begin() + dictionaryOffset,
                        value.Data(),
                        value.length());

                    dictionaryOffset += value.length();
                    dictionaryOffsets.push_back(dictionaryOffset);
                    ++dictionarySize;
                }
            }
        }

        YCHECK(dictionaryOffset == DictionarySize_);

        // 1. Value ids.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(ids), dictionarySize + 1));

        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&dictionaryOffsets, &expectedLength, &maxDiff);

        // 2. Dictionary offsets.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(dictionaryOffsets), maxDiff));

        // 3. Dictionary data.
        segmentInfo->Data.push_back(dictionaryData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
    }

    void DumpDirectValues(TSegmentInfo* segmentInfo, TSharedRef nullBitmap)
    {
        auto offsets = GetDirectDenseOffsets();

        // Save offsets as diff from expected.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);

        // 1. Direct offsets.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(offsets), maxDiff));

        // 2. Null bitmap.
        segmentInfo->Data.push_back(std::move(nullBitmap));

        auto directData = DirectBuffer_->Flush();

        // 3. Direct data.
        segmentInfo->Data.insert(segmentInfo->Data.end(), directData.begin(), directData.end());

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedStringColumnWriter
    : public TVersionedColumnWriterBase
    , private TStringColumnWriterBase
{
public:
    TVersionedStringColumnWriter(int columnId, bool aggregate, TDataBlockWriter* blockWriter)
        : TVersionedColumnWriterBase(columnId, aggregate, blockWriter)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        size_t cumulativeSize = 0;
        for (auto row : rows) {
            for (const auto& value : FindValues(row, ColumnId_)) {
                if (value.Type != EValueType::Null) {
                    cumulativeSize += value.Length;
                }
            }
        }
        EnsureCapacity(cumulativeSize);

        AddPendingValues(
            rows,
            [&](const TVersionedValue& value) {
                auto stringBuf = UpdateStatistics(value);
                Values_.push_back(stringBuf);
            });

        if (Values_.size() > MaxValueCount) {
            FinishCurrentSegment();
        }
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return std::min(GetDirectSize(), GetDictionarySize()) +
                   TVersionedColumnWriterBase::GetCurrentSegmentSize();
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (!ValuesPerRow_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        TStringColumnWriterBase::Reset();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_version(0);

        DumpVersionedData(&segmentInfo);

        ui64 dictionarySize = GetDictionarySize();
        ui64 directSize = GetDirectSize();
        if (dictionarySize < directSize) {
            DumpDictionaryValues(&segmentInfo);

            segmentInfo.SegmentMeta.set_type(static_cast<int>(segmentInfo.Dense
                ? EVersionedStringSegmentType::DictionaryDense
                : EVersionedStringSegmentType::DictionarySparse));

        } else {
            DumpDirectValues(&segmentInfo, NullBitmap_.Flush<TSegmentWriterTag>());

            segmentInfo.SegmentMeta.set_type(static_cast<int>(segmentInfo.Dense
                ? EVersionedStringSegmentType::DirectDense
                : EVersionedStringSegmentType::DirectSparse));
        }

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedStringColumnWriter(
    int columnId,
    bool aggregate,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TVersionedStringColumnWriter>(
        columnId,
        aggregate,
        dataBlockWriter);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedStringColumnWriter
    : public TColumnWriterBase
    , private TStringColumnWriterBase
{
public:
    TUnversionedStringColumnWriter(int columnIndex, TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
        , ColumnIndex_(columnIndex)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        AddPendingValues(rows);
        if (Values_.size() > MaxValueCount) {
            FinishCurrentSegment();
        }
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
        if (!Values_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;

    i64 DirectRleSize_;
    std::vector<ui64> RleRowIndexes_;

    void Reset()
    {
        DirectRleSize_ = 0;
        RleRowIndexes_.clear();
        TStringColumnWriterBase::Reset();
    }

    TSharedRef GetDirectDenseNullBitmap() const
    {
        TAppendOnlyBitmap<ui64> nullBitmap(Values_.size());

        for (const auto& value : Values_) {
            nullBitmap.Append(value.Data() == nullptr);
        }

        return nullBitmap.Flush<TSegmentWriterTag>();
    }

    void DumpDirectRleData(TSegmentInfo* segmentInfo)
    {
        auto stringData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DirectRleSize_, false);
        std::vector<ui32> offsets;
        offsets.reserve(Dictionary_.size());

        TAppendOnlyBitmap<ui64> nullBitmap(RleRowIndexes_.size());

        ui32 stringOffset = 0;
        for (auto rowIndex : RleRowIndexes_) {
            nullBitmap.Append(Values_[rowIndex].Data() == nullptr);
            std::memcpy(
                stringData.Begin() + stringOffset,
                Values_[rowIndex].Data(),
                Values_[rowIndex].length());
            stringOffset += Values_[rowIndex].length();
            offsets.push_back(stringOffset);
        }

        YCHECK(stringOffset == DirectRleSize_);

        // 1. Row indexes.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(RleRowIndexes_), RleRowIndexes_.back()));

        // 2. Value offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(offsets), maxDiff));

        // 3. Null bitmap.
        segmentInfo->Data.push_back(nullBitmap.Flush<TSegmentWriterTag>());

        // 4. String data.
        segmentInfo->Data.push_back(stringData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
    }

    void DumpDictionaryRleData(TSegmentInfo* segmentInfo)
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DictionarySize_, false);
        std::vector<ui32> offsets;
        offsets.reserve(Dictionary_.size());

        std::vector<ui32> ids;
        ids.reserve(RleRowIndexes_.size());

        ui32 dictionaryOffset = 0;
        ui32 dictionarySize = 0;
        for (auto rowIndex : RleRowIndexes_) {
            const auto& value = Values_[rowIndex];
            if (value.Data() == nullptr) {
                ids.push_back(0);
            } else {
                auto it = Dictionary_.find(Values_[rowIndex]);
                YCHECK(it != Dictionary_.end());
                ids.push_back(it->second);

                if (it->second > dictionarySize) {
                    std::memcpy(
                        dictionaryData.Begin() + dictionaryOffset,
                        Values_[rowIndex].Data(),
                        Values_[rowIndex].length());

                    dictionaryOffset += Values_[rowIndex].length();
                    offsets.push_back(dictionaryOffset);
                    ++dictionarySize;
                }
            }
        }

        // 1. Row indexes.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(RleRowIndexes_), RleRowIndexes_.back()));

        // 2. Value ids.
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(ids), Dictionary_.size()));

        // 3. Dictionary offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);
        segmentInfo->Data.push_back(CompressUnsignedVector(MakeRange(offsets), maxDiff));

        // 4. Dictionary data.
        segmentInfo->Data.push_back(dictionaryData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
    }

    void DumpSegment()
    {
        auto sizes = GetSegmentSizeVector();

        auto minElement = std::min_element(sizes.begin(), sizes.end());
        auto type = EUnversionedStringSegmentType(std::distance(sizes.begin(), minElement));

        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(static_cast<int>(type));
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Values_.size());

        switch (type) {
            case EUnversionedStringSegmentType::DirectRle:
                DumpDirectRleData(&segmentInfo);
                break;

            case EUnversionedStringSegmentType::DictionaryRle:
                DumpDictionaryRleData(&segmentInfo);
                break;

            case EUnversionedStringSegmentType::DirectDense:
                DumpDirectValues(&segmentInfo, GetDirectDenseNullBitmap());
                break;

            case EUnversionedStringSegmentType::DictionaryDense:
                DumpDictionaryValues(&segmentInfo);
                break;

            default:
                YUNREACHABLE();
        }

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }

    TEnumIndexedVector<i32, EUnversionedStringSegmentType> GetSegmentSizeVector() const
    {
        TEnumIndexedVector<i32, EUnversionedStringSegmentType> sizes;
        for (auto type : TEnumTraits<EUnversionedStringSegmentType>::GetDomainValues()) {
            sizes[type] = GetSegmentSize(type);
        }
        return sizes;
    }

    i32 GetSegmentSize(EUnversionedStringSegmentType type) const
    {
        switch (type) {
            case EUnversionedStringSegmentType::DictionaryRle:
                return
                    DictionarySize_ +
                    // This is estimate. We will keep diff from expected offset.
                    CompressedUnsignedVectorSizeInBytes(MaxValueLength_, Dictionary_.size()) +
                    CompressedUnsignedVectorSizeInBytes(Dictionary_.size() + 1, RleRowIndexes_.size()) +
                    CompressedUnsignedVectorSizeInBytes(Values_.size(), RleRowIndexes_.size());

            case EUnversionedStringSegmentType::DirectRle:
                return
                    DirectRleSize_ +
                    CompressedUnsignedVectorSizeInBytes(MaxValueLength_, RleRowIndexes_.size()) +
                    CompressedUnsignedVectorSizeInBytes(Values_.size(), RleRowIndexes_.size()) +
                    Values_.size() / 8; // Null bitmaps.

            case EUnversionedStringSegmentType::DictionaryDense:
                return GetDictionarySize();

            case EUnversionedStringSegmentType::DirectDense:
                return GetDirectSize();

            default:
                YUNREACHABLE();
        }
    }

    void AddPendingValues(const TRange<TVersionedRow> rows)
    {
        size_t cumulativeSize = 0;
        for (auto row : rows) {
            const auto& unversionedValue = GetUnversionedValue(row, ColumnIndex_);
            if (unversionedValue.Type != EValueType::Null) {
                YASSERT(unversionedValue.Type == EValueType::String);
                cumulativeSize += unversionedValue.Length;
            }
        }

        EnsureCapacity(cumulativeSize);

        for (auto row : rows) {
            const auto& unversionedValue = GetUnversionedValue(row, ColumnIndex_);
            TStringBuf value = UpdateStatistics(unversionedValue);

            if (Values_.empty() || !EqualValues(value, Values_.back())) {
                DirectRleSize_ += value.length();
                RleRowIndexes_.push_back(Values_.size());
            }

            Values_.push_back(value);
        }

        RowCount_ += rows.Size();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedStringColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TUnversionedStringColumnWriter>(columnIndex, blockWriter);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
