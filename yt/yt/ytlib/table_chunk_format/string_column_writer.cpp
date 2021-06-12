#include "string_column_writer.h"
#include "column_writer_detail.h"
#include "helpers.h"

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>
#include <yt/yt/core/misc/chunked_output_stream.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const int MaxValueCount = 128 * 1024 * 1024;;
static const int MaxBufferSize = 32_MB;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TStringColumnWriterBase
{
protected:
    const bool Hunk_;

    std::unique_ptr<TChunkedOutputStream> DirectBuffer_;
    char* CurrentPreallocated_;

    ui32 MaxValueLength_;
    std::vector<TStringBuf> Values_;

    i64 DictionaryByteSize_;
    THashMap<TStringBuf, ui32> Dictionary_;

    explicit TStringColumnWriterBase(const TColumnSchema& columnSchema = {})
        : Hunk_(columnSchema.MaxInlineHunkSize().has_value())
    { }

    void Reset()
    {
        DirectBuffer_ = std::make_unique<TChunkedOutputStream>();
        CurrentPreallocated_ = nullptr;

        MaxValueLength_ = 0;
        Values_.clear();

        DictionaryByteSize_ = 0;
        Dictionary_.clear();
    }

    void EnsureCapacity(i64 size)
    {
        CurrentPreallocated_ = DirectBuffer_->Preallocate(size);
    }

    std::vector<ui32> GetDirectDenseOffsets() const
    {
        std::vector<ui32> offsets;
        offsets.reserve(Values_.size());

        ui32 offset = 0;
        for (auto value : Values_) {
            offset += value.length();
            offsets.push_back(offset);
        }

        return offsets;
    }

    i64 GetDictionaryByteSize() const
    {
        return
            DictionaryByteSize_ +
            CompressedUnsignedVectorSizeInBytes(MaxValueLength_, Dictionary_.size()) +
            CompressedUnsignedVectorSizeInBytes(Dictionary_.size() + 1, Values_.size());
    }

    i64 GetDirectByteSize() const
    {
        return
            DirectBuffer_->GetSize() +
            CompressedUnsignedVectorSizeInBytes(MaxValueLength_, Values_.size()) +
            Values_.size() / 8;
    }

    TStringBuf CaptureValue(const TUnversionedValue& unversionedValue)
    {
        if (!CurrentPreallocated_) {
            // This means, that we reserved nothing, because all strings are either null or empty.
            // To distinguish between null and empty, we set preallocated pointer to special value.
            static char* const EmptyStringBase = reinterpret_cast<char*>(1);
            CurrentPreallocated_ = EmptyStringBase;
        }

        if (unversionedValue.Type == EValueType::Null) {
            return {};
        }

        char* dst = CurrentPreallocated_;
        if (Hunk_ && None(unversionedValue.Flags & EValueFlags::Hunk)) {
            *dst++ = static_cast<char>(EHunkValueTag::Inline);
        }
        if (IsAnyOrComposite(ValueType) && !IsAnyOrComposite(unversionedValue.Type)) {
            // Any non-any and non-null value convert to YSON.
            dst += WriteYson(dst, unversionedValue);
        } else {
            std::memcpy(
                dst,
                unversionedValue.Data.String,
                unversionedValue.Length);
            dst += unversionedValue.Length;
        }

        auto value = TStringBuf(CurrentPreallocated_, dst);
        auto length = value.length();
        CurrentPreallocated_ = dst;

        DirectBuffer_->Advance(length);

        if (Dictionary_.emplace(value, Dictionary_.size() + 1).second) {
            DictionaryByteSize_ += length;
            MaxValueLength_ = std::max(MaxValueLength_, static_cast<ui32>(length));
        }

        return value;
    }

    void DumpDictionaryValues(TSegmentInfo* segmentInfo)
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DictionaryByteSize_, false);

        std::vector<ui32> dictionaryOffsets;
        dictionaryOffsets.reserve(Dictionary_.size());

        std::vector<ui32> ids;
        ids.reserve(Values_.size());

        ui32 dictionarySize = 0;
        ui32 dictionaryOffset = 0;
        for (auto value : Values_) {
            if (this->IsValueNull(value)) {
                ids.push_back(0);
                continue;
            }

            ui32 id = GetOrCrash(Dictionary_, value);
            ids.push_back(id);

            if (id > dictionarySize) {
                std::memcpy(
                    dictionaryData.Begin() + dictionaryOffset,
                    value.data(),
                    value.length());
                dictionaryOffset += value.length();
                dictionaryOffsets.push_back(dictionaryOffset);
                ++dictionarySize;
            }
        }

        YT_VERIFY(dictionaryOffset == DictionaryByteSize_);

        // 1. Value ids.
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(ids), dictionarySize + 1));

        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&dictionaryOffsets, &expectedLength, &maxDiff);

        // 2. Dictionary offsets.
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(dictionaryOffsets), maxDiff));

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
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(offsets), maxDiff));

        // 2. Null bitmap.
        segmentInfo->Data.push_back(std::move(nullBitmap));

        auto directData = DirectBuffer_->Flush();

        // 3. Direct data.
        segmentInfo->Data.insert(segmentInfo->Data.end(), directData.begin(), directData.end());

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
    }


    static bool IsValueNull(TStringBuf lhs)
    {
        return !lhs.data();
    }

    static bool AreValuesEqual(TStringBuf lhs, TStringBuf rhs)
    {
        if (IsValueNull(lhs) && IsValueNull(rhs)) {
            // Both are null.
            return true;
        } else if (IsValueNull(lhs) || IsValueNull(rhs)) {
            // One is null, and the other is not.
            return false;
        } else {
            // Compare as strings.
            return lhs == rhs;
        }
    }

    i64 GetValueByteSize(const TUnversionedValue& value) const
    {
        if (value.Type == EValueType::Null) {
            return 0;
        }
        YT_ASSERT(IsStringLikeType(value.Type));
        auto result = static_cast<i64>(value.Length);
        if (Hunk_ && None(value.Flags & EValueFlags::Hunk)) {
            result += 1;
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TVersionedStringColumnWriter
    : public TVersionedColumnWriterBase
    , public TStringColumnWriterBase<ValueType>
{
public:
    TVersionedStringColumnWriter(
        int columnId,
        const TColumnSchema& columnSchema,
        TDataBlockWriter* blockWriter)
        : TVersionedColumnWriterBase(
            columnId,
            columnSchema,
            blockWriter)
        , TStringColumnWriterBase<ValueType>(columnSchema)
    {
        this->Reset();
    }

    virtual void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        i64 totalSize = 0;
        for (auto row : rows) {
            for (const auto& value : FindValues(row, ColumnId_)) {
                totalSize += this->GetValueByteSize(value);
            }
        }
        this->EnsureCapacity(totalSize);

        AddValues(
            rows,
            [&] (const TVersionedValue& value) {
                Values_.push_back(this->CaptureValue(value));
            });

        if (Values_.size() > MaxValueCount || DirectBuffer_->GetSize() > MaxBufferSize) {
            FinishCurrentSegment();
        }
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return
                std::min(this->GetDirectByteSize(), this->GetDictionaryByteSize()) +
                TVersionedColumnWriterBase::GetCurrentSegmentSize();
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (!ValuesPerRow_.empty()) {
            this->DumpSegment();
            this->Reset();
        }
    }

private:
    using TStringColumnWriterBase<ValueType>::Values_;
    using TStringColumnWriterBase<ValueType>::Dictionary_;
    using TStringColumnWriterBase<ValueType>::DirectBuffer_;

    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        TStringColumnWriterBase<ValueType>::Reset();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_version(0);

        DumpVersionedData(&segmentInfo);

        i64 dictionaryByteSize = this->GetDictionaryByteSize();
        i64 directByteSize = this->GetDirectByteSize();
        if (dictionaryByteSize < directByteSize) {
            this->DumpDictionaryValues(&segmentInfo);

            segmentInfo.SegmentMeta.set_type(ToProto<int>(segmentInfo.Dense
                ? EVersionedStringSegmentType::DictionaryDense
                : EVersionedStringSegmentType::DictionarySparse));

        } else {
            this->DumpDirectValues(&segmentInfo, NullBitmap_.Flush<TSegmentWriterTag>());

            segmentInfo.SegmentMeta.set_type(ToProto<int>(segmentInfo.Dense
                ? EVersionedStringSegmentType::DirectDense
                : EVersionedStringSegmentType::DirectSparse));
        }

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedStringColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TVersionedStringColumnWriter<EValueType::String>>(
        columnId,
        columnSchema,
        dataBlockWriter);
}

std::unique_ptr<IValueColumnWriter> CreateVersionedAnyColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TVersionedStringColumnWriter<EValueType::Any>>(
        columnId,
        columnSchema,
        dataBlockWriter);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TUnversionedStringColumnWriter
    : public TColumnWriterBase
    , public TStringColumnWriterBase<ValueType>
{
public:
    TUnversionedStringColumnWriter(int columnIndex, TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
        , ColumnIndex_(columnIndex)
    {
        Reset();
    }

    virtual void WriteVersionedValues(TRange<TVersionedRow> rows) override
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
        }

        auto sizes = GetSegmentSizeVector();
        auto minElement = std::min_element(sizes.begin(), sizes.end());
        return *minElement;
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

    using TStringColumnWriterBase<ValueType>::Values_;
    using TStringColumnWriterBase<ValueType>::Dictionary_;
    using TStringColumnWriterBase<ValueType>::DictionaryByteSize_;
    using TStringColumnWriterBase<ValueType>::MaxValueLength_;
    using TStringColumnWriterBase<ValueType>::DirectBuffer_;

    void Reset()
    {
        DirectRleSize_ = 0;
        RleRowIndexes_.clear();
        TStringColumnWriterBase<ValueType>::Reset();
    }

    TSharedRef GetDirectDenseNullBitmap() const
    {
        TBitmapOutput nullBitmap(Values_.size());

        for (auto value : Values_) {
            nullBitmap.Append(this->IsValueNull(value));
        }

        return nullBitmap.Flush<TSegmentWriterTag>();
    }

    void DumpDirectRleData(TSegmentInfo* segmentInfo)
    {
        auto stringData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DirectRleSize_, false);
        std::vector<ui32> offsets;
        offsets.reserve(Dictionary_.size());

        TBitmapOutput nullBitmap(RleRowIndexes_.size());

        ui32 stringOffset = 0;
        for (auto rowIndex : RleRowIndexes_) {
            auto value = Values_[rowIndex];
            nullBitmap.Append(this->IsValueNull(value));
            std::memcpy(
                stringData.Begin() + stringOffset,
                value.data(),
                value.length());
            stringOffset += value.length();
            offsets.push_back(stringOffset);
        }

        YT_VERIFY(stringOffset == DirectRleSize_);

        // 1. Row indexes.
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(RleRowIndexes_), RleRowIndexes_.back()));

        // 2. Value offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(offsets), maxDiff));

        // 3. Null bitmap.
        segmentInfo->Data.push_back(nullBitmap.Flush<TSegmentWriterTag>());

        // 4. String data.
        segmentInfo->Data.push_back(stringData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
    }

    void DumpDictionaryRleData(TSegmentInfo* segmentInfo)
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DictionaryByteSize_, false);

        std::vector<ui32> offsets;
        offsets.reserve(Dictionary_.size());

        std::vector<ui32> ids;
        ids.reserve(RleRowIndexes_.size());

        ui32 dictionaryOffset = 0;
        ui32 dictionarySize = 0;
        for (auto rowIndex : RleRowIndexes_) {
            auto value = Values_[rowIndex];
            if (this->IsValueNull(value)) {
                ids.push_back(0);
                continue;
            }

            ui32 id = GetOrCrash(Dictionary_, value);
            ids.push_back(id);

            if (id > dictionarySize) {
                std::memcpy(
                    dictionaryData.Begin() + dictionaryOffset,
                    value.data(),
                    value.length());
                dictionaryOffset += value.length();
                offsets.push_back(dictionaryOffset);
                ++dictionarySize;
            }
        }

        // 1. Row indexes.
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(RleRowIndexes_), RleRowIndexes_.back()));

        // 2. Value ids.
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(ids), Dictionary_.size()));

        // 3. Dictionary offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);
        segmentInfo->Data.push_back(BitPackUnsignedVector(MakeRange(offsets), maxDiff));

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
        segmentInfo.SegmentMeta.set_type(ToProto<int>(type));
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
                this->DumpDirectValues(&segmentInfo, GetDirectDenseNullBitmap());
                break;

            case EUnversionedStringSegmentType::DictionaryDense:
                this->DumpDictionaryValues(&segmentInfo);
                break;

            default:
                YT_ABORT();
        }

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }

    TEnumIndexedVector<EUnversionedStringSegmentType, i32> GetSegmentSizeVector() const
    {
        TEnumIndexedVector<EUnversionedStringSegmentType, i32> sizes;
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
                    DictionaryByteSize_ +
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
                return this->GetDictionaryByteSize();

            case EUnversionedStringSegmentType::DirectDense:
                return this->GetDirectByteSize();

            default:
                YT_ABORT();
        }
    }

    template <class TRow>
    void DoWriteValues(TRange<TRow> rows)
    {
        AddValues(rows);
        if (Values_.size() > MaxValueCount || DirectBuffer_->GetSize() > MaxBufferSize) {
            FinishCurrentSegment();
        }
    }

    template <class TRow>
    void AddValues(TRange<TRow> rows)
    {
        i64 totalSize = 0;
        for (auto row : rows) {
            const auto& unversionedValue = GetUnversionedValue(row, ColumnIndex_);
            YT_ASSERT(None(unversionedValue.Flags & EValueFlags::Hunk));
            if (unversionedValue.Type == EValueType::Null) {
                continue;
            }
            if constexpr (
                ValueType == EValueType::String ||
                ValueType == EValueType::Composite)
            {
                totalSize += unversionedValue.Length;
            } else {
                static_assert(ValueType == EValueType::Any);
                totalSize += GetYsonSize(unversionedValue);
            }
        }

        this->EnsureCapacity(totalSize);

        for (auto row : rows) {
            const auto& unversionedValue = GetUnversionedValue(row, ColumnIndex_);
            YT_ASSERT(None(unversionedValue.Flags & EValueFlags::Hunk));
            auto value = this->CaptureValue(unversionedValue);
            if (Values_.empty() || !this->AreValuesEqual(value, Values_.back())) {
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
    return std::make_unique<TUnversionedStringColumnWriter<EValueType::String>>(columnIndex, blockWriter);
}

std::unique_ptr<IValueColumnWriter> CreateUnversionedAnyColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TUnversionedStringColumnWriter<EValueType::Any>>(columnIndex, blockWriter);
}

std::unique_ptr<IValueColumnWriter> CreateUnversionedComplexColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TUnversionedStringColumnWriter<EValueType::Composite>>(columnIndex, blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
