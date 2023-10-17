#include "string_column_writer.h"
#include "data_block_writer.h"
#include "column_writer_detail.h"
#include "helpers.h"

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TStringColumnWriterBufferTag { };

static const int MaxBufferSize = 32_MB;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TStringColumnWriterBase
{
protected:
    const bool Hunk_;

    std::unique_ptr<TChunkedOutputStream> DirectBuffer_;

    ui32 MaxValueLength_;
    std::vector<TStringBuf> Values_;

    i64 DictionaryByteSize_;
    THashMap<TStringBuf, ui32> Dictionary_;

    explicit TStringColumnWriterBase(const TColumnSchema& columnSchema)
        : Hunk_(columnSchema.MaxInlineHunkSize().has_value())
    { }

    void Reset()
    {
        DirectBuffer_ = std::make_unique<TChunkedOutputStream>(
            GetRefCountedTypeCookie<TStringColumnWriterBufferTag>(),
            256_KB,
            1_MB);

        MaxValueLength_ = 0;
        Values_.clear();

        DictionaryByteSize_ = 0;
        Dictionary_.clear();
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
        if (unversionedValue.Type == EValueType::Null) {
            return {};
        }

        auto valueCapacity = IsAnyOrComposite(ValueType) && !IsAnyOrComposite(unversionedValue.Type)
            ? GetYsonSize(unversionedValue)
            : static_cast<i64>(unversionedValue.Length);

        if (Hunk_ && None(unversionedValue.Flags & EValueFlags::Hunk)) {
            valueCapacity += 1;
        }

        char* buffer = DirectBuffer_->Preallocate(valueCapacity);
        if (!buffer) {
            // This means, that we reserved nothing, because all strings are either null or empty.
            // To distinguish between null and empty, we set preallocated pointer to special value.
            static char* const EmptyStringBase = reinterpret_cast<char*>(1);
            buffer = EmptyStringBase;
        }

        auto start = buffer;

        if (Hunk_ && None(unversionedValue.Flags & EValueFlags::Hunk)) {
            *buffer++ = static_cast<char>(EHunkValueTag::Inline);
        }
        if (IsAnyOrComposite(ValueType) && !IsAnyOrComposite(unversionedValue.Type)) {
            // Any non-any and non-null value convert to YSON.
            buffer += WriteYson(buffer, unversionedValue);
        } else {
            std::memcpy(
                buffer,
                unversionedValue.Data.String,
                unversionedValue.Length);
            buffer += unversionedValue.Length;
        }

        auto value = TStringBuf(start, buffer);

        YT_VERIFY(value.size() <= valueCapacity);

        DirectBuffer_->Advance(value.size());

        if (Dictionary_.emplace(value, Dictionary_.size() + 1).second) {
            DictionaryByteSize_ += value.size();
            MaxValueLength_ = std::max(MaxValueLength_, static_cast<ui32>(value.size()));
        }

        return value;
    }

    void DumpDictionaryValues(TSegmentInfo* segmentInfo, NNewTableClient::TBlobMeta* rawBlobMeta)
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DictionaryByteSize_, {.InitializeStorage = false});

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

        rawBlobMeta->Direct = false;

        // 1. Value ids.
        segmentInfo->Data.push_back(BitpackVector(MakeRange(ids), dictionarySize + 1, &rawBlobMeta->IdsSize, &rawBlobMeta->IdsWidth));

        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&dictionaryOffsets, &expectedLength, &maxDiff);

        // 2. Dictionary offsets.
        segmentInfo->Data.push_back(BitpackVector(MakeRange(dictionaryOffsets), maxDiff, &rawBlobMeta->OffsetsSize, &rawBlobMeta->OffsetsWidth));

        // 3. Dictionary data.
        segmentInfo->Data.push_back(dictionaryData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
        rawBlobMeta->ExpectedLength = expectedLength;
    }

    void DumpDirectValues(TSegmentInfo* segmentInfo, TSharedRef nullBitmap, NNewTableClient::TBlobMeta* rawBlobMeta)
    {
        auto offsets = GetDirectDenseOffsets();

        // Save offsets as diff from expected.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);

        rawBlobMeta->Direct = true;

        // 1. Direct offsets.
        segmentInfo->Data.push_back(BitpackVector(MakeRange(offsets), maxDiff, &rawBlobMeta->OffsetsSize, &rawBlobMeta->OffsetsWidth));

        // 2. Null bitmap.
        segmentInfo->Data.push_back(std::move(nullBitmap));

        auto directData = DirectBuffer_->Finish();

        // 3. Direct data.
        segmentInfo->Data.insert(segmentInfo->Data.end(), directData.begin(), directData.end());

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
        rawBlobMeta->ExpectedLength = expectedLength;
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
        TDataBlockWriter* blockWriter,
        int maxValueCount)
        : TVersionedColumnWriterBase(
            columnId,
            columnSchema,
            blockWriter)
        , TStringColumnWriterBase<ValueType>(columnSchema)
        , MaxValueCount_(maxValueCount)
    {
        this->Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        AddValues(
            rows,
            [&] (const TVersionedValue& value) {
                Values_.push_back(this->CaptureValue(value));
                return std::ssize(Values_) >= MaxValueCount_ || DirectBuffer_->GetSize() > MaxBufferSize;
            });
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return
                std::min(this->GetDirectByteSize(), this->GetDictionaryByteSize()) +
                TVersionedColumnWriterBase::GetCurrentSegmentSize();
        }
    }

    void FinishCurrentSegment() override
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

    const int MaxValueCount_;

    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        TStringColumnWriterBase<ValueType>::Reset();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_version(0);

        NNewTableClient::TValueMeta<ValueType> rawMeta;
        memset(&rawMeta, 0, sizeof(rawMeta));
        rawMeta.DataOffset = TColumnWriterBase::GetOffset();
        rawMeta.ChunkRowCount = RowCount_;

        DumpVersionedData(&segmentInfo, &rawMeta);

        i64 dictionaryByteSize = this->GetDictionaryByteSize();
        i64 directByteSize = this->GetDirectByteSize();
        if (dictionaryByteSize < directByteSize) {
            this->DumpDictionaryValues(&segmentInfo, &rawMeta);

            segmentInfo.SegmentMeta.set_type(ToProto<int>(segmentInfo.Dense
                ? EVersionedStringSegmentType::DictionaryDense
                : EVersionedStringSegmentType::DictionarySparse));

        } else {
            this->DumpDirectValues(&segmentInfo, NullBitmap_.Flush<TSegmentWriterTag>(), &rawMeta);

            segmentInfo.SegmentMeta.set_type(ToProto<int>(segmentInfo.Dense
                ? EVersionedStringSegmentType::DirectDense
                : EVersionedStringSegmentType::DirectSparse));
        }

        TColumnWriterBase::DumpSegment(&segmentInfo, TSharedRef::MakeCopy<TSegmentWriterTag>(MetaToRef(rawMeta)));

        if (BlockWriter_->GetEnableSegmentMetaInBlocks()) {
            VerifyRawVersionedSegmentMeta(segmentInfo.SegmentMeta, segmentInfo.Data, rawMeta, Aggregate_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedStringColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount)
{
    return std::make_unique<TVersionedStringColumnWriter<EValueType::String>>(
        columnId,
        columnSchema,
        dataBlockWriter,
        maxValueCount);
}

std::unique_ptr<IValueColumnWriter> CreateVersionedAnyColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount)
{
    return std::make_unique<TVersionedStringColumnWriter<EValueType::Any>>(
        columnId,
        columnSchema,
        dataBlockWriter,
        maxValueCount);
}

std::unique_ptr<IValueColumnWriter> CreateVersionedCompositeColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount)
{
    return std::make_unique<TVersionedStringColumnWriter<EValueType::Composite>>(
        columnId,
        columnSchema,
        dataBlockWriter,
        maxValueCount);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TUnversionedStringColumnWriter
    : public TColumnWriterBase
    , public TStringColumnWriterBase<ValueType>
{
public:
    TUnversionedStringColumnWriter(
        int columnIndex,
        const TColumnSchema& columnSchema,
        TDataBlockWriter* blockWriter,
        int maxValueCount)
        : TColumnWriterBase(blockWriter)
        , TStringColumnWriterBase<ValueType>(columnSchema)
        , ColumnIndex_(columnIndex)
        , MaxValueCount_(maxValueCount)
    {
        Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (Values_.empty()) {
            return 0;
        }

        auto sizes = GetSegmentSizeVector();
        auto minElement = std::min_element(sizes.begin(), sizes.end());
        return *minElement;
    }

    void FinishCurrentSegment() override
    {
        if (!Values_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;
    const int MaxValueCount_;

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

    void DumpDirectRleData(TSegmentInfo* segmentInfo, NNewTableClient::TKeyIndexMeta* rawIndexMeta, NNewTableClient::TBlobMeta* rawBlobMeta)
    {
        auto stringData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DirectRleSize_, {.InitializeStorage = false});
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

        rawBlobMeta->Direct = true;

        // 1. Row indexes.
        segmentInfo->Data.push_back(BitpackVector(MakeRange(RleRowIndexes_), RleRowIndexes_.back(), &rawIndexMeta->RowIndexesSize, &rawIndexMeta->RowIndexesWidth));

        // 2. Value offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);
        segmentInfo->Data.push_back(BitpackVector(MakeRange(offsets), maxDiff, &rawBlobMeta->OffsetsSize, &rawBlobMeta->OffsetsWidth));

        // 3. Null bitmap.
        segmentInfo->Data.push_back(nullBitmap.Flush<TSegmentWriterTag>());

        // 4. String data.
        segmentInfo->Data.push_back(stringData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
        rawBlobMeta->ExpectedLength = expectedLength;
    }

    void DumpDictionaryRleData(TSegmentInfo* segmentInfo, NNewTableClient::TKeyIndexMeta* rawIndexMeta, NNewTableClient::TBlobMeta* rawBlobMeta)
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TSegmentWriterTag>(DictionaryByteSize_, {.InitializeStorage = false});

        std::vector<ui32> offsets;
        offsets.reserve(Dictionary_.size());

        std::vector<ui32> ids;
        ids.reserve(RleRowIndexes_.size());

        rawBlobMeta->Direct = false;

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
        segmentInfo->Data.push_back(BitpackVector(MakeRange(RleRowIndexes_), RleRowIndexes_.back(), &rawIndexMeta->RowIndexesSize, &rawIndexMeta->RowIndexesWidth));

        // 2. Value ids.
        segmentInfo->Data.push_back(BitpackVector(MakeRange(ids), Dictionary_.size(), &rawBlobMeta->IdsSize, &rawBlobMeta->IdsWidth));

        // 3. Dictionary offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);
        segmentInfo->Data.push_back(BitpackVector(MakeRange(offsets), maxDiff, &rawBlobMeta->OffsetsSize, &rawBlobMeta->OffsetsWidth));

        // 4. Dictionary data.
        segmentInfo->Data.push_back(dictionaryData);

        auto* stringSegmentMeta = segmentInfo->SegmentMeta.MutableExtension(TStringSegmentMeta::string_segment_meta);
        stringSegmentMeta->set_expected_length(expectedLength);
        rawBlobMeta->ExpectedLength = expectedLength;
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

        NNewTableClient::TKeyMeta<ValueType> rawMeta;
        memset(&rawMeta, 0, sizeof(rawMeta));
        rawMeta.DataOffset = TColumnWriterBase::GetOffset();
        rawMeta.RowCount = Values_.size();
        rawMeta.ChunkRowCount = RowCount_;

        switch (type) {
            case EUnversionedStringSegmentType::DirectRle:
                rawMeta.Dense = false;
                DumpDirectRleData(&segmentInfo, &rawMeta, &rawMeta);
                break;

            case EUnversionedStringSegmentType::DictionaryRle:
                rawMeta.Dense = false;
                DumpDictionaryRleData(&segmentInfo, &rawMeta, &rawMeta);
                break;

            case EUnversionedStringSegmentType::DirectDense:
                rawMeta.Dense = true;
                this->DumpDirectValues(&segmentInfo, GetDirectDenseNullBitmap(), &rawMeta);
                break;

            case EUnversionedStringSegmentType::DictionaryDense:
                rawMeta.Dense = true;
                this->DumpDictionaryValues(&segmentInfo, &rawMeta);
                break;

            default:
                YT_ABORT();
        }

        TColumnWriterBase::DumpSegment(&segmentInfo, TSharedRef::MakeCopy<TSegmentWriterTag>(MetaToRef(rawMeta)));

        if (BlockWriter_->GetEnableSegmentMetaInBlocks()) {
            VerifyRawSegmentMeta(segmentInfo.SegmentMeta, segmentInfo.Data, rawMeta);
        }
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
    }

    template <class TRow>
    void AddValues(TRange<TRow> rows)
    {
        for (auto row : rows) {
            const auto& unversionedValue = GetUnversionedValue(row, ColumnIndex_);
            auto value = this->CaptureValue(unversionedValue);
            if (Values_.empty() || !this->AreValuesEqual(value, Values_.back())) {
                DirectRleSize_ += value.length();
                RleRowIndexes_.push_back(Values_.size());
            }
            Values_.push_back(value);
            ++RowCount_;

            if (std::ssize(Values_) >= MaxValueCount_ || DirectBuffer_->GetSize() > MaxBufferSize) {
                FinishCurrentSegment();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedStringColumnWriter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount)
{
    return std::make_unique<TUnversionedStringColumnWriter<EValueType::String>>(columnIndex, columnSchema, blockWriter, maxValueCount);
}

std::unique_ptr<IValueColumnWriter> CreateUnversionedAnyColumnWriter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount)
{
    return std::make_unique<TUnversionedStringColumnWriter<EValueType::Any>>(columnIndex, columnSchema, blockWriter, maxValueCount);
}

std::unique_ptr<IValueColumnWriter> CreateUnversionedCompositeColumnWriter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount)
{
    return std::make_unique<TUnversionedStringColumnWriter<EValueType::Composite>>(columnIndex, columnSchema, blockWriter, maxValueCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
