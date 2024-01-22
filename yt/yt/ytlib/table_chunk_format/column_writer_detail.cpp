#include "column_writer_detail.h"

#include "data_block_writer.h"
#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TColumnWriterBase::TColumnWriterBase(TDataBlockWriter* blockWriter)
    : BlockWriter_(blockWriter)
{
    BlockWriter_->RegisterColumnWriter(this);
}

TSharedRef TColumnWriterBase::FinishBlock(int blockIndex)
{
    FinishCurrentSegment();

    for (auto& segmentMeta : CurrentBlockSegments_) {
        segmentMeta.set_block_index(blockIndex);

        auto segmentCount = ColumnMeta_.segments_size();
        if (segmentCount > 0) {
            auto currentSegmentStartRowIndex = segmentMeta.chunk_row_count() - segmentMeta.row_count();
            YT_VERIFY(ColumnMeta_.segments(segmentCount - 1).chunk_row_count() == currentSegmentStartRowIndex);
        }

        ColumnMeta_.add_segments()->Swap(&segmentMeta);
    }

    CurrentBlockSegments_.clear();

    size_t metaSize = 0;
    for (const auto& meta : CurrentBlockSegmentMetas_) {
        metaSize += meta.Size();
    }

    auto mergedMeta = TSharedMutableRef::Allocate(metaSize);
    char* metasData = mergedMeta.Begin();

    for (const auto& meta : CurrentBlockSegmentMetas_) {
        std::copy(meta.begin(), meta.end(), metasData);
        metasData += meta.size();
    }

    CurrentBlockSegmentMetas_.clear();

    return mergedMeta;
}

const TColumnMeta& TColumnWriterBase::ColumnMeta() const
{
    return ColumnMeta_;
}

void TColumnWriterBase::DumpSegment(TSegmentInfo* segmentInfo, TSharedRef inBlockMeta)
{
    ui64 size = 0;
    for (const auto& part : segmentInfo->Data)  {
        size += part.Size();
    }
    segmentInfo->SegmentMeta.set_size(size);
    segmentInfo->SegmentMeta.set_offset(BlockWriter_->GetOffset());
    segmentInfo->SegmentMeta.set_chunk_row_count(RowCount_);

    // This is a rough estimate, we don't account extensions, varint coding, etc.
    // We don't want to pay for the ByteSize call.
    MetaSize_ += sizeof(TSegmentMeta);

    CurrentBlockSegments_.push_back(segmentInfo->SegmentMeta);
    BlockWriter_->WriteSegment(MakeRange(segmentInfo->Data));

    CurrentBlockSegmentMetas_.push_back(std::move(inBlockMeta));
}

i64 TColumnWriterBase::GetOffset() const
{
    return BlockWriter_->GetOffset();
}

i64 TColumnWriterBase::GetMetaSize() const
{
    return MetaSize_;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedColumnWriterBase::TVersionedColumnWriterBase(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter)
    : TColumnWriterBase(blockWriter)
    , ColumnId_(columnId)
    , Aggregate_(columnSchema.Aggregate().has_value())
    , Hunk_(columnSchema.MaxInlineHunkSize().has_value())
{ }

i32 TVersionedColumnWriterBase::GetCurrentSegmentSize() const
{
    return
        // Estimate dense size assuming max diff from expected to be 7.
        CompressedUnsignedVectorSizeInBytes(7, ValuesPerRow_.size()) +
        CompressedUnsignedVectorSizeInBytes(MaxTimestampIndex_, TimestampIndexes_.size()) +
        AggregateBitmap_.GetByteSize();
}

void TVersionedColumnWriterBase::WriteUnversionedValues(TRange<NTableClient::TUnversionedRow> /*rows*/)
{
    // Versioned column writers don't support unversioned rows.
    YT_ABORT();
}

void TVersionedColumnWriterBase::Reset()
{
    TimestampIndexes_.clear();
    NullBitmap_ = TBitmapOutput();
    if (Aggregate_) {
        AggregateBitmap_ = TBitmapOutput();
    }
    ValuesPerRow_.clear();

    MaxTimestampIndex_ = 0;
    EmptyPendingRowCount_ = 0;
}

void TVersionedColumnWriterBase::AddValues(
    TRange<TVersionedRow> rows,
    std::function<bool (const TVersionedValue& value)> onValue)
{
    for (auto row : rows) {
        auto values = FindValues(row, ColumnId_);
        ValuesPerRow_.push_back((ValuesPerRow_.empty()
            ? 0
            : ValuesPerRow_.back()) + values.Size());
        ++RowCount_;

        if (values.Empty()) {
            // No values with given column index in current row.
            ++EmptyPendingRowCount_;
            continue;
        }

        bool finishSegment = false;
        for (const auto& value : values) {
            bool isNull = value.Type == EValueType::Null;

            finishSegment |= onValue(value);

            ui32 timestampIndex = GetTimestampIndex(value, row);
            MaxTimestampIndex_ = std::max(MaxTimestampIndex_, timestampIndex);

            TimestampIndexes_.push_back(timestampIndex);
            NullBitmap_.Append(isNull);
            if (Aggregate_) {
                AggregateBitmap_.Append(Any(value.Flags & EValueFlags::Aggregate));
            }
        }

        if (finishSegment) {
            FinishCurrentSegment();
        }
    }
}

void TVersionedColumnWriterBase::DumpVersionedData(TSegmentInfo* segmentInfo, NColumnarChunkFormat::TMultiValueIndexMeta* rawIndexMeta)
{
    ui32 expectedValuesPerRow;
    ui32 maxDiffFromExpected;

    PrepareDiffFromExpected(&ValuesPerRow_, &expectedValuesPerRow, &maxDiffFromExpected);

    auto denseSize = CompressedUnsignedVectorSizeInBytes(
        maxDiffFromExpected,
        ValuesPerRow_.size());

    auto sparseSize = CompressedUnsignedVectorSizeInBytes(
        ValuesPerRow_.size(),
        NullBitmap_.GetBitSize());

    segmentInfo->Dense = denseSize <= sparseSize;

    if (segmentInfo->Dense) {
        auto* denseMeta = segmentInfo->SegmentMeta.MutableExtension(TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
        denseMeta->set_expected_values_per_row(expectedValuesPerRow);

        rawIndexMeta->ExpectedPerRow = expectedValuesPerRow;

        segmentInfo->Data.push_back(BitpackVector(
            MakeRange(ValuesPerRow_),
            maxDiffFromExpected,
            &rawIndexMeta->OffsetsSize,
            &rawIndexMeta->OffsetsWidth));
    } else {
        std::vector<ui64> rowIndexes;
        rowIndexes.reserve(NullBitmap_.GetBitSize());

        rawIndexMeta->ExpectedPerRow = static_cast<ui32>(-1);

        for (int rowIndex = 0; rowIndex < std::ssize(ValuesPerRow_); ++rowIndex) {
            ui32 upperValueIndex = expectedValuesPerRow * (rowIndex + 1) + ZigZagDecode32(ValuesPerRow_[rowIndex]);
            while (rowIndexes.size() < upperValueIndex) {
                rowIndexes.push_back(rowIndex);
            }
        }
        YT_VERIFY(rowIndexes.size() == NullBitmap_.GetBitSize());

        segmentInfo->Data.push_back(BitpackVector(
            MakeRange(rowIndexes),
            rowIndexes.back(),
            &rawIndexMeta->OffsetsSize,
            &rawIndexMeta->OffsetsWidth));
    }

    segmentInfo->Data.push_back(BitpackVector(
        MakeRange(TimestampIndexes_),
        MaxTimestampIndex_,
        &rawIndexMeta->WriteTimestampIdsSize,
        &rawIndexMeta->WriteTimestampIdsWidth));

    if (Aggregate_) {
        segmentInfo->Data.push_back(AggregateBitmap_.Flush<TSegmentWriterTag>());
    }

    rawIndexMeta->RowCount = ValuesPerRow_.size();

    segmentInfo->SegmentMeta.set_row_count(ValuesPerRow_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
