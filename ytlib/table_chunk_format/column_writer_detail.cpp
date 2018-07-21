#include "column_writer_detail.h"

#include "data_block_writer.h"
#include "helpers.h"
#include "compressed_integer_vector.h"

#include <yt/client/table_client/versioned_row.h>

#include <yt/core/misc/zigzag.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TColumnWriterBase::TColumnWriterBase(TDataBlockWriter* blockWriter)
    : BlockWriter_(blockWriter)
{
    BlockWriter_->RegisterColumnWriter(this);
}

void TColumnWriterBase::FinishBlock(int blockIndex)
{
    FinishCurrentSegment();

    for (auto& segmentMeta : CurrentBlockSegments_) {
        segmentMeta.set_block_index(blockIndex);
        ColumnMeta_.add_segments()->Swap(&segmentMeta);
    }

    CurrentBlockSegments_.clear();
}

const TColumnMeta& TColumnWriterBase::ColumnMeta() const
{
    return ColumnMeta_;
}

void TColumnWriterBase::DumpSegment(TSegmentInfo* segmentInfo)
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
}

i64 TColumnWriterBase::GetMetaSize() const
{
    return MetaSize_;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedColumnWriterBase::TVersionedColumnWriterBase(int columnId, bool aggregate, TDataBlockWriter* blockWriter)
    : TColumnWriterBase(blockWriter)
    , ColumnId_(columnId)
    , Aggregate_(aggregate)
{ }

i32 TVersionedColumnWriterBase::GetCurrentSegmentSize() const
{
    return
        // Estimate dense size assuming max diff from expected to be 7.
        CompressedUnsignedVectorSizeInBytes(7, ValuesPerRow_.size()) +
        CompressedUnsignedVectorSizeInBytes(MaxTimestampIndex_, TimestampIndexes_.size()) +
        AggregateBitmap_.Size();
}

void TVersionedColumnWriterBase::WriteUnversionedValues(TRange<NTableClient::TUnversionedRow> rows)
{
    // Versioned column writers don't support unversioned rows.
    Y_UNREACHABLE();
}

 void TVersionedColumnWriterBase::Reset()
 {
     TimestampIndexes_.clear();
     NullBitmap_ = TAppendOnlyBitmap<ui64>();
     if (Aggregate_) {
         AggregateBitmap_ = TAppendOnlyBitmap<ui64>();
     }
     ValuesPerRow_.clear();

     MaxTimestampIndex_ = 0;
     EmptyPendingRowCount_ = 0;
 }

void TVersionedColumnWriterBase::AddPendingValues(
    const TRange<TVersionedRow> rows, 
    std::function<void (const TVersionedValue& value)> onValue)
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

        for (const auto& value : values) {
            
            bool isNull = value.Type == EValueType::Null;

            onValue(value);

            ui32 timestampIndex = GetTimestampIndex(value, row);
            MaxTimestampIndex_ = std::max(MaxTimestampIndex_, timestampIndex);

            TimestampIndexes_.push_back(timestampIndex);
            NullBitmap_.Append(isNull);
            if (Aggregate_) {
                AggregateBitmap_.Append(value.Aggregate);
            }
        }
    }
}

void TVersionedColumnWriterBase::DumpVersionedData(TSegmentInfo* segmentInfo)
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

        segmentInfo->Data.push_back(CompressUnsignedVector(
            MakeRange(ValuesPerRow_),
            maxDiffFromExpected));
    } else {
        std::vector<ui64> rowIndexes;
        rowIndexes.reserve(NullBitmap_.GetBitSize());

        for (int rowIndex = 0; rowIndex < ValuesPerRow_.size(); ++rowIndex) {
            ui32 upperValueIndex = expectedValuesPerRow * (rowIndex + 1) + ZigZagDecode32(ValuesPerRow_[rowIndex]);
            while (rowIndexes.size() < upperValueIndex) {
                rowIndexes.push_back(rowIndex);
            }
        }
        YCHECK(rowIndexes.size() == NullBitmap_.GetBitSize());

        segmentInfo->Data.push_back(CompressUnsignedVector(
            MakeRange(rowIndexes),
            rowIndexes.back()));
    }

    segmentInfo->Data.push_back(CompressUnsignedVector(
        MakeRange(TimestampIndexes_),
        MaxTimestampIndex_));

    if (Aggregate_) {
        segmentInfo->Data.push_back(AggregateBitmap_.Flush<TSegmentWriterTag>());
    }

    segmentInfo->SegmentMeta.set_row_count(ValuesPerRow_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
