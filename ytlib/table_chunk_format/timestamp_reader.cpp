#include "timestamp_reader.h"

#include <yt/core/misc/zigzag.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TTimestampSegmentReader::TTimestampSegmentReader(
    const TSegmentMeta& meta,
    const char* data,
    TTimestamp timestamp)
    : Meta_(meta)
      , TimestampMeta_(Meta_.GetExtension(TTimestampSegmentMeta::timestamp_segment_meta))
      , Timestamp_(timestamp)
{
    SegmentStartRowIndex_ = Meta_.chunk_row_count() - Meta_.row_count();

    DictionaryReader_ = TCompressedUnsignedVectorReader<ui64>(
        reinterpret_cast<const ui64*>(data));
    data += DictionaryReader_.GetByteSize();

    WriteIdReader_ = TCompressedUnsignedVectorReader<ui32>(
        reinterpret_cast<const ui64*>(data));
    data += WriteIdReader_.GetByteSize();

    DeleteIdReader_ = TCompressedUnsignedVectorReader<ui32>(
        reinterpret_cast<const ui64*>(data));
    data += DeleteIdReader_.GetByteSize();

    WriteIndexReader_ = TCompressedUnsignedVectorReader<ui32>(
        reinterpret_cast<const ui64*>(data));
    data += WriteIndexReader_.GetByteSize();

    DeleteIndexReader_ = TCompressedUnsignedVectorReader<ui32>(
        reinterpret_cast<const ui64*>(data));
    data += DeleteIndexReader_.GetByteSize();
}

void TTimestampSegmentReader::SkipToRowIndex(i64 rowIndex)
{
    // Get delete timestamp.
    // Timestamps inside row are sorted in reverse order.
    ui32 lowerDeleteIndex = GetLowerDeleteIndex(rowIndex);
    ui32 upperDeleteIndex = GetLowerDeleteIndex(rowIndex + 1);

    ui32 deleteIndex = LowerBound(
        lowerDeleteIndex,
        upperDeleteIndex,
        [&] (ui32 index) {
            return GetDeleteTimestamp(index) > Timestamp_;
        });

    DeleteTimestamp_ = NullTimestamp;
    if (deleteIndex != upperDeleteIndex) {
        DeleteTimestamp_ = GetDeleteTimestamp(deleteIndex);
    }

    // Get write timestamp.
    ui32 lowerWriteIndex = GetLowerWriteIndex(rowIndex);
    ui32 upperWriteIndex = GetLowerWriteIndex(rowIndex + 1);

    ui32 writeIndex = LowerBound(
        lowerWriteIndex,
        upperWriteIndex,
        [&] (ui32 index) {
            return GetWriteTimestamp(index) > Timestamp_;
        });

    WriteTimestamp_ = NullTimestamp;
    if (writeIndex != upperWriteIndex) {
        WriteTimestamp_ = GetWriteTimestamp(writeIndex);
    }

    ui32 adjustedLowerWriteIndex = writeIndex - lowerWriteIndex;
    ui32 adjustedUpperWriteIndex = adjustedLowerWriteIndex;
    if (WriteTimestamp_ > DeleteTimestamp_) {
        if (DeleteTimestamp_ == NullTimestamp) {
            adjustedUpperWriteIndex = upperWriteIndex;
        } else {
            adjustedUpperWriteIndex = LowerBound(
                writeIndex + 1,
                upperWriteIndex,
                [&] (ui32 index) {
                    return GetWriteTimestamp(index) > DeleteTimestamp_;
                });
        }
        adjustedUpperWriteIndex -= lowerWriteIndex;
    } else {
        WriteTimestamp_ = NullTimestamp;
    }

    TimestampIndexRange_ = std::make_pair(adjustedLowerWriteIndex, adjustedUpperWriteIndex);
}

ui32 TTimestampSegmentReader::GetWriteTimestampCount(i64 rowIndex) const
{
    YCHECK(rowIndex < Meta_.chunk_row_count());
    ui32 lowerWriteIndex = GetLowerWriteIndex(rowIndex);
    ui32 upperWriteIndex = GetLowerWriteIndex(rowIndex + 1);
    return upperWriteIndex - lowerWriteIndex;
}

ui32 TTimestampSegmentReader::GetDeleteTimestampCount(i64 rowIndex) const
{
    ui32 lowerDeleteIndex = GetLowerDeleteIndex(rowIndex);
    ui32 upperDeleteIndex = GetLowerDeleteIndex(rowIndex + 1);
    return upperDeleteIndex - lowerDeleteIndex;
}

TTimestamp TTimestampSegmentReader::GetDeleteTimestamp(i64 rowIndex, ui32 timestampIndex) const
{
    return GetDeleteTimestamp(GetLowerDeleteIndex(rowIndex) + timestampIndex);
}

TTimestamp TTimestampSegmentReader::GetValueTimestamp(i64 rowIndex, ui32 timestampIndex) const
{
    return GetWriteTimestamp(GetLowerWriteIndex(rowIndex) + timestampIndex);
}

ui32 TTimestampSegmentReader::GetDeleteIndex(i64 adjustedRowIndex) const
{
    return TimestampMeta_.expected_deletes_per_row() * (adjustedRowIndex + 1) +
        ZigZagDecode32(DeleteIndexReader_[adjustedRowIndex]);
}

ui32 TTimestampSegmentReader::GetWriteIndex(i64 adjustedRowIndex) const
{
    return TimestampMeta_.expected_writes_per_row() * (adjustedRowIndex + 1) +
        ZigZagDecode32(WriteIndexReader_[adjustedRowIndex]);
}

TTimestamp TTimestampSegmentReader::GetTimestampById(ui32 id) const
{
    return TimestampMeta_.min_timestamp() + DictionaryReader_[id];
}

TTimestamp TTimestampSegmentReader::GetDeleteTimestamp(ui32 deleteIndex) const
{
    ui32 id = DeleteIdReader_[deleteIndex];
    return GetTimestampById(id);
}

TTimestamp TTimestampSegmentReader::GetWriteTimestamp(ui32 writeIndex) const
{
    ui32 id = WriteIdReader_[writeIndex];
    return GetTimestampById(id);
}

ui32 TTimestampSegmentReader::GetLowerDeleteIndex(i64 rowIndex) const
{
    i64 adjustedRowIndex = rowIndex - SegmentStartRowIndex_;
    return (adjustedRowIndex == 0)
        ? 0
        : GetDeleteIndex(adjustedRowIndex - 1);
};

ui32 TTimestampSegmentReader::GetLowerWriteIndex(i64 rowIndex) const
{
    i64 adjustedRowIndex = rowIndex - SegmentStartRowIndex_;
    return (adjustedRowIndex == 0)
        ? 0
        : GetWriteIndex(adjustedRowIndex - 1);
};

////////////////////////////////////////////////////////////////////////////////

TTimestampReaderBase::TTimestampReaderBase(const TColumnMeta& meta)
    : TColumnReaderBase(meta)
{ }

////////////////////////////////////////////////////////////////////////////////

TTransactionTimestampReaderBase::TTransactionTimestampReaderBase(
    const NProto::TColumnMeta& meta,
    NTableClient::TTimestamp timestamp)
    : TTimestampReaderBase(meta)
    , Timestamp_(timestamp)
{ }

void TTransactionTimestampReaderBase::DoPrepareRows(i64 rowCount)
{
    PreparedRowCount_ = rowCount;

    if (!SegmentReader_) {
        InitSegmentReader();
    }

    TimestampIndexRanges_.reserve(rowCount);
    DeleteTimestamps_.reserve(rowCount);
    WriteTimestamps_.reserve(rowCount);

    for (i64 index = 0; index < rowCount; ++index) {
        SegmentReader_->SkipToRowIndex(CurrentRowIndex_ + index);
        TimestampIndexRanges_.push_back(SegmentReader_->GetTimestampIndexRange());
        DeleteTimestamps_.push_back(SegmentReader_->GetDeleteTimestamp());
        WriteTimestamps_.push_back(SegmentReader_->GetWriteTimestamp());
    }
}

void TTransactionTimestampReaderBase::DoSkipPreparedRows()
{
    TimestampIndexRanges_.clear();
    DeleteTimestamps_.clear();
    WriteTimestamps_.clear();
    CurrentRowIndex_ += PreparedRowCount_;
    PreparedRowCount_ = 0;

    if (CurrentRowIndex_ == CurrentSegmentMeta().chunk_row_count()) {
        SegmentReader_.reset();
        ++CurrentSegmentIndex_;
    }
}

void TTransactionTimestampReaderBase::InitSegmentReader()
{
    SegmentReader_.reset(new TTimestampSegmentReader(
        CurrentSegmentMeta(),
        Block_.Begin() + CurrentSegmentMeta().offset(),
        Timestamp_));
}

////////////////////////////////////////////////////////////////////////////////

void TCompactionTimestampReader::PrepareRows(i64 rowCount)
{
    PreparedRowCount_ = rowCount;

    if (!SegmentReader_) {
        InitSegmentReader();
    }
}

void TCompactionTimestampReader::SkipPreparedRows()
{
    CurrentRowIndex_ += PreparedRowCount_;
    PreparedRowCount_ = 0;

    if (CurrentRowIndex_ == CurrentSegmentMeta().chunk_row_count()) {
        SegmentReader_.reset();
        ++CurrentSegmentIndex_;
    }
}

void TCompactionTimestampReader::InitSegmentReader()
{
    SegmentReader_.reset(new TTimestampSegmentReader(
        CurrentSegmentMeta(),
        Block_.Begin() + CurrentSegmentMeta().offset()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT

