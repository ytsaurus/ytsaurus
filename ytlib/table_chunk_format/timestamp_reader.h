#pragma once

#include "column_reader.h"
#include "compressed_integer_vector.h"

#include "column_reader_detail.h"

#include <yt/ytlib/table_chunk_format/column_meta.pb.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TTimestampSegmentReader
{
public:
    TTimestampSegmentReader(
        const NProto::TSegmentMeta& meta, 
        const char* data, 
        NTableClient::TTimestamp timestamp = NTableClient::AllCommittedTimestamp);

    void SkipToRowIndex(i64 rowIndex);

    NTableClient::TTimestamp GetDeleteTimestamp() const
    {
        return DeleteTimestamp_;
    }

    NTableClient::TTimestamp GetWriteTimestamp() const
    {
        return WriteTimestamp_;
    }

    std::pair<ui32, ui32> GetTimestampIndexRange() const
    {
        return TimestampIndexRange_;
    };

    NTableClient::TTimestamp GetValueTimestamp(i64 rowIndex, ui32 timestampIndex) const;
    NTableClient::TTimestamp GetDeleteTimestamp(i64 rowIndex, ui32 timestampIndex) const;

    ui32 GetWriteTimestampCount(i64 rowIndex) const;
    ui32 GetDeleteTimestampCount(i64 rowIndex) const;

private:
    const NProto::TSegmentMeta& Meta_;
    const NProto::TTimestampSegmentMeta& TimestampMeta_;

    const NTableClient::TTimestamp Timestamp_;

    i64 SegmentStartRowIndex_;

    // Dictionary id -> timestamp.
    TCompressedUnsignedVectorReader<ui64> DictionaryReader_;

    // Sequence of write timestamp ids.
    TCompressedUnsignedVectorReader<ui32> WriteIdReader_;

    // Sequence of delete timestamp ids.
    TCompressedUnsignedVectorReader<ui32> DeleteIdReader_;

    // End of row write id indexes. One value per row.
    TCompressedUnsignedVectorReader<ui32> WriteIndexReader_;

    // One value per row. Each value is the index of last delete timestamp id for current row.
    TCompressedUnsignedVectorReader<ui32> DeleteIndexReader_;

    NTableClient::TTimestamp DeleteTimestamp_;
    NTableClient::TTimestamp WriteTimestamp_;
    std::pair<ui32, ui32> TimestampIndexRange_;


    ui32 GetDeleteIndex(i64 adjustedRowIndex) const;
    ui32 GetWriteIndex(i64 adjustedRowIndex) const;

    NTableClient::TTimestamp GetTimestampById(ui32 id) const;
    NTableClient::TTimestamp GetWriteTimestamp(ui32 writeIndex) const;
    NTableClient::TTimestamp GetDeleteTimestamp(ui32 deleteIndex) const;

    ui32 GetLowerWriteIndex(i64 rowIndex) const;
    ui32 GetLowerDeleteIndex(i64 rowIndex) const;
};

////////////////////////////////////////////////////////////////////////////////

class TTimestampReaderBase
    : public TColumnReaderBase
{
public:
    explicit TTimestampReaderBase(const NProto::TColumnMeta& meta);

    virtual i64 GetReadyUpperRowIndex() const override
    {
        if (CurrentSegmentIndex_ == ColumnMeta_.segments_size()) {
            return CurrentRowIndex_;
        } else {
            return CurrentSegmentMeta().chunk_row_count();
        }
    }

protected:
    std::unique_ptr<TTimestampSegmentReader> SegmentReader_;

    virtual void ResetSegmentReader() override
    {
        SegmentReader_.reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionTimestampReaderBase
    : public TTimestampReaderBase
{
public:
    TTransactionTimestampReaderBase(
        const NProto::TColumnMeta& meta,
        NTableClient::TTimestamp timestamp);

protected:
    NTableClient::TTimestamp Timestamp_;

    i64 PreparedRowCount_ = 0;

    std::vector<std::pair<ui32, ui32>> TimestampIndexRanges_;
    std::vector<NTableClient::TTimestamp> DeleteTimestamps_;
    std::vector<NTableClient::TTimestamp> WriteTimestamps_;

    void DoPrepareRows(i64 rowCount);

    void DoSkipPreparedRows();

    void InitSegmentReader();
};

////////////////////////////////////////////////////////////////////////////////

class TScanTransactionTimestampReader
    : public TTransactionTimestampReaderBase
{
public:
    TScanTransactionTimestampReader(
        const NProto::TColumnMeta& meta,
        NTableClient::TTimestamp timestamp)
        : TTransactionTimestampReaderBase(meta, timestamp)
    { }

    void PrepareRows(i64 rowCount)
    {
        DoPrepareRows(rowCount);
    }

    void SkipPreparedRows()
    {
        DoSkipPreparedRows();
    }

    NTableClient::TTimestamp GetDeleteTimestamp(i64 rowIndex) const
    {
        YCHECK(rowIndex < CurrentRowIndex_ + PreparedRowCount_);
        return DeleteTimestamps_[rowIndex - CurrentRowIndex_];
    }

    NTableClient::TTimestamp GetWriteTimestamp(i64 rowIndex) const
    {
        YCHECK(rowIndex < CurrentRowIndex_ + PreparedRowCount_);
        return WriteTimestamps_[rowIndex - CurrentRowIndex_];
    }

    TRange<std::pair<ui32, ui32>> GetTimestampIndexRanges(i64 rowCount) const
    {
        YCHECK(rowCount <= PreparedRowCount_);
        return MakeRange(
            TimestampIndexRanges_.data(),
            TimestampIndexRanges_.data() + rowCount);
    };

    NTableClient::TTimestamp GetValueTimestamp(i64 rowIndex, i64 timestampIndex) const
    {
        return SegmentReader_->GetValueTimestamp(rowIndex, timestampIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLookupTransactionTimestampReader
    : public TTransactionTimestampReaderBase
{
public:
    TLookupTransactionTimestampReader(
        const NProto::TColumnMeta& meta,
        NTableClient::TTimestamp timestamp)
        : TTransactionTimestampReaderBase(meta, timestamp)
    { }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        DoSkipPreparedRows();
        TTransactionTimestampReaderBase::SkipToRowIndex(rowIndex);
        DoPrepareRows(1);
    }

    NTableClient::TTimestamp GetDeleteTimestamp() const
    {
        return DeleteTimestamps_[0];
    }

    NTableClient::TTimestamp GetWriteTimestamp() const
    {
        return WriteTimestamps_[0];
    }

    std::pair<ui32, ui32> GetTimestampIndexRange() const
    {
        return TimestampIndexRanges_[0];
    }

    NTableClient::TTimestamp GetTimestamp(i64 timestampIndex) const
    {
        return SegmentReader_->GetValueTimestamp(CurrentRowIndex_, timestampIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionTimestampReader
    : public TTimestampReaderBase
{
public:
    explicit TCompactionTimestampReader(const NProto::TColumnMeta& meta)
        : TTimestampReaderBase(meta)
    { }

    void PrepareRows(i64 rowCount);

    void SkipPreparedRows();

    ui32 GetWriteTimestampCount(i64 rowIndex) const
    {
        return SegmentReader_->GetWriteTimestampCount(rowIndex);
    }

    ui32 GetDeleteTimestampCount(i64 rowIndex) const
    {
        return SegmentReader_->GetDeleteTimestampCount(rowIndex);
    }

    NTableClient::TTimestamp GetDeleteTimestamp(i64 rowIndex, ui32 timestampIndex) const
    {
        return SegmentReader_->GetDeleteTimestamp(rowIndex, timestampIndex);
    }

    NTableClient::TTimestamp GetValueTimestamp(i64 rowIndex, i64 timestampIndex) const
    {
        return SegmentReader_->GetValueTimestamp(rowIndex, timestampIndex);
    }
private:
    i64 PreparedRowCount_ = 0;

    void InitSegmentReader();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
