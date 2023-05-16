#pragma once

#include "column_reader.h"
#include "column_reader_detail.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TTimestampSegmentReader
    : public ISegmentReaderBase
{
public:
    TTimestampSegmentReader(
        const NProto::TSegmentMeta& meta,
        const char* data,
        NTableClient::TTimestamp timestamp = NTableClient::AllCommittedTimestamp);

    void SkipToRowIndex(i64 rowIndex) override;

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
    }

    std::pair<ui32, ui32> GetFullWriteTimestampIndexRange() const
    {
        return FullWriteTimestampIndexRange_;
    }

    std::pair<ui32, ui32> GetFullDeleteTimestampIndexRange() const
    {
        return FullDeleteTimestampIndexRange_;
    }

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
    TBitPackedUnsignedVectorReader<ui64> DictionaryReader_;

    // Sequence of write timestamp ids.
    TBitPackedUnsignedVectorReader<ui32> WriteIdReader_;

    // Sequence of delete timestamp ids.
    TBitPackedUnsignedVectorReader<ui32> DeleteIdReader_;

    // End of row write id indexes. One value per row.
    TBitPackedUnsignedVectorReader<ui32> WriteIndexReader_;

    // One value per row. Each value is the index of last delete timestamp id for current row.
    TBitPackedUnsignedVectorReader<ui32> DeleteIndexReader_;

    NTableClient::TTimestamp DeleteTimestamp_;
    NTableClient::TTimestamp WriteTimestamp_;
    std::pair<ui32, ui32> TimestampIndexRange_;
    std::pair<ui32, ui32> FullWriteTimestampIndexRange_;
    std::pair<ui32, ui32> FullDeleteTimestampIndexRange_;

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
    TTimestampReaderBase(
        const NProto::TColumnMeta& meta,
        NTableClient::TTimestamp timestamp);

    i64 GetReadyUpperRowIndex() const override
    {
        if (CurrentSegmentIndex_ == ColumnMeta_.segments_size()) {
            return CurrentRowIndex_;
        } else {
            return CurrentSegmentMeta().chunk_row_count();
        }
    }

protected:
    const NTableClient::TTimestamp Timestamp_;

    std::unique_ptr<TTimestampSegmentReader> SegmentReader_;


    ISegmentReaderBase* GetCurrentSegmentReader() const override
    {
        return SegmentReader_.get();
    }

    void ResetCurrentSegmentReader() override
    {
        SegmentReader_.reset();
    }

    void CreateCurrentSegmentReader() override
    {
        SegmentReader_.reset(new TTimestampSegmentReader(
            CurrentSegmentMeta(),
            Block_.Begin() + CurrentSegmentMeta().offset(),
            Timestamp_));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionTimestampReaderBase
    : public TTimestampReaderBase
{
public:
    using TTimestampReaderBase::TTimestampReaderBase;

protected:
    i64 PreparedRowCount_ = 0;

    std::vector<std::pair<ui32, ui32>> TimestampIndexRanges_;
    std::vector<NTableClient::TTimestamp> DeleteTimestamps_;
    std::vector<NTableClient::TTimestamp> WriteTimestamps_;

    void DoPrepareRows(i64 rowCount);
    void DoSkipPreparedRows();
};

////////////////////////////////////////////////////////////////////////////////

class TScanTransactionTimestampReader
    : public TTransactionTimestampReaderBase
{
public:
    using TTransactionTimestampReaderBase::TTransactionTimestampReaderBase;

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
        YT_VERIFY(rowIndex < CurrentRowIndex_ + PreparedRowCount_);
        return DeleteTimestamps_[rowIndex - CurrentRowIndex_];
    }

    NTableClient::TTimestamp GetWriteTimestamp(i64 rowIndex) const
    {
        YT_VERIFY(rowIndex < CurrentRowIndex_ + PreparedRowCount_);
        return WriteTimestamps_[rowIndex - CurrentRowIndex_];
    }

    TRange<std::pair<ui32, ui32>> GetTimestampIndexRanges(i64 rowCount) const
    {
        YT_VERIFY(rowCount <= PreparedRowCount_);
        return MakeRange(
            TimestampIndexRanges_.data(),
            TimestampIndexRanges_.data() + rowCount);
    }

    NTableClient::TTimestamp GetValueTimestamp(i64 rowIndex, ui32 timestampIndex) const
    {
        return SegmentReader_->GetValueTimestamp(rowIndex, timestampIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLookupTransactionTimestampReader
    : public TTransactionTimestampReaderBase
{
public:
    using TTransactionTimestampReaderBase::TTransactionTimestampReaderBase;

    void SkipToRowIndex(i64 rowIndex) override
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

    NTableClient::TTimestamp GetTimestamp(ui32 timestampIndex) const
    {
        return SegmentReader_->GetValueTimestamp(CurrentRowIndex_, timestampIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLookupTransactionAllVersionsTimestampReader
    : public TTransactionTimestampReaderBase
{
public:
    using TTransactionTimestampReaderBase::TTransactionTimestampReaderBase;

    void SkipToRowIndex(i64 rowIndex) override
    {
        DoSkipPreparedRows();
        TTransactionTimestampReaderBase::SkipToRowIndex(rowIndex);
        DoPrepareRows(1);
    }

    std::pair<ui32, ui32> GetWriteTimestampIndexRange() const
    {
        return SegmentReader_->GetFullWriteTimestampIndexRange();
    }

    ui32 GetWriteTimestampCount() const
    {
        auto range = SegmentReader_->GetFullWriteTimestampIndexRange();
        return range.second - range.first;
    }

    ui32 GetDeleteTimestampCount() const
    {
        auto range = SegmentReader_->GetFullDeleteTimestampIndexRange();
        return range.second - range.first;
    }

    NTableClient::TTimestamp GetDeleteTimestamp(ui32 timestampIndex) const
    {
        auto adjustedIndex = SegmentReader_->GetFullDeleteTimestampIndexRange().first + timestampIndex;
        return SegmentReader_->GetDeleteTimestamp(CurrentRowIndex_, adjustedIndex);
    }

    NTableClient::TTimestamp GetValueTimestamp(ui32 timestampIndex) const
    {
        auto adjustedIndex = SegmentReader_->GetFullWriteTimestampIndexRange().first + timestampIndex;
        return SegmentReader_->GetValueTimestamp(CurrentRowIndex_, adjustedIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionTimestampReader
    : public TTimestampReaderBase
{
public:
    explicit TCompactionTimestampReader(const NProto::TColumnMeta& meta)
        : TTimestampReaderBase(meta, NTransactionClient::AllCommittedTimestamp)
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

    NTableClient::TTimestamp GetValueTimestamp(i64 rowIndex, ui32 timestampIndex) const
    {
        return SegmentReader_->GetValueTimestamp(rowIndex, timestampIndex);
    }
private:
    i64 PreparedRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
