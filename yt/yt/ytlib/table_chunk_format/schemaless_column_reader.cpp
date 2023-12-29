#include "column_reader.h"

#include "column_reader_detail.h"

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSegmentReader
    : public ISegmentReaderBase
{
public:
    TSchemalessSegmentReader(
        TRef data,
        const TSegmentMeta& meta,
        const std::vector<int>& chunkToReaderIdMapping)
        : Meta_(meta)
        , ChunkToReaderIdMapping_(chunkToReaderIdMapping)
        , SegmentStartRowIndex_(meta.chunk_row_count() - meta.row_count())
    {
        const char* ptr = data.Begin();

        OffsetDiffReader_ = TOffsetDiffReader(reinterpret_cast<const ui64*>(ptr));
        ptr += OffsetDiffReader_.GetByteSize();

        ValueCountReader_ = TValueCountReader(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueCountReader_.GetByteSize();

        Data_ = ptr;

        const auto& metaExt = Meta_.GetExtension(TSchemalessSegmentMeta::schemaless_segment_meta);
        ExpectedBytesPerRow_ = metaExt.expected_bytes_per_row();
    }

    void SkipToRowIndex(i64 rowIndex) override
    {
        YT_VERIFY(GetSegmentRowIndex(rowIndex) >= SegmentRowIndex_);
        SegmentRowIndex_ = GetSegmentRowIndex(rowIndex);
    }

    i64 ReadValues(TMutableRange<TMutableUnversionedRow> rows)
    {
        YT_VERIFY(SegmentRowIndex_ + std::ssize(rows) <= Meta_.row_count());
        for (i64 rowIndex = 0; rowIndex < std::ssize(rows); ++rowIndex) {
            ui32 offset = GetOffset(SegmentRowIndex_ + rowIndex);
            const char* ptr = Data_ + offset;

            for (int index = 0; index < static_cast<int>(ValueCountReader_[SegmentRowIndex_ + rowIndex]); ++index) {
                TUnversionedValue value;
                ptr += ReadRowValue(ptr, &value);

                if (value.Type == EValueType::Any) {
                    value = TryDecodeUnversionedAnyValue(value);
                }

                auto id = ChunkToReaderIdMapping_[value.Id];
                if (id >= 0) {
                    value.Id = id;
                    *rows[rowIndex].End() = value;
                    rows[rowIndex].SetCount(rows[rowIndex].GetCount() + 1);
                }
            }
        }
        SegmentRowIndex_ += rows.Size();
        return rows.Size();
    }

    void ReadValueCounts(TMutableRange<ui32> valueCounts)
    {
        YT_VERIFY(SegmentRowIndex_ + std::ssize(valueCounts) <= Meta_.row_count());
        for (i64 rowIndex = 0; rowIndex < std::ssize(valueCounts); ++rowIndex) {
            valueCounts[rowIndex] = ValueCountReader_[SegmentRowIndex_ + rowIndex];
        }
    }

private:
    const NProto::TSegmentMeta& Meta_;
    const std::vector<int>& ChunkToReaderIdMapping_;

    const i64 SegmentStartRowIndex_;

    using TOffsetDiffReader = TBitPackedUnsignedVectorReader<ui32, true>;
    using TValueCountReader = TBitPackedUnsignedVectorReader<ui32, true>;

    i64 ExpectedBytesPerRow_;

    TOffsetDiffReader OffsetDiffReader_;
    TValueCountReader ValueCountReader_;

    const char* Data_;

    i64 SegmentRowIndex_ = 0;

    i64 GetSegmentRowIndex(i64 rowIndex) const
    {
        return rowIndex - SegmentStartRowIndex_;
    }

    ui32 GetOffset(i64 segmentRowIndex) const
    {
        return ExpectedBytesPerRow_ * (segmentRowIndex + 1) +
            ZigZagDecode32(OffsetDiffReader_[segmentRowIndex]);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessColumnReader
    : public TColumnReaderBase
    , public ISchemalessColumnReader
{
public:
    TSchemalessColumnReader(
        const TColumnMeta& meta,
        const std::vector<int>& chunkToReaderIdMapping)
        : TColumnReaderBase(meta)
        , ChunkToReaderIdMapping_(chunkToReaderIdMapping)
    { }

    void ReadValues(TMutableRange<TMutableUnversionedRow> rows) override
    {
        EnsureCurrentSegmentReader();
        CurrentRowIndex_ += SegmentReader_->ReadValues(rows);
    }

    void ReadValueCounts(TMutableRange<ui32> valueCounts) override
    {
        EnsureCurrentSegmentReader();
        SegmentReader_->ReadValueCounts(valueCounts);
    }

private:
    std::unique_ptr<TSchemalessSegmentReader> SegmentReader_;
    std::vector<int> ChunkToReaderIdMapping_;


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
        SegmentReader_ = std::make_unique<TSchemalessSegmentReader>(
            TRef(Block_.Begin() + CurrentSegmentMeta().offset(), CurrentSegmentMeta().size()),
            CurrentSegmentMeta(),
            ChunkToReaderIdMapping_);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISchemalessColumnReader> CreateSchemalessColumnReader(
    const TColumnMeta& meta,
    const std::vector<int>& chunkToReaderIdMapping)
{
    return std::make_unique<TSchemalessColumnReader>(
        meta,
        chunkToReaderIdMapping);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
