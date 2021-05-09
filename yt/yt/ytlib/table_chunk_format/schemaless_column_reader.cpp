#include "column_reader.h"

#include "column_reader_detail.h"

#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/core/yson/lexer.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSegmentReader
    : public ISegmentReaderBase
{
public:
    TSchemalessSegmentReader(
        TRef data,
        const TSegmentMeta& meta,
        const std::vector<TColumnIdMapping>& idMapping)
        : Meta_(meta)
        , IdMapping_(idMapping)
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

    virtual void SkipToRowIndex(i64 rowIndex) override
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
                ptr += ReadValue(ptr, &value);

                if (value.Type == EValueType::Any) {
                    value = MakeUnversionedValue(
                        TStringBuf(value.Data.String, value.Length),
                        value.Id,
                        Lexer_);
                }

                auto id = IdMapping_[value.Id].ReaderSchemaIndex;
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
    const std::vector<TColumnIdMapping>& IdMapping_;

    const i64 SegmentStartRowIndex_;

    using TOffsetDiffReader = TBitPackedUnsignedVectorReader<ui32, true>;
    using TValueCountReader = TBitPackedUnsignedVectorReader<ui32, true>;

    i64 ExpectedBytesPerRow_;

    TOffsetDiffReader OffsetDiffReader_;
    TValueCountReader ValueCountReader_;

    const char* Data_;

    i64 SegmentRowIndex_ = 0;

    TStatelessLexer Lexer_;

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
        const std::vector<TColumnIdMapping>& idMapping)
        : TColumnReaderBase(meta)
        , IdMapping_(idMapping)
    { }

    virtual void ReadValues(TMutableRange<TMutableUnversionedRow> rows) override
    {
        EnsureCurrentSegmentReader();
        CurrentRowIndex_ += SegmentReader_->ReadValues(rows);
    }

    virtual void ReadValueCounts(TMutableRange<ui32> valueCounts) override
    {
        EnsureCurrentSegmentReader();
        SegmentReader_->ReadValueCounts(valueCounts);
    }

private:
    std::unique_ptr<TSchemalessSegmentReader> SegmentReader_;
    std::vector<TColumnIdMapping> IdMapping_;


    virtual ISegmentReaderBase* GetCurrentSegmentReader() const override
    {
        return SegmentReader_.get();
    }

    virtual void ResetCurrentSegmentReader() override
    {
        SegmentReader_.reset();
    }

    virtual void CreateCurrentSegmentReader() override
    {
        SegmentReader_ = std::make_unique<TSchemalessSegmentReader>(
            TRef(Block_.Begin() + CurrentSegmentMeta().offset(), CurrentSegmentMeta().size()),
            CurrentSegmentMeta(),
            IdMapping_);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISchemalessColumnReader> CreateSchemalessColumnReader(
    const TColumnMeta& meta,
    const std::vector<TColumnIdMapping>& idMapping)
{
    return std::make_unique<TSchemalessColumnReader>(meta, idMapping);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
