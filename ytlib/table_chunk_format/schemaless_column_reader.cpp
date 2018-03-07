#include "column_reader.h"

#include "column_reader_detail.h"

#include <yt/ytlib/table_client/helpers.h>
#include <yt/core/yson/lexer.h>

namespace NYT {
namespace NTableChunkFormat {

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
        YCHECK(GetSegmentRowIndex(rowIndex) >= SegmentRowIndex_);
        SegmentRowIndex_ = GetSegmentRowIndex(rowIndex);
    }

    virtual bool EndOfSegment() const override
    {
        return SegmentRowIndex_ == Meta_.row_count();
    }

    i64 ReadValues(TMutableRange<TMutableUnversionedRow> rows)
    {
        YCHECK(SegmentRowIndex_ + rows.Size() <= Meta_.row_count());
        for (i64 rowIndex = 0; rowIndex < rows.Size(); ++rowIndex) {
            ui32 offset = GetOffset(SegmentRowIndex_ + rowIndex);
            const char* ptr = Data_ + offset;

            for (int index = 0; index < ValueCountReader_[SegmentRowIndex_ + rowIndex]; ++index) {
                TUnversionedValue value;
                ptr += ReadValue(ptr, &value);

                if (value.Type == EValueType::Any) {
                    value = MakeUnversionedValue(
                        TStringBuf(value.Data.String, value.Length),
                        value.Id,
                        Lexer_);
                }

                if (IdMapping_[value.Id].ReaderSchemaIndex >= 0) {
                    value.Id = IdMapping_[value.Id].ReaderSchemaIndex;
                    *rows[rowIndex].End() = value;
                    rows[rowIndex].SetCount(rows[rowIndex].GetCount() + 1);
                }
            }
        }
        SegmentRowIndex_ += rows.Size();
        return rows.Size();
    }

    void GetValueCounts(TMutableRange<ui32> valueCounts)
    {
        YCHECK(SegmentRowIndex_ + valueCounts.Size() <= Meta_.row_count());
        for (i64 rowIndex = 0; rowIndex < valueCounts.Size(); ++rowIndex) {
            valueCounts[rowIndex] = ValueCountReader_[SegmentRowIndex_ + rowIndex];
        }
    }

private:
    const NProto::TSegmentMeta& Meta_;
    const std::vector<TColumnIdMapping>& IdMapping_;

    const i64 SegmentStartRowIndex_;

    using TOffsetDiffReader = TCompressedUnsignedVectorReader<ui32, true>;
    using TValueCountReader = TCompressedUnsignedVectorReader<ui32, true>;

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
        EnsureSegmentReader();
        CurrentRowIndex_ += SegmentReader_->ReadValues(rows);

        if (SegmentReader_->EndOfSegment()) {
            SegmentReader_.reset();
            ++CurrentSegmentIndex_;
        }
    }

    virtual void GetValueCounts(TMutableRange<ui32> valueCounts) override
    {
        EnsureSegmentReader();
        SegmentReader_->GetValueCounts(valueCounts);
    }

private:
    std::unique_ptr<TSchemalessSegmentReader> SegmentReader_;

    std::vector<TColumnIdMapping> IdMapping_;

    virtual void ResetSegmentReader() override
    {
        SegmentReader_.reset();
    }

    void EnsureSegmentReader()
    {
        if (!SegmentReader_) {
            const auto& meta = ColumnMeta_.segments(CurrentSegmentIndex_);
            SegmentReader_ = std::make_unique<TSchemalessSegmentReader>(
                TRef(Block_.Begin() + meta.offset(), meta.size()),
                meta,
                IdMapping_);
        }
        SegmentReader_->SkipToRowIndex(CurrentRowIndex_);
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

} // namespace NTableChunkFormat
} // namespace NYT
