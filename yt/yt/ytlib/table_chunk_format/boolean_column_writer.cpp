#include "boolean_column_writer.h"
#include "data_block_writer.h"
#include "column_writer_detail.h"
#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void DumpBooleanValues(
    TSegmentInfo* segmentInfo,
    TBitmapOutput& valueBitmap,
    // Copy?
    TBitmapOutput nullBitmap)
{
    ui64 valueCount = valueBitmap.GetBitSize();
    segmentInfo->Data.push_back(TSharedRef::MakeCopy<TSegmentWriterTag>(TRef::FromPod(valueCount)));
    segmentInfo->Data.push_back(valueBitmap.Flush<TSegmentWriterTag>());
    segmentInfo->Data.push_back(nullBitmap.Flush<TSegmentWriterTag>());
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedBooleanColumnWriter
    : public TVersionedColumnWriterBase
{
public:
    TVersionedBooleanColumnWriter(
        int columnId,
        const TColumnSchema& columnSchema,
        TDataBlockWriter* blockWriter)
        : TVersionedColumnWriterBase(
            columnId,
            columnSchema,
            blockWriter)
    {
        Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        AddValues(
            rows,
            [&] (const TVersionedValue& value) {
                bool isNull = value.Type == EValueType::Null;
                bool data = isNull ? false : value.Data.Boolean;
                Values_.Append(data);
                return false;
            });
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return Values_.GetByteSize() +
               NullBitmap_.GetByteSize() +
               TVersionedColumnWriterBase::GetCurrentSegmentSize();
        }
    }

    void FinishCurrentSegment() override
    {
        if (!ValuesPerRow_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    TBitmapOutput Values_;

    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        Values_ = TBitmapOutput();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);

        NNewTableClient::TValueMeta<EValueType::Boolean> rawMeta;
        memset(&rawMeta, 0, sizeof(rawMeta));
        rawMeta.DataOffset = TColumnWriterBase::GetOffset();
        rawMeta.ChunkRowCount = RowCount_;

        DumpVersionedData(&segmentInfo, &rawMeta);
        DumpBooleanValues(&segmentInfo, Values_, NullBitmap_);
        TColumnWriterBase::DumpSegment(&segmentInfo, TSharedRef::MakeCopy<TSegmentWriterTag>(MetaToRef(rawMeta)));

        if (BlockWriter_->GetEnableSegmentMetaInBlocks()) {
            VerifyRawVersionedSegmentMeta(segmentInfo.SegmentMeta, segmentInfo.Data, rawMeta, Aggregate_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedBooleanColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TVersionedBooleanColumnWriter>(
        columnId,
        columnSchema,
        blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedBooleanColumnWriter
    : public TColumnWriterBase
{
public:
    TUnversionedBooleanColumnWriter(int columnIndex, TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
        , ColumnIndex_(columnIndex)
    {
        Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        AddValues(rows);
    }

    void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        AddValues(rows);
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (Values_.GetBitSize() > 0) {
            return Values_.GetByteSize() + NullBitmap_.GetByteSize();
        } else {
            return 0;
        }
    }

    void FinishCurrentSegment() override
    {
        if (Values_.GetBitSize() > 0) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;

    TBitmapOutput Values_;
    TBitmapOutput NullBitmap_;

    void Reset()
    {
        Values_ = TBitmapOutput();
        NullBitmap_ = TBitmapOutput();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Values_.GetBitSize());

        DumpBooleanValues(&segmentInfo, Values_, NullBitmap_);

        NNewTableClient::TKeyMeta<EValueType::Boolean> rawMeta;
        memset(&rawMeta, 0, sizeof(rawMeta));
        rawMeta.DataOffset = TColumnWriterBase::GetOffset();
        rawMeta.ChunkRowCount = RowCount_;
        rawMeta.RowCount = Values_.GetBitSize();
        rawMeta.Dense = true;

        TColumnWriterBase::DumpSegment(&segmentInfo, TSharedRef::MakeCopy<TSegmentWriterTag>(MetaToRef(rawMeta)));

        if (BlockWriter_->GetEnableSegmentMetaInBlocks()) {
            VerifyRawSegmentMeta(segmentInfo.SegmentMeta, segmentInfo.Data, rawMeta);
        }
    }

    template <class TRow>
    void AddValues(TRange<TRow> rows)
    {
        for (auto row : rows) {
            ++RowCount_;

            const auto& value = GetUnversionedValue(row, ColumnIndex_);
            bool isNull = value.Type == EValueType::Null;
            bool data = isNull ? false : value.Data.Boolean;
            NullBitmap_.Append(isNull);
            Values_.Append(data);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedBooleanColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TUnversionedBooleanColumnWriter>(columnIndex, blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
