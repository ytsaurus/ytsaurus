#include "boolean_column_writer.h"

#include "column_writer_detail.h"
#include "compressed_integer_vector.h"
#include "helpers.h"

#include <yt/client/table_client/versioned_row.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void DumpBooleanValues(
    TSegmentInfo* segmentInfo, 
    TAppendOnlyBitmap<ui64>& valueBitmap, 
    TAppendOnlyBitmap<ui64> nullBitmap)
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
    TVersionedBooleanColumnWriter(int columnId, bool aggregate, TDataBlockWriter* blockWriter)
        : TVersionedColumnWriterBase(columnId, aggregate, blockWriter)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        AddPendingValues(
            rows,
            [&] (const TVersionedValue& value) {
                bool isNull = value.Type == EValueType::Null;
                bool data = isNull ? false : value.Data.Boolean;
                Values_.Append(data);
            });
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return Values_.Size() +
               NullBitmap_.Size() +
               TVersionedColumnWriterBase::GetCurrentSegmentSize();
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (!ValuesPerRow_.empty()) {
            DumpSegment();
            Reset();
        }
    }   

private:
    TAppendOnlyBitmap<ui64> Values_;

    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        Values_ = TAppendOnlyBitmap<ui64>();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);

        DumpVersionedData(&segmentInfo);
        DumpBooleanValues(&segmentInfo, Values_, NullBitmap_);
        TColumnWriterBase::DumpSegment(&segmentInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedBooleanColumnWriter(
    int columnId, 
    bool aggregate, 
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TVersionedBooleanColumnWriter>(
        columnId, 
        aggregate, 
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

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        AddPendingValues(rows);
    }

    virtual void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        AddPendingValues(rows);
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (Values_.GetBitSize() > 0) {
            return Values_.Size() + NullBitmap_.Size();
        } else {
            return 0;
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (Values_.GetBitSize() > 0) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;

    TAppendOnlyBitmap<ui64> Values_;
    TAppendOnlyBitmap<ui64> NullBitmap_;

    void Reset()
    {
        Values_ = TAppendOnlyBitmap<ui64>();
        NullBitmap_ = TAppendOnlyBitmap<ui64>();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Values_.GetBitSize());

        DumpBooleanValues(&segmentInfo, Values_, NullBitmap_);

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }

    template <class TRow>
    void AddPendingValues(TRange<TRow> rows)
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
