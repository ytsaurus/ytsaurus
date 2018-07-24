#include "double_column_writer.h"
#include "column_writer_detail.h"
#include "helpers.h"

#include "compressed_integer_vector.h"

#include <yt/client/table_client/versioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

const int MaxValueCount = 128 * 1024;

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeDoubleVector(const std::vector<double>& values)
{
    auto data = TSharedMutableRef::Allocate<TSegmentWriterTag>(values.size() * sizeof(double) + sizeof(ui64), false);
    *reinterpret_cast<ui64*>(data.Begin()) = static_cast<ui64>(values.size());
    std::memcpy(
        data.Begin() + sizeof(ui64),
        values.data(),
        values.size() * sizeof(double));
    return data;
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedDoubleColumnWriter
    : public TVersionedColumnWriterBase
{
public:
    TVersionedDoubleColumnWriter(int columnId, bool aggregate, TDataBlockWriter* blockWriter)
        : TVersionedColumnWriterBase(columnId, aggregate, blockWriter)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        AddPendingValues(
            rows,
            [&] (const TVersionedValue& value) {
                Values_.push_back(value.Data.Double);
            });

        if (Values_.size() > MaxValueCount) {
            FinishCurrentSegment();
        }
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return Values_.size() * sizeof(double) +
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
    std::vector<double> Values_;

    void Reset()
    {
        TVersionedColumnWriterBase::Reset();
        Values_.clear();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);

        DumpVersionedData(&segmentInfo);

        segmentInfo.Data.push_back(SerializeDoubleVector(Values_));
        segmentInfo.Data.push_back(NullBitmap_.Flush<TSegmentWriterTag>());

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedDoubleColumnWriter(
    int columnId, 
    bool aggregate, 
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TVersionedDoubleColumnWriter>(
        columnId, 
        aggregate, 
        blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedDoubleColumnWriter
    : public TColumnWriterBase
{
public:
    TUnversionedDoubleColumnWriter(int columnIndex, TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
        , ColumnIndex_(columnIndex)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    virtual void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (Values_.empty()) {
            return 0;
        } else {
            return Values_.size() * sizeof(double);
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (!Values_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;

    std::vector<double> Values_;
    TAppendOnlyBitmap<ui64> NullBitmap_;

    void Reset()
    {
        Values_.clear();
        NullBitmap_ = TAppendOnlyBitmap<ui64>();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.Data.push_back(SerializeDoubleVector(Values_));
        segmentInfo.Data.push_back(NullBitmap_.Flush<TSegmentWriterTag>());

        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Values_.size());
        segmentInfo.SegmentMeta.set_chunk_row_count(RowCount_);
        
        TColumnWriterBase::DumpSegment(&segmentInfo);
    }

    template <class TRow>
    void DoWriteValues(TRange<TRow> rows)
    {
        AddPendingValues(rows);
        if (Values_.size() > MaxValueCount) {
            FinishCurrentSegment();
        }
    }

    template <class TRow>
    void AddPendingValues(TRange<TRow> rows)
    {
        for (auto row : rows) {
            ++RowCount_;

            const auto& value = GetUnversionedValue(row, ColumnIndex_);
            NullBitmap_.Append(value.Type == EValueType::Null);
            Values_.push_back(value.Data.Double);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedDoubleColumnWriter(
    int columnIndex, 
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TUnversionedDoubleColumnWriter>(columnIndex, blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
