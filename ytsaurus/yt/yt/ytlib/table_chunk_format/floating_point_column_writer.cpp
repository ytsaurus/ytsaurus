#include "floating_point_column_writer.h"
#include "column_writer_detail.h"
#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
TSharedRef SerializeFloatingPointVector(const std::vector<T>& values)
{
    auto data = TSharedMutableRef::Allocate<TSegmentWriterTag>(values.size() * sizeof(T) + sizeof(ui64), {.InitializeStorage = false});
    *reinterpret_cast<ui64*>(data.Begin()) = static_cast<ui64>(values.size());
    std::memcpy(
        data.Begin() + sizeof(ui64),
        values.data(),
        values.size() * sizeof(T));
    return data;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TVersionedFloatingPointColumnWriter
    : public TVersionedColumnWriterBase
{
public:
    TVersionedFloatingPointColumnWriter(
        int columnId,
        const TColumnSchema& columnSchema,
        TDataBlockWriter* blockWriter,
        int maxValueCount)
        : TVersionedColumnWriterBase(
            columnId,
            columnSchema,
            blockWriter)
        , MaxValueCount_(maxValueCount)
    {
        Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        AddValues(
            rows,
            [&] (const TVersionedValue& value) {
                Values_.push_back(static_cast<T>(value.Data.Double));
                return std::ssize(Values_) >= MaxValueCount_;
            });
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (ValuesPerRow_.empty()) {
            return 0;
        } else {
            return
                Values_.size() * sizeof(double) +
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
    static_assert(std::is_floating_point_v<T>);
    const int MaxValueCount_;

    std::vector<T> Values_;

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

        segmentInfo.Data.push_back(SerializeFloatingPointVector(Values_));
        segmentInfo.Data.push_back(NullBitmap_.Flush<TSegmentWriterTag>());

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IValueColumnWriter> CreateVersionedFloatingPointColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int mavValueCount)
{
    return std::make_unique<TVersionedFloatingPointColumnWriter<T>>(
        columnId,
        columnSchema,
        blockWriter,
        mavValueCount);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TUnversionedFloatingPointColumnWriter
    : public TColumnWriterBase
{
public:
    TUnversionedFloatingPointColumnWriter(int columnIndex, TDataBlockWriter* blockWriter, int maxValueCount)
        : TColumnWriterBase(blockWriter)
        , ColumnIndex_(columnIndex)
        , MaxValueCount_(maxValueCount)
    {
        static_assert(std::is_floating_point_v<T>);
        Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        DoWriteValues(rows);
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (Values_.empty()) {
            return 0;
        } else {
            return Values_.size() * sizeof(double);
        }
    }

    void FinishCurrentSegment() override
    {
        if (!Values_.empty()) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int ColumnIndex_;
    const int MaxValueCount_;

    std::vector<T> Values_;
    TBitmapOutput NullBitmap_;

    void Reset()
    {
        Values_.clear();
        NullBitmap_ = TBitmapOutput();
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.Data.push_back(SerializeFloatingPointVector(Values_));
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
        AddValues(rows);
        if (std::ssize(Values_) >= MaxValueCount_) {
            FinishCurrentSegment();
        }
    }

    template <class TRow>
    void AddValues(TRange<TRow> rows)
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

template <typename T>
std::unique_ptr<IValueColumnWriter> CreateUnversionedFloatingPointColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter,
    int mavValueCount)
{
    return std::make_unique<TUnversionedFloatingPointColumnWriter<T>>(columnIndex, blockWriter, mavValueCount);
}

////////////////////////////////////////////////////////////////////////////////

template
std::unique_ptr<IValueColumnWriter> CreateUnversionedFloatingPointColumnWriter<float>(
    int columnIndex,
    TDataBlockWriter* blockWriter,
    int mavValueCount);

template
std::unique_ptr<IValueColumnWriter> CreateUnversionedFloatingPointColumnWriter<double>(
    int columnIndex,
    TDataBlockWriter* blockWriter,
    int mavValueCount);

template
std::unique_ptr<IValueColumnWriter> CreateVersionedFloatingPointColumnWriter<float>(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int mavValueCount);

template
std::unique_ptr<IValueColumnWriter> CreateVersionedFloatingPointColumnWriter<double>(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int mavValueCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
