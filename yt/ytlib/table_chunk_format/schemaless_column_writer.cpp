#include "schemaless_column_writer.h"

#include "column_writer_detail.h"
#include "compressed_integer_vector.h"
#include "helpers.h"

#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

const int MaxRowCount = 128 * 1024;
const int MaxBufferSize = 32 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessColumnWriter
    : public TColumnWriterBase
{
public:
    TSchemalessColumnWriter(int schemaColumnCount, TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
        , SchemaColumnCount_(schemaColumnCount)
    {
        Reset();
    }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        Y_UNREACHABLE();
    }

    virtual void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        AddPendingValues(rows);
        if (Offsets_.size() > MaxRowCount || DataBuffer_->GetSize() > MaxBufferSize) {
            FinishCurrentSegment();
        }
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        if (Offsets_.empty()) {
            return 0;
        } else {
            // DataBuffer may be empty (if there were no values), but we still must report nonzero result.
            return DataBuffer_->GetSize() + sizeof(ui32) * Offsets_.size();
        }
    }

    virtual void FinishCurrentSegment() override
    {
        if (Offsets_.size() > 0) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int SchemaColumnCount_;

    std::unique_ptr<TChunkedOutputStream> DataBuffer_;

    std::vector<ui32> Offsets_;

    std::vector<ui32> ValueCounts_;
    ui32 MaxValueCount_;

    void Reset()
    {
        Offsets_.clear();
        ValueCounts_.clear();

        DataBuffer_ = std::make_unique<TChunkedOutputStream>();
        MaxValueCount_ = 0;
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Offsets_.size());

        ui32 expectedBytesPerRow;
        ui32 maxOffsetDelta;
        PrepareDiffFromExpected(&Offsets_, &expectedBytesPerRow, &maxOffsetDelta);

        segmentInfo.Data.push_back(CompressUnsignedVector(MakeRange(Offsets_), maxOffsetDelta));
        segmentInfo.Data.push_back(CompressUnsignedVector(MakeRange(ValueCounts_), MaxValueCount_));

        auto data = DataBuffer_->Flush();
        segmentInfo.Data.insert(segmentInfo.Data.end(), data.begin(), data.end());

        auto* schemalessSegmentMeta = segmentInfo.SegmentMeta.MutableExtension(TSchemalessSegmentMeta::schemaless_segment_meta);
        schemalessSegmentMeta->set_expected_bytes_per_row(expectedBytesPerRow);

        TColumnWriterBase::DumpSegment(&segmentInfo);
    }

    void AddPendingValues(TRange<TUnversionedRow> rows)
    {
        size_t cumulativeSize = 0;

        for (auto row : rows) {
            for (int index = SchemaColumnCount_; index < row.GetCount(); ++index) {
                cumulativeSize += GetByteSize(row[index]);
            }
        }

        ui32 base = DataBuffer_->GetSize();
        char* begin = DataBuffer_->Preallocate(cumulativeSize);
        char* current = begin;

        for (auto row : rows) {
            ++RowCount_;
            Offsets_.push_back(base + current - begin);

            i32 valueCount = row.GetCount() - SchemaColumnCount_;
            if (valueCount <= 0) {
                ValueCounts_.push_back(0);
            } else {
                MaxValueCount_ = std::max(MaxValueCount_, static_cast<ui32>(valueCount));
                ValueCounts_.push_back(valueCount);
                for (int index = SchemaColumnCount_; index < row.GetCount(); ++index) {
                    current += WriteValue(current, row[index]);
                }
            }
        }

        DataBuffer_->Advance(current - begin);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateSchemalessColumnWriter(
    int schemaColumnCount,
    TDataBlockWriter* blockWriter)
{
    return std::make_unique<TSchemalessColumnWriter>(schemaColumnCount, blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
