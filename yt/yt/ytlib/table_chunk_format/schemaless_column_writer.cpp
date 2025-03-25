#include "schemaless_column_writer.h"

#include "column_writer_detail.h"
#include "helpers.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const int MaxRowCount = 128 * 1024;
static const int MaxBufferSize = 32 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessColumnWriter
    : public TColumnWriterBase
{
public:
    TSchemalessColumnWriter(
        int schemaColumnCount,
        TDataBlockWriter* blockWriter,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : TColumnWriterBase(blockWriter, memoryUsageTracker)
        , SchemaColumnCount_(schemaColumnCount)
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        Reset();
    }

    void WriteVersionedValues(TRange<TVersionedRow> /*rows*/) override
    {
        YT_ABORT();
    }

    void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        AddValues(rows);
        MemoryGuard_.SetSize(GetUntrackedMemoryUsage());
        if (Offsets_.size() > MaxRowCount || DataBuffer_->GetSize() > MaxBufferSize) {
            FinishCurrentSegment();
        }
    }

    i64 GetUntrackedMemoryUsage() const
    {
        return GetVectorMemoryUsage(Offsets_) + GetVectorMemoryUsage(ValueCounts_);
    }

    i32 GetCurrentSegmentSize() const override
    {
        if (Offsets_.empty()) {
            return 0;
        } else {
            // DataBuffer may be empty (if there were no values), but we still must report nonzero result.
            return DataBuffer_->GetSize() + sizeof(ui32) * Offsets_.size();
        }
    }

    void FinishCurrentSegment() override
    {
        if (Offsets_.size() > 0) {
            DumpSegment();
            Reset();
        }
    }

private:
    const int SchemaColumnCount_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    std::unique_ptr<TChunkedOutputStream> DataBuffer_;

    std::vector<ui32> Offsets_;

    std::vector<ui32> ValueCounts_;
    ui32 MaxValueCount_;

    void Reset()
    {
        Offsets_.clear();
        ValueCounts_.clear();

        DataBuffer_ = std::make_unique<TChunkedOutputStream>(
            GetRefCountedTypeCookie<TDefaultChunkedOutputStreamTag>(),
            MemoryUsageTracker_
        );
        MaxValueCount_ = 0;
        MemoryGuard_.SetSize(GetUntrackedMemoryUsage());
    }

    void DumpSegment()
    {
        TSegmentInfo segmentInfo;
        segmentInfo.SegmentMeta.set_type(0);
        segmentInfo.SegmentMeta.set_version(0);
        segmentInfo.SegmentMeta.set_row_count(Offsets_.size());

        auto [expectedBytesPerRow, maxOffsetDelta] = PrepareDiffFromExpected(&Offsets_);

        segmentInfo.Data.push_back(BitPackUnsignedVector(TRange(Offsets_), maxOffsetDelta));
        segmentInfo.Data.push_back(BitPackUnsignedVector(TRange(ValueCounts_), MaxValueCount_));

        auto data = DataBuffer_->Finish();
        segmentInfo.Data.insert(segmentInfo.Data.end(), data.begin(), data.end());

        auto* schemalessSegmentMeta = segmentInfo.SegmentMeta.MutableExtension(NProto::TSchemalessSegmentMeta::schemaless_segment_meta);
        schemalessSegmentMeta->set_expected_bytes_per_row(expectedBytesPerRow);

        TColumnWriterBase::DumpSegment(&segmentInfo, {});
    }

    void AddValues(TRange<TUnversionedRow> rows)
    {
        size_t totalSize = 0;
        for (auto row : rows) {
            for (int index = SchemaColumnCount_; index < static_cast<int>(row.GetCount()); ++index) {
                totalSize += EstimateRowValueSize(row[index]);
            }
        }

        ui32 base = DataBuffer_->GetSize();
        char* begin = DataBuffer_->Preallocate(totalSize);
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
                for (int index = SchemaColumnCount_; index < static_cast<int>(row.GetCount()); ++index) {
                    current += WriteRowValue(current, row[index]);
                }
            }
        }

        DataBuffer_->Advance(current - begin);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateSchemalessColumnWriter(
    int schemaColumnCount,
    TDataBlockWriter* blockWriter,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    return std::make_unique<TSchemalessColumnWriter>(
        schemaColumnCount,
        blockWriter,
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
