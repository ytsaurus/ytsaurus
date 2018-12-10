#include "timestamp_writer.h"
#include "data_block_writer.h"
#include "compressed_integer_vector.h"

#include <yt/client/table_client/versioned_row.h>

#include <yt/core/misc/zigzag.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

const int MaxTimestampCount = 128 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TTimestampWriter
    : public ITimestampWriter
{
public:
    explicit TTimestampWriter(TDataBlockWriter* blockWriter)
        : BlockWriter_(blockWriter)
    {
        Reset();
        BlockWriter_->RegisterColumnWriter(this);
    }

    virtual void WriteTimestamps(TRange<TVersionedRow> rows) override
    {
        for (const auto row : rows) {
            WriteTimestampCounts_.push_back((WriteTimestampCounts_.empty()
                ? 0
                : WriteTimestampCounts_.back()) + row.GetWriteTimestampCount());

            for (int i = 0; i < row.GetWriteTimestampCount(); ++i) {
                WriteTimestampIds_.push_back(RegisterTimestamp(row.BeginWriteTimestamps()[i]));
            }

            DeleteTimestampCounts_.push_back((DeleteTimestampCounts_.empty()
                ? 0
                : DeleteTimestampCounts_.back()) + row.GetDeleteTimestampCount());

            for (int i = 0; i < row.GetDeleteTimestampCount(); ++i) {
                DeleteTimestampIds_.push_back(RegisterTimestamp(row.BeginDeleteTimestamps()[i]));
            }
        }

        RowCount_ += rows.Size();
        TryDumpSegment();
    }

    virtual void FinishBlock(int blockIndex) override
    {
        FinishCurrentSegment();

        for (auto& meta : CurrentBlockSegments_) {
            meta.set_block_index(blockIndex);
            ColumnMeta_.add_segments()->Swap(&meta);
        }

        CurrentBlockSegments_.clear();
    }

    virtual void FinishCurrentSegment() override
    {
        if (!WriteTimestampCounts_.empty()) {
            DumpSegment();
            MinTimestamp_ = std::min(MinTimestamp_, MinSegmentTimestamp_);
            MaxTimestamp_ = std::max(MaxTimestamp_, MaxSegmentTimestamp_);
            Reset();
        }
    }

    // Size currently occupied.
    virtual i32 GetCurrentSegmentSize() const override
    {
        if (UniqueTimestamps_.empty()) {
            return 0;
        }

        auto dictionarySize = CompressedUnsignedVectorSizeInBytes(
            MaxSegmentTimestamp_ - MinSegmentTimestamp_,
            UniqueTimestamps_.size());

        auto idsSize = CompressedUnsignedVectorSizeInBytes(
            UniqueTimestamps_.size(),
            WriteTimestampIds_.size() + DeleteTimestampIds_.size());

        // Assume max diff from expected to be 63.
        auto indexesSize = CompressedUnsignedVectorSizeInBytes(
            63U,
            WriteTimestampCounts_.size() + DeleteTimestampCounts_.size());

        return dictionarySize + idsSize + indexesSize;
    }

    virtual const NProto::TColumnMeta& ColumnMeta() const override
    {
        return ColumnMeta_;
    }

    virtual TTimestamp GetMinTimestamp() const
    {
        return MinTimestamp_;
    }

    virtual TTimestamp GetMaxTimestamp() const
    {
        return MaxTimestamp_;
    }

    virtual i64 GetMetaSize() const
    {
        return MetaSize_;
    }

private:
    TTimestamp MinTimestamp_ = MaxTimestamp;
    TTimestamp MaxTimestamp_ = MinTimestamp;

    TTimestamp MinSegmentTimestamp_;
    TTimestamp MaxSegmentTimestamp_;
    THashMap<TTimestamp, ui32> UniqueTimestamps_;
    std::vector<TTimestamp> Dictionary_;

    std::vector<ui32> WriteTimestampIds_;
    std::vector<ui32> DeleteTimestampIds_;

    std::vector<ui32> WriteTimestampCounts_;
    std::vector<ui32> DeleteTimestampCounts_;

    TColumnMeta ColumnMeta_;
    std::vector<TSegmentMeta> CurrentBlockSegments_;

    TDataBlockWriter* BlockWriter_;

    i64 RowCount_ = 0;
    i64 MetaSize_ = 0;


    ui32 RegisterTimestamp(TTimestamp timestamp)
    {
        MinSegmentTimestamp_ = std::min(MinSegmentTimestamp_, timestamp);
        MaxSegmentTimestamp_ = std::max(MaxSegmentTimestamp_, timestamp);
        auto result = UniqueTimestamps_.insert(std::make_pair(timestamp, Dictionary_.size()));
        if (result.second) {
            Dictionary_.push_back(timestamp);
        }
        // Return id.
        return result.first->second;
    }

    void TryDumpSegment()
    {
        // Limit size of the buffer.
        if (UniqueTimestamps_.size() + WriteTimestampIds_.size() + DeleteTimestampIds_.size() < MaxTimestampCount) {
            return;
        }

        FinishCurrentSegment();
    }

    void Reset()
    {
        MinSegmentTimestamp_ = NTransactionClient::MaxTimestamp;
        MaxSegmentTimestamp_ = NTransactionClient::MinTimestamp;

        WriteTimestampIds_.clear();
        DeleteTimestampIds_.clear();

        WriteTimestampCounts_.clear();
        DeleteTimestampCounts_.clear();

        UniqueTimestamps_.clear();
        Dictionary_.clear();
    }

    void DumpSegment()
    {
        // TODO(psushin): rewrite with SSE.
        for (auto& timestamp : Dictionary_) {
            timestamp -= MinSegmentTimestamp_;
        }

        ui32 expectedWritesPerRow ;
        ui32 maxWriteIndex;
        PrepareDiffFromExpected(&WriteTimestampCounts_, &expectedWritesPerRow, &maxWriteIndex);

        ui32 expectedDeletesPerRow;
        ui32 maxDeleteIndex;
        PrepareDiffFromExpected(&DeleteTimestampCounts_, &expectedDeletesPerRow, &maxDeleteIndex);

        i64 size = 0;

        std::vector<TSharedRef> data;
        data.push_back(CompressUnsignedVector(MakeRange(Dictionary_), MaxSegmentTimestamp_ - MinSegmentTimestamp_));
        size += data.back().Size();

        data.push_back(CompressUnsignedVector(MakeRange(WriteTimestampIds_), Dictionary_.size()));
        size += data.back().Size();

        data.push_back(CompressUnsignedVector(MakeRange(DeleteTimestampIds_), Dictionary_.size()));
        size += data.back().Size();

        data.push_back(CompressUnsignedVector(MakeRange(WriteTimestampCounts_), maxWriteIndex));
        size += data.back().Size();

        data.push_back(CompressUnsignedVector(MakeRange(DeleteTimestampCounts_), maxDeleteIndex));
        size += data.back().Size();

        TSegmentMeta segmentMeta;
        segmentMeta.set_type(0);
        segmentMeta.set_version(0);
        segmentMeta.set_row_count(WriteTimestampCounts_.size());
        segmentMeta.set_offset(BlockWriter_->GetOffset());
        segmentMeta.set_chunk_row_count(RowCount_);
        segmentMeta.set_size(size);

        auto* meta = segmentMeta.MutableExtension(TTimestampSegmentMeta::timestamp_segment_meta);
        meta->set_min_timestamp(MinSegmentTimestamp_);
        meta->set_expected_writes_per_row(expectedWritesPerRow);
        meta->set_expected_deletes_per_row(expectedDeletesPerRow);

        // This is a rough approximation, but we don't want to pay for additional ByteSize call.
        MetaSize_ += sizeof(TSegmentMeta);

        CurrentBlockSegments_.push_back(segmentMeta);

        BlockWriter_->WriteSegment(MakeRange(data));
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ITimestampWriter> CreateTimestampWriter(TDataBlockWriter* blockWriter)
{
    return std::make_unique<TTimestampWriter>(blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
