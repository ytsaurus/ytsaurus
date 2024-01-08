#include "timestamp_writer.h"
#include "data_block_writer.h"

#include <yt/yt/ytlib/new_table_client/prepared_meta.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/coding/zig_zag.h>

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

    void WriteTimestamps(TRange<TVersionedRow> rows) override
    {
        for (const auto row : rows) {
            WriteTimestampCounts_.push_back((WriteTimestampCounts_.empty()
                ? 0
                : WriteTimestampCounts_.back()) + row.GetWriteTimestampCount());

            for (auto timestamp : row.WriteTimestamps()) {
                WriteTimestampIds_.push_back(RegisterTimestamp(timestamp));
            }

            DeleteTimestampCounts_.push_back((DeleteTimestampCounts_.empty()
                ? 0
                : DeleteTimestampCounts_.back()) + row.GetDeleteTimestampCount());

            for (auto timestamp : row.DeleteTimestamps()) {
                DeleteTimestampIds_.push_back(RegisterTimestamp(timestamp));
            }
        }

        RowCount_ += rows.Size();
        TryDumpSegment();
    }

    TSharedRef FinishBlock(int blockIndex) override
    {
        FinishCurrentSegment();

        for (auto& meta : CurrentBlockSegments_) {
            // TODO(max42): this block index should be mapped in deferred callback
            // when versioned writers are able to reorder blocks.
            meta.set_block_index(blockIndex);
            ColumnMeta_.add_segments()->Swap(&meta);
        }

        CurrentBlockSegments_.clear();

        size_t metaSize = sizeof(NNewTableClient::TTimestampMeta) * CurrentBlockSegmentMetas_.size();

        auto mergedMeta = TSharedMutableRef::Allocate(metaSize);
        char* metasData = mergedMeta.Begin();

        for (const auto& meta : CurrentBlockSegmentMetas_) {
            auto ref = MetaToRef(meta);
            std::copy(ref.Begin(), ref.End(), metasData);
            metasData += ref.size();
        }

        CurrentBlockSegmentMetas_.clear();

        return mergedMeta;
    }

    void FinishCurrentSegment() override
    {
        if (!WriteTimestampCounts_.empty()) {
            DumpSegment();
            MinTimestamp_ = std::min(MinTimestamp_, MinSegmentTimestamp_);
            MaxTimestamp_ = std::max(MaxTimestamp_, MaxSegmentTimestamp_);
            Reset();
        }
    }

    // Size currently occupied.
    i32 GetCurrentSegmentSize() const override
    {
        if (WriteTimestampCounts_.empty()) {
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

    const NProto::TColumnMeta& ColumnMeta() const override
    {
        return ColumnMeta_;
    }

    TTimestamp GetMinTimestamp() const override
    {
        return MinTimestamp_;
    }

    TTimestamp GetMaxTimestamp() const override
    {
        return MaxTimestamp_;
    }

    i64 GetMetaSize() const override
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

    // Segment metas are stored in block in new columnar format.
    std::vector<NNewTableClient::TTimestampMeta> CurrentBlockSegmentMetas_;

    ui32 RegisterTimestamp(TTimestamp timestamp)
    {
        MinSegmentTimestamp_ = std::min(MinSegmentTimestamp_, timestamp);
        MaxSegmentTimestamp_ = std::max(MaxSegmentTimestamp_, timestamp);
        auto result = UniqueTimestamps_.emplace(timestamp, Dictionary_.size());
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

        NNewTableClient::TTimestampMeta rawMeta;
        memset(&rawMeta, 0, sizeof(rawMeta));

        rawMeta.BaseTimestamp = MinSegmentTimestamp_;

        ui32 expectedWritesPerRow ;
        ui32 maxWriteIndex;
        PrepareDiffFromExpected(&WriteTimestampCounts_, &expectedWritesPerRow, &maxWriteIndex);

        rawMeta.ExpectedWritesPerRow = expectedWritesPerRow;

        ui32 expectedDeletesPerRow;
        ui32 maxDeleteIndex;
        PrepareDiffFromExpected(&DeleteTimestampCounts_, &expectedDeletesPerRow, &maxDeleteIndex);

        rawMeta.ExpectedDeletesPerRow = expectedDeletesPerRow;

        i64 size = 0;
        std::vector<TSharedRef> data;

        data.push_back(BitpackVector(
            MakeRange(Dictionary_),
            MaxSegmentTimestamp_ - MinSegmentTimestamp_,
            &rawMeta.TimestampsDictSize,
            &rawMeta.TimestampsDictWidth));
        size += data.back().Size();

        data.push_back(BitpackVector(MakeRange(WriteTimestampIds_), Dictionary_.size(), &rawMeta.WriteTimestampSize, &rawMeta.WriteTimestampWidth));
        size += data.back().Size();

        data.push_back(BitpackVector(MakeRange(DeleteTimestampIds_), Dictionary_.size(), &rawMeta.DeleteTimestampSize, &rawMeta.DeleteTimestampWidth));
        size += data.back().Size();

        data.push_back(BitpackVector(MakeRange(WriteTimestampCounts_), maxWriteIndex, &rawMeta.WriteOffsetDiffsSize, &rawMeta.WriteOffsetDiffsWidth));
        size += data.back().Size();

        data.push_back(BitpackVector(MakeRange(DeleteTimestampCounts_), maxDeleteIndex, &rawMeta.DeleteOffsetDiffsSize, &rawMeta.DeleteOffsetDiffsWidth));
        size += data.back().Size();

        TSegmentMeta segmentMeta;
        segmentMeta.set_type(0);
        segmentMeta.set_version(0);
        segmentMeta.set_row_count(WriteTimestampCounts_.size());
        segmentMeta.set_offset(BlockWriter_->GetOffset());
        segmentMeta.set_chunk_row_count(RowCount_);
        segmentMeta.set_size(size);

        rawMeta.DataOffset = BlockWriter_->GetOffset();
        rawMeta.RowCount = WriteTimestampCounts_.size();
        rawMeta.ChunkRowCount = RowCount_;

        auto* meta = segmentMeta.MutableExtension(TTimestampSegmentMeta::timestamp_segment_meta);
        meta->set_min_timestamp(MinSegmentTimestamp_);
        meta->set_expected_writes_per_row(expectedWritesPerRow);
        meta->set_expected_deletes_per_row(expectedDeletesPerRow);

        // This is a rough approximation, but we don't want to pay for additional ByteSize call.
        MetaSize_ += sizeof(TSegmentMeta);

        CurrentBlockSegments_.push_back(segmentMeta);

        BlockWriter_->WriteSegment(MakeRange(data));

        if (BlockWriter_->GetEnableSegmentMetaInBlocks()) {
            VerifyRawSegmentMeta(segmentMeta, data, rawMeta);
        }

        CurrentBlockSegmentMetas_.push_back(std::move(rawMeta));
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ITimestampWriter> CreateTimestampWriter(TDataBlockWriter* blockWriter)
{
    return std::make_unique<TTimestampWriter>(blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
