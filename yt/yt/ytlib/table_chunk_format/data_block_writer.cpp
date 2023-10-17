#include "data_block_writer.h"

#include "column_writer.h"

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDataBlockWriter::TDataBlockWriter(bool enableSegmentMetaInBlocks)
    : EnableSegmentMetaInBlocks_(enableSegmentMetaInBlocks)
{ }

void TDataBlockWriter::WriteSegment(TRange<TSharedRef> segment)
{
    Data_.insert(Data_.end(), segment.Begin(), segment.End());
    for (const auto& part : segment) {
        CurrentOffset_ += part.Size();
    }
}

i64 TDataBlockWriter::GetOffset() const
{
    return CurrentOffset_;
}

void TDataBlockWriter::RegisterColumnWriter(IColumnWriterBase* columnWriter)
{
    ColumnWriters_.push_back(columnWriter);
}

TBlock TDataBlockWriter::DumpBlock(int blockIndex, i64 currentRowCount)
{
    std::vector<TSharedRef> blockSegmentMetas;
    for (auto* columnWriter : ColumnWriters_) {
        blockSegmentMetas.push_back(columnWriter->FinishBlock(blockIndex));
    }

    i64 dataSize = 0;
    for (const auto& part : Data_) {
        dataSize += part.Size();
    }
    YT_VERIFY(dataSize > 0);

    i64 segmentMetaSize = 0;

    if (EnableSegmentMetaInBlocks_) {
        auto columnCount = blockSegmentMetas.size();

        size_t segmentMetasSize = 0;
        for (const auto& metas : blockSegmentMetas) {
            YT_VERIFY(metas.size() % sizeof(ui64) == 0);
            segmentMetasSize += metas.size();
        }

        auto offset = sizeof(ui32) * (columnCount + 1);
        auto mergedMeta = TSharedMutableRef::Allocate(offset + segmentMetasSize);

        ui32* offsets = reinterpret_cast<ui32*>(mergedMeta.Begin());
        auto* metasData = reinterpret_cast<char*>(mergedMeta.Begin() + offset);

        for (const auto& metas : blockSegmentMetas) {
            *offsets++ = offset;
            std::copy(metas.begin(), metas.end(), metasData);
            offset += metas.size();
            metasData += metas.size();
        }
        *offsets++ = offset;

        segmentMetaSize += mergedMeta.size();

        Data_.push_back(std::move(mergedMeta));
    }

    TBlock block;
    block.Data.swap(Data_);
    block.Meta.set_row_count(currentRowCount - LastRowCount_);
    block.Meta.set_chunk_row_count(currentRowCount);
    block.Meta.set_uncompressed_size(dataSize + segmentMetaSize);
    block.SegmentMetaOffset = dataSize;
    block.GroupIndex = GroupIndex_;

    LastRowCount_ = currentRowCount;
    CurrentOffset_ = 0;

    return block;
}

i32 TDataBlockWriter::GetCurrentSize() const
{
    i64 result = CurrentOffset_;
    for (const auto* columnWriter : ColumnWriters_) {
        result += columnWriter->GetCurrentSegmentSize();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
