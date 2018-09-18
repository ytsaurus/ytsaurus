#include "data_block_writer.h"

#include "column_writer.h"

namespace NYT {
namespace NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

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
    for (auto* columnWriter : ColumnWriters_) {
        columnWriter->FinishBlock(blockIndex);
    }

    i64 size = 0;
    for (const auto& part : Data_) {
        size += part.Size();
    }
    YCHECK(size > 0);

    TBlock block;
    block.Data.swap(Data_);
    block.Meta.set_row_count(currentRowCount - LastRowCount_);
    block.Meta.set_chunk_row_count(currentRowCount);
    block.Meta.set_uncompressed_size(size);

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

} // namespace NTableChunkFormat
} // namespace NYT
