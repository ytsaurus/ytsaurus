#include "stdafx.h"
#include "channel_writer.h"
#include "value.h"

#include <core/misc/blob_output.h>
#include <core/misc/varint.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

const int TChannelWriter::MaxUpperReserveLimit = 64 * 1024;
const int TChannelWriter::MinUpperReserveLimit = 4 * 1024;
static const int RangeSizesChunk = 1024;

TChannelWriter::TChannelWriter(
    int bufferIndex,
    int fixedColumnCount,
    bool writeRangeSizes,
    int upperReserveLimit)
    : BufferIndex_(bufferIndex)
    , HeapIndex_(bufferIndex)
    , FixedColumns(fixedColumnCount, TChunkedOutputStream(upperReserveLimit))
    , RangeColumns(upperReserveLimit)
    // This buffer incurs additional overhead for
    // partition chunks, but it is very small: 1K per partition.
    , RangeSizes(writeRangeSizes ? RangeSizesChunk : 1)
    , RangeOffset(0)
    , WriteRangeSizes(writeRangeSizes)
    , IsColumnUsed(fixedColumnCount)
    , CurrentSize(0)
    , Capacity(0)
    , CurrentRowCount(0)
{
    InitCapacity();
}

void TChannelWriter::InitCapacity()
{
    Capacity += RangeSizes.GetCapacity();
    Capacity += RangeColumns.GetCapacity();
    for (const auto& column : FixedColumns) {
        Capacity += column.GetCapacity();
    }
}

void TChannelWriter::WriteFixed(int columnIndex, const TStringBuf& value)
{
    auto& columnOutput = FixedColumns[columnIndex];
    auto capacity = columnOutput.GetCapacity();
    CurrentSize += TValue(value).Save(&columnOutput);
    Capacity += columnOutput.GetCapacity() - capacity;
    IsColumnUsed[columnIndex] = true;
}

void TChannelWriter::WriteRange(const TStringBuf& name, const TStringBuf& value)
{
    auto capacity = RangeColumns.GetCapacity();
    CurrentSize += TValue(value).Save(&RangeColumns);
    CurrentSize += WriteVarInt32(&RangeColumns, static_cast<i32>(name.length()));
    CurrentSize += name.length();
    RangeColumns.Write(name);
    Capacity += RangeColumns.GetCapacity() - capacity;
}

void TChannelWriter::WriteRange(int chunkColumnIndex, const TStringBuf& value)
{
    YASSERT(chunkColumnIndex > 0);
    auto capacity = RangeColumns.GetCapacity();
    CurrentSize += TValue(value).Save(&RangeColumns);
    CurrentSize += WriteVarInt32(&RangeColumns, -(chunkColumnIndex + 1));
    Capacity += RangeColumns.GetCapacity() - capacity;
}

void TChannelWriter::EndRow()
{
    for (int columnIdx = 0; columnIdx < IsColumnUsed.size(); ++columnIdx) {
        if (IsColumnUsed[columnIdx]) {
            // Clean flags
            IsColumnUsed[columnIdx] = false;
        } else {
            auto& columnData = FixedColumns[columnIdx];
            auto capacity = columnData.GetCapacity();
            CurrentSize += TValue().Save(&columnData);
            Capacity += columnData.GetCapacity() - capacity;
        }
    }

    {
        // End of the row
        auto capacity = RangeColumns.GetCapacity();
        CurrentSize += TValue().Save(&RangeColumns);
        Capacity += RangeColumns.GetCapacity() - capacity;
    }

    if (WriteRangeSizes) {
        auto capacity = RangeSizes.GetCapacity();
        CurrentSize += WriteVarUint64(&RangeSizes, RangeColumns.GetSize() - RangeOffset);
        Capacity += RangeSizes.GetCapacity() - capacity;
        RangeOffset = RangeColumns.GetSize();
    }

    ++CurrentRowCount;
}

i64 TChannelWriter::GetDataSize() const
{
    return CurrentSize;
}

i64 TChannelWriter::GetCapacity() const
{
    return Capacity;
}

std::vector<TSharedRef> TChannelWriter::FlushBlock()
{
    TBlobOutput sizeOutput(8 * (FixedColumns.size() + 1));

    for (const auto& column : FixedColumns) {
        WriteVarUint64(&sizeOutput, column.GetSize());
    }
    WriteVarUint64(&sizeOutput, RangeColumns.GetSize());

    std::vector<TSharedRef> result;
    result.reserve(FixedColumns.size() + 3);
    result.push_back(sizeOutput.Flush());

    for (auto& column : FixedColumns) {
        auto blocks = column.Flush();
        result.insert(result.end(), blocks.begin(), blocks.end());
    }

    {
        auto blocks = RangeColumns.Flush();
        result.insert(result.end(), blocks.begin(), blocks.end());
    }

    if (WriteRangeSizes) {
        auto blocks = RangeSizes.Flush();
        result.insert(result.end(), blocks.begin(), blocks.end());
        RangeOffset = 0;
    }

    CurrentSize = 0;
    CurrentRowCount = 0;
    Capacity = 0;
    InitCapacity();

    return result;
}

i64 TChannelWriter::GetCurrentRowCount() const
{
    return CurrentRowCount;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
