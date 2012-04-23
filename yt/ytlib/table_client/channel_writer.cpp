#include "stdafx.h"
#include "channel_writer.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

TChannelWriter::TChannelWriter(
    const TChannel& channel,
    const yhash_map<TColumn, int>& chunkColumnIndexes)
    : Channel(channel)
    , FixedColumns(Channel.GetColumns().size())
    , IsColumnUsed(Channel.GetColumns().size())
    , CurrentRowCount(0)
{
    ColumnIndexMapping.resize(chunkColumnIndexes.size(), UnknownIndex);

    for (int i = 0; i < Channel.GetColumns().size(); ++i) {
        auto& column = Channel.GetColumns()[i];
        auto it = chunkColumnIndexes.find(column);
        YASSERT(chunkColumnIndexes.end() != it);

        ColumnIndexMapping[it->second] = i;
    }

    FOREACH (auto& item, chunkColumnIndexes) {
        if ((ColumnIndexMapping[item.Second()] < 0) &&
            Channel.ContainsInRanges(item.First()))
        {
            ColumnIndexMapping[item.Second()] = RangeIndex;
        }
    }

    CurrentSize = GetEmptySize();
}

void TChannelWriter::Write(
    int chunkColumnIndex, 
    const TColumn& column, 
    TValue value)
{
    if (chunkColumnIndex > UnknownIndex) {
        int columnIndex = ColumnIndexMapping[chunkColumnIndex];
        if (columnIndex == UnknownIndex)
            return;

        if (columnIndex == RangeIndex) {
            CurrentSize += TValue(column).Save(&RangeColumns);
            CurrentSize += value.Save(&RangeColumns);
        } else {
            YASSERT(columnIndex > UnknownIndex);
            auto& columnOutput = FixedColumns[columnIndex];
            CurrentSize += value.Save(&columnOutput);
            IsColumnUsed[columnIndex] = true;
        }
    } else if (Channel.ContainsInRanges(column)) {
        CurrentSize += TValue(column).Save(&RangeColumns);
        CurrentSize += value.Save(&RangeColumns);
    }
}

void TChannelWriter::EndRow()
{
    for(int columnIdx = 0; columnIdx < IsColumnUsed.ysize(); ++columnIdx) {
        if (IsColumnUsed[columnIdx]) {
            // Clean flags
            IsColumnUsed[columnIdx] = false;
        } else {
            auto& columnData = FixedColumns[columnIdx];
            CurrentSize += TValue().Save(&columnData);
        }
    }

    // End of the row
    CurrentSize += TValue().Save(&RangeColumns);
    ++ CurrentRowCount;
}

size_t TChannelWriter::GetCurrentSize() const
{
    return CurrentSize;
}

size_t TChannelWriter::GetEmptySize() const
{
    return FixedColumns.size() * sizeof(i32);
}

bool TChannelWriter::HasUnflushedData() const
{
    return CurrentSize > GetEmptySize();
}

TSharedRef TChannelWriter::FlushBlock()
{
    TBlobOutput blockStream(CurrentSize);

    FOREACH (const auto& column, FixedColumns) {
        WriteVarUInt64(&blockStream, column.GetSize());
    }

    FOREACH (auto& column, FixedColumns) {
        blockStream.Write(column.Begin(), column.GetSize());
        column.Clear();
    }

    blockStream.Write(RangeColumns.Begin(), RangeColumns.GetSize());
    RangeColumns.Clear();

    CurrentSize = GetEmptySize();
    CurrentRowCount = 0;

    return blockStream.Flush();
}

int TChannelWriter::GetCurrentRowCount() const
{
    return CurrentRowCount;
}

const int TChannelWriter::UnknownIndex = -1;
const int TChannelWriter::RangeIndex = -2;

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
