#include "stdafx.h"
#include "channel_writer.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

TChannelWriter::TChannelWriter(const TChannel& channel)
    : Channel(channel)
    , FixedColumns(Channel.GetColumns().size())
    , IsColumnUsed(Channel.GetColumns().size())
    , CurrentRowCount(0)
{
    for (int columnIndex = 0; columnIndex < Channel.GetColumns().ysize(); ++columnIndex) {
        auto& column = Channel.GetColumns()[columnIndex];
        ColumnIndexes[column] = columnIndex;
    }
    CurrentSize = GetEmptySize();
}

void TChannelWriter::Write(const TColumn& column, TValue value)
{
    if (!Channel.Contains(column)) {
        return;
    }

    auto it = ColumnIndexes.find(column);
    if (it == ColumnIndexes.end()) {
        CurrentSize += TValue(column).Save(&RangeColumns);
        CurrentSize += value.Save(&RangeColumns);
    } else {
        int columnIndex = it->second;
        auto& columnOutput = FixedColumns[columnIndex];
        CurrentSize += value.Save(&columnOutput);
        IsColumnUsed[columnIndex] = true;
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

    FOREACH(const auto& column, FixedColumns) {
        WriteVarUInt64(&blockStream, column.GetSize());
    }

    FOREACH(auto& column, FixedColumns) {
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

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
