#include "channel_writer.h"

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

TChannelWriter::TChannelWriter(const TChannel& channel)
    : Channel(channel)
    , FixedColumns(Channel.GetColumns().size())
    , IsColumnUsed(Channel.GetColumns().size())
    , CurrentRowCount(0)
{
    for (int index = 0; index < Channel.GetColumns().size(); ++index) {
        auto& column = Channel.GetColumns()[index];
        ColumnIndexes[column] = index;
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
        AppendValue(TValue(column), &RangeColumns);
        AppendValue(value, &RangeColumns);
    } else {
        int columnIndex = it->Second();
        TBlob& columnData = FixedColumns[columnIndex];
        AppendValue(value, &columnData);
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
            TBlob& columnData = FixedColumns[columnIdx];
            AppendValue(TValue(), &columnData);
        }
    }

    // End of the row
    AppendValue(TValue(), &RangeColumns);
    ++ CurrentRowCount;
}

void TChannelWriter::AppendSize(i32 value, TBlob* data) {
    data->insert(data->end(), 
        reinterpret_cast<ui8*>(&value), 
        reinterpret_cast<ui8*>(&value + 1));
    CurrentSize += sizeof(value);
}

void TChannelWriter::AppendValue(TValue value, TBlob* data)
{
    AppendSize(value.GetSize(), data);
    data->insert(data->end(), value.Begin(), value.End());
    CurrentSize += value.GetSize();
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
    TBlob block(CurrentSize);
    auto curPos = block.begin();

    FOREACH(const auto& column, FixedColumns) {
        i32 size = column.size();
        *reinterpret_cast<i32*>(curPos) = size;
        curPos += sizeof(size);
    }

    FOREACH(auto& column, FixedColumns) {
        memcpy(curPos, column.begin(), column.size());
        curPos += column.size();
        column.clear();
    }

    memcpy(curPos, RangeColumns.begin(), RangeColumns.size());
    RangeColumns.clear();

    CurrentSize = GetEmptySize();
    CurrentRowCount = 0;

    return TSharedRef(block);
}

int TChannelWriter::GetCurrentRowCount() const
{
    return CurrentRowCount;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
