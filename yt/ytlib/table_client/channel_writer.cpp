#include "channel_writer.h"

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

TChannelWriter::TChannelWriter(const TChannel& channel)
    : Channel(channel)
    , FixedColumns(Channel.Columns().size())
    , ColumnSetFlags(Channel.Columns().size())
{
    for (int index = 0; index < Channel.Columns().size(); ++index) {
        auto& column = Channel.Columns()[index];
        ColumnIndexes[column] = index;
    }
    CurrentSize = GetEmptySize();
}

void TChannelWriter::Write(TColumn column, TValue value)
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
        ColumnSetFlags[columnIndex] = true;
    }
}

void TChannelWriter::EndRow()
{
    for(int columnIdx = 0; columnIdx < ColumnSetFlags.ysize(); ++columnIdx) {
        if (ColumnSetFlags[columnIdx]) {
            // Clean flags
            ColumnSetFlags[columnIdx] = false;
        } else {
            TBlob& columnData = FixedColumns[columnIdx];
            AppendValue(TValue(), &columnData);
        }
    }

    // End of the row
    AppendValue(TValue(), &RangeColumns);
}

void TChannelWriter::AppendSize(ui32 value, TBlob* data) {
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
        ui32 size = column.size();
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

    return TSharedRef(block);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
