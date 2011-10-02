#include "channel_writer.h"
#include "../misc/foreach.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TChannelWriter::TChannelWriter(const TChannel& channel)
    : Channel(channel)
    , FixedColumns(Channel.Columns().size())
     // size of block header - used to store sizes of fixed columns
{
    for (int index = 0; index < Channel.Columns().size(); ++index) {
        auto& column = Channel.Columns()[index];
        ColumnIndexes[column] = index;
    }
    CurrentSize = EmptySize();
}

void TChannelWriter::AddRow(const TTableRow& tableRow)
{
    yvector<bool> columnFlags(FixedColumns.size(), false);
    FOREACH(const auto& rowItem, tableRow) {
        auto it = ColumnIndexes.find(rowItem.first);
        if (it != ColumnIndexes.end()) {
            // Fixed column
            int columnIdx = it->second;
            TBlob& columnData = FixedColumns[columnIdx];
            AppendValue(rowItem.second, &columnData);

            YASSERT(!columnFlags[columnIdx]);
            columnFlags[columnIdx] = true;
        } else if (Channel.MatchRanges(rowItem.first)) {
            AppendValue(rowItem.first, &RangeColumns);
            AppendValue(rowItem.second, &RangeColumns);
        }
    }

    for(int columnIdx = 0; columnIdx  < columnFlags.ysize(); ++columnIdx) {
        if (!columnFlags[columnIdx]) {
            TBlob& columnData = FixedColumns[columnIdx];
            AppendValue(TValue::Null(), &columnData);
        }
    }

    // End of the row
    AppendValue(TValue::Null(), &RangeColumns);
}

void TChannelWriter::AppendSize(i32 value, TBlob* data)
{
    data->insert(data->end(), (char*)(&value), (char*)(&value + 1));
    CurrentSize += sizeof(value);
}

void TChannelWriter::AppendValue(const TValue& value, TBlob* data)
{
    AppendSize(value.GetSize(), data);
    data->insert(data->end(), value.Begin(), value.End());
    CurrentSize += value.GetSize();
}

size_t TChannelWriter::GetCurrentSize() const
{
    return CurrentSize;
}

size_t TChannelWriter::EmptySize() const
{
    return FixedColumns.size() * sizeof(i32);
}

bool TChannelWriter::HasData() const
{
    return CurrentSize > EmptySize();
}

TSharedRef TChannelWriter::FlushBlock()
{
    TBlob block(CurrentSize);
    auto curPos = block.begin();

    FOREACH(const auto& column, FixedColumns) {
        i32 size = column.size();
        memcpy(curPos, &size, sizeof(i32));
        curPos += sizeof(i32);
    }

    FOREACH(auto& column, FixedColumns) {
        memcpy(curPos, column.begin(), column.size());
        curPos += column.size();
        column.clear();
    }

    memcpy(curPos, RangeColumns.begin(), RangeColumns.size());
    RangeColumns.clear();

    CurrentSize = EmptySize();

    return TSharedRef(block);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
