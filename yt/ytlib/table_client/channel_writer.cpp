#include "channel_buffer.cpp"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

void TChannelBuffer::AddRow(const TTableRow& tableRow)
{
    yvector<bool> columnFlags(Channel.Columns.size(), false);
    FOREACH(const auto& rowItem, tableRow) {
        auto it = ColumnMap.find(rowItem.first)
        if (it != ColumnMap.end()) {
            // Fixed column
            int columnIdx = it->second;
            TBlob& columnData = FixedColumns[columnIdx];
            SerializeValue(rowItem->second, &columnData);
            YASSERT(!columnFlags[columnIdx]);
            columnFlags[columnIdx] = true;
        } else if (Channel.MatchRange(rowItem.first)) {
            SerializeValue(rowItem.first, &columnData);
            SerializeValue(rowItem.second, &columnData);
        }
    }

    for(int columnIdx = 0; columnIdx  < columnFlags.ysize(); ++columnIdx) {
        if (!columnFlags[columnIdx]) {
            TBlob& columnData = FixedColumns[columnIdx];
            SerializeValue(TValue::Null(), &columnData);
        
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
