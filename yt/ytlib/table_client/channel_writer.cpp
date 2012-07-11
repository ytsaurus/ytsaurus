#include "stdafx.h"
#include "channel_writer.h"
#include "value.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

TChannelWriter::TChannelWriter(int fixedColumnCount)
    : FixedColumns(fixedColumnCount)
    , IsColumnUsed(fixedColumnCount)
    , CurrentRowCount(0)
{
    CurrentSize = GetEmptySize();
}

void TChannelWriter::WriteFixed(int columnIndex, const TStringBuf& value)
{
    auto& columnOutput = FixedColumns[columnIndex];
    CurrentSize += TValue(value).Save(&columnOutput);
    IsColumnUsed[columnIndex] = true;
}

void TChannelWriter::WriteRange(const TStringBuf& name, const TStringBuf& value)
{
    CurrentSize += TValue(name).Save(&RangeColumns);
    CurrentSize += TValue(value).Save(&RangeColumns);
}

void TChannelWriter::EndRow()
{
    for (int columnIdx = 0; columnIdx < IsColumnUsed.size(); ++columnIdx) {
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

std::vector<TSharedRef> TChannelWriter::FlushBlock()
{
    TBlobOutput sizeOutput(8 * FixedColumns.size());

    FOREACH (const auto& column, FixedColumns) {
        WriteVarUInt64(&sizeOutput, column.GetSize());
    }

    std::vector<TSharedRef> result;
    result.reserve(FixedColumns.size() + 2);
    result.push_back(sizeOutput.Flush());

    FOREACH (auto& column, FixedColumns) {
        auto capacity = column.GetBlob()->capacity();
        result.push_back(column.Flush());
        column.Reserve(capacity);
    }

    auto capacity = RangeColumns.GetBlob()->capacity();
    result.push_back(RangeColumns.Flush());
    RangeColumns.Reserve(capacity);

    CurrentSize = GetEmptySize();
    CurrentRowCount = 0;

    return result;
}

int TChannelWriter::GetCurrentRowCount() const
{
    return CurrentRowCount;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
