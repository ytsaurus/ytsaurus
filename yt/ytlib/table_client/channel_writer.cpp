#include "../misc/stdafx.h"
#include "channel_writer.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

TBlobOutput::TBlobOutput(size_t size = 0)
{
    Blob.reserve(size);
}

TBlobOutput::~TBlobOutput() throw()
{ }

void TBlobOutput::DoWrite(const void* buf, size_t len)
{
    Blob.insert(
        Blob.end(), 
        static_cast<const char*>(buf), 
        static_cast<const char*>(buf) + len);
}

const char* TBlobOutput::Begin() const
{
    return Blob.begin();
}

i32 TBlobOutput::GetSize() const
{
    return static_cast<i32>(Blob.size());
}

void TBlobOutput::Clear()
{
    Blob.clear();
}

TSharedRef TBlobOutput::Flush()
{
    return TSharedRef(Blob);
}

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
        CurrentSize += TValue(column).Save(&RangeColumns);
        CurrentSize += value.Save(&RangeColumns);
    } else {
        int columnIndex = it->Second();
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
        WriteVarInt(column.GetSize(), &blockStream);
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
