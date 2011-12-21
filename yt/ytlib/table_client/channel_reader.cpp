#include "stdafx.h"
#include "channel_reader.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TChannelReader::TChannelReader(const TChannel& channel)
    : Channel(channel)
    , ColumnBuffers(channel.GetColumns().size() + 1)
    , CurrentColumnIndex(-1)
{ }

void TChannelReader::SetBlock(TSharedRef&& block)
{
    YASSERT(CurrentColumnIndex == -1);

    CurrentBlock = block;

    TMemoryInput input(CurrentBlock.Begin(), CurrentBlock.Size());
    yvector<size_t> columnSizes;
    columnSizes.reserve(Channel.GetColumns().ysize());
    for (int columnIndex = 0; columnIndex < Channel.GetColumns().ysize(); ++columnIndex) {
        ui64 size;
        ReadVarUInt64(&input, &size);
        YASSERT(size <= static_cast<ui64>(Max<size_t>()));
        columnSizes.push_back(static_cast<size_t>(size));
    }

    const char* currentPos = input.Buf();
    for (int columnIndex = 0; columnIndex < columnSizes.ysize(); ++columnIndex) {
        size_t size = columnSizes[columnIndex];
        ColumnBuffers[columnIndex].Reset(currentPos, size);
        currentPos += size;
    }

    ColumnBuffers.back().Reset(currentPos, CurrentBlock.End() - currentPos);
}

bool TChannelReader::NextRow()
{
    if (!CurrentBlock.Begin()) {
        return false;
    }

    if (CurrentColumnIndex >= 0) {
        while (NextColumn())
        { }
    }

    CurrentColumn = TValue();
    CurrentValue = TValue();
    CurrentColumnIndex = -1;

    if (ColumnBuffers.front().Avail() == 0) {
        return false;
    }

    return true;
}

bool TChannelReader::NextColumn()
{
    while (true) {
        YASSERT(CurrentColumnIndex <= ColumnBuffers.ysize());

        if (CurrentColumnIndex == ColumnBuffers.ysize()) {
            return false;
        } else if (CurrentColumnIndex == ColumnBuffers.ysize() - 1) {
            YASSERT(ColumnBuffers.back().Avail() > 0);
            // Processing range column.
            auto& RangeBuffer = ColumnBuffers[CurrentColumnIndex];
            CurrentColumn = TValue::Load(&RangeBuffer);
            if (CurrentColumn.IsNull()) {
                ++CurrentColumnIndex;
                return false;
            }
            CurrentValue = TValue::Load(&RangeBuffer);
            return true;
        } 

        YASSERT(ColumnBuffers.back().Avail() > 0);
        ++CurrentColumnIndex;

        if (CurrentColumnIndex < ColumnBuffers.ysize() - 1) {
            // Processing fixed column.
            CurrentValue = TValue::Load(&ColumnBuffers[CurrentColumnIndex]);
            if (!CurrentValue.IsNull()) {
                return true;
            }
        }
    }
}

TColumn TChannelReader::GetColumn() const
{
    YASSERT(CurrentColumnIndex >= 0);
    YASSERT(CurrentColumnIndex < ColumnBuffers.ysize());

    if (CurrentColumnIndex < ColumnBuffers.ysize() - 1) {
        return Channel.GetColumns()[CurrentColumnIndex];
    } else {
        return CurrentColumn.ToString();
    }
}

TValue TChannelReader::GetValue() const
{
    YASSERT(CurrentColumnIndex >= 0);
    YASSERT(CurrentColumnIndex <= ColumnBuffers.ysize());

    return CurrentValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
