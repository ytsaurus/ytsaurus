#include "../misc/stdafx.h"
#include "channel_reader.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TChannelReader::TChannelReader(const TChannel& channel)
    : Channel(channel)
    , ColumnBuffers(channel.GetColumns().size() + 1)
    , CurrentColumnIndex(-1)
{ }

void TChannelReader::SetBlock(const TSharedRef& block)
{
    YASSERT(CurrentColumnIndex = -1);

    CurrentBlock = block;

    TMemoryInput input(CurrentBlock.Begin(), CurrentBlock.Size());
    yvector<i32> columnSizes(Channel.GetColumns().size());
    for (int i = 0; i < columnSizes.ysize(); ++i) {
        Load(&input, columnSizes[i]);
    }

    const char* currentPos = input.Buf();
    for (int i = 0; i < columnSizes.ysize(); ++i) {
        i32& size = columnSizes[i];
        ColumnBuffers[i].Reset(currentPos, size);
        currentPos += size;
    }
    ColumnBuffers.back().Reset(currentPos, CurrentBlock.End() - currentPos);
}

bool TChannelReader::NextRow()
{
    if (CurrentBlock.Begin() == NULL) {
        return false;
    }

    while (NextColumn())
    { }

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
        YASSERT(CurrentColumnIndex <= ColumnBuffers.size());

        if (CurrentColumnIndex == ColumnBuffers.size()) {
            return false;
        } else if (CurrentColumnIndex == ColumnBuffers.size() - 1) {
            YASSERT(ColumnBuffers.back().Avail() > 0);
            // Processing range column
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

        if (CurrentColumnIndex < ColumnBuffers.size() - 1) {
            // Processing fixed column
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
    YASSERT(CurrentColumnIndex < ColumnBuffers.size());

    if (CurrentColumnIndex < ColumnBuffers.size() - 1) {
        return Channel.GetColumns()[CurrentColumnIndex];
    } else {
        return CurrentColumn.ToString();
    }
}

TValue TChannelReader::GetValue() const
{
    YASSERT(CurrentColumnIndex >= 0);
    YASSERT(CurrentColumnIndex <= ColumnBuffers.size());

    return CurrentValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
