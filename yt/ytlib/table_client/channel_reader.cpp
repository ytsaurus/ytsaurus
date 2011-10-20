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

void TChannelReader::SetBlock(const TSharedRef& block)
{
    YASSERT(CurrentColumnIndex == -1);

    CurrentBlock = block;

    TMemoryInput input(CurrentBlock.Begin(), CurrentBlock.Size());
    yvector<ui64> columnSizes;
    columnSizes.reserve(Channel.GetColumns().size());
    for (int i = 0; i < Channel.GetColumns().size(); ++i) {
        ui64 size;
        ReadVarInt(&size, &input);
        YASSERT(size > 0);
        columnSizes.push_back(size);
    }

    const char* currentPos = input.Buf();
    for (int i = 0; i < columnSizes.ysize(); ++i) {
        ui64& size = columnSizes[i];
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
        YASSERT(CurrentColumnIndex <= 
            static_cast<int>(ColumnBuffers.size()));

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
