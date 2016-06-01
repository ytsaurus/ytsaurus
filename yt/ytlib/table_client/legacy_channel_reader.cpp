#include "legacy_channel_reader.h"

#include <yt/core/misc/varint.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TLegacyChannelReader::TLegacyChannelReader(const NChunkClient::TChannel& channel)
    : Channel(channel)
    , ColumnBuffers(channel.GetColumns().size() + 1)
{ }

void TLegacyChannelReader::SetBlock(const TSharedRef& block)
{
    Y_ASSERT(CurrentColumnIndex == -1);

    CurrentBlock = block;
    BlockFinished = false;

    TMemoryInput input(CurrentBlock.Begin(), CurrentBlock.Size());
    std::vector<size_t> columnSizes;

    int bufferCount = Channel.GetColumns().size() + 1;
    // One buffer for RangeColumn.
    columnSizes.reserve(bufferCount);
    for (int columnIndex = 0; columnIndex < bufferCount; ++columnIndex) {
        ui64 size;
        ReadVarUint64(&input, &size);
        Y_ASSERT(size <= static_cast<ui64>(std::numeric_limits<size_t>::max()));
        columnSizes.push_back(static_cast<size_t>(size));
    }

    const char* currentPos = input.Buf();
    for (int columnIndex = 0; columnIndex < bufferCount; ++columnIndex) {
        size_t size = columnSizes[columnIndex];
        ColumnBuffers[columnIndex].Reset(currentPos, size);
        currentPos += size;
    }
}

bool TLegacyChannelReader::NextRow()
{
    if (BlockFinished) {
        return false;
    }

    while (NextColumn())
    { }

    CurrentColumn = TStringBuf();
    CurrentValue = TStringBuf();
    CurrentColumnIndex = -1;

    if (ColumnBuffers.front().Avail() == 0) {
        BlockFinished = true;
        return false;
    }

    return true;
}

TStringBuf TLegacyChannelReader::LoadValue(TMemoryInput* input)
{
    Y_ASSERT(input);

    ui64 size;
    ReadVarUint64(input, &size);
    if (size == 0) {
        return TStringBuf();
    }

    --size;
    TStringBuf tmp(const_cast<char*>(input->Buf()), static_cast<size_t>(size));
    input->Skip(static_cast<size_t>(size));
    return tmp;
}

bool TLegacyChannelReader::NextColumn()
{
    int columnBuffersSize = static_cast<int>(ColumnBuffers.size());
    while (true) {
        Y_ASSERT(CurrentColumnIndex <= columnBuffersSize);

        if (CurrentColumnIndex == columnBuffersSize) {
            return false;
        } else if (CurrentColumnIndex == columnBuffersSize - 1) {
            Y_ASSERT(ColumnBuffers.back().Avail() > 0);
            // Processing range column.
            auto& rangeBuffer = ColumnBuffers[CurrentColumnIndex];
            auto value = LoadValue(&rangeBuffer);
            if (!value.IsInited()) {
                ++CurrentColumnIndex;
                return false;
            }
            CurrentValue = value;
            i32 nameSize;
            ReadVarInt32(&rangeBuffer, &nameSize);

            if (nameSize < 0) {
                // global key column index, not implemented yet.
                YUNREACHABLE();
            } else {
                CurrentColumn = TStringBuf(rangeBuffer.Buf(), nameSize);
                rangeBuffer.Skip(nameSize);
            }

            return true;
        }

        Y_ASSERT(ColumnBuffers.back().Avail() > 0);
        ++CurrentColumnIndex;

        if (CurrentColumnIndex < columnBuffersSize - 1) {
            // Processing fixed column.
            auto& rangeBuffer = ColumnBuffers[CurrentColumnIndex];
            auto value = LoadValue(&rangeBuffer);
            if (value.IsInited()) {
                CurrentValue = value;
                return true;
            }
        }
    }
}

TStringBuf TLegacyChannelReader::GetColumn() const
{
    Y_ASSERT(CurrentColumnIndex >= 0);

    int ColumnBuffersSize = static_cast<int>(ColumnBuffers.size());
    Y_ASSERT(CurrentColumnIndex < ColumnBuffersSize);

    if (CurrentColumnIndex < ColumnBuffersSize - 1) {
        return Channel.GetColumns()[CurrentColumnIndex];
    } else {
        return CurrentColumn;
    }
}

const TStringBuf& TLegacyChannelReader::GetValue() const
{
    Y_ASSERT(CurrentColumnIndex >= 0);
    Y_ASSERT(CurrentColumnIndex <= static_cast<int>(ColumnBuffers.size()));

    return CurrentValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
