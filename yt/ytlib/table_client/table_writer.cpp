#include "table_writer.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    const TConfig& config, 
    IChunkWriter::TPtr chunkWriter,
    TSchema::TPtr schema,
    ICompressor::TPtr compressor)
    : Config(config)
    , CurrentBlockIndex(0)
    , CurrentRowIndex(0)
    , ChunkWriter(chunkWriter)
    , Schema(schema)
    , Compressor(compressor)
{
    // Fill protobuf chunk meta
    FOREACH(auto channel, Schema->Channels()) {
        auto* protoChannel = ChunkMeta.AddChannels();
        FOREACH(auto& column, channel.Columns()) {
            protoChannel->AddColumns(column.GetData(), column.GetSize());
        }

        FOREACH(auto& range, channel.Ranges()) {
            auto* protoRange = protoChannel->AddRanges();
            protoRange->SetBegin(range.Begin().GetData(), range.Begin().GetSize());
            protoRange->SetEnd(range.End().GetData(), range.End().GetSize());
        }

        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }
}

TTableWriter& TTableWriter::Write(const TValue& column, const TValue& value)
{
    // Old value for this column will be overwritten, if present
    CurrentRow[column] = value;
    return *this;
}

void TTableWriter::AddRow() 
{
    for (int channelIndex = 0; 
        channelIndex < ChannelWriters.size(); 
        ++channelIndex) 
    {
        TChannelWriter::TPtr channel = ChannelWriters[channelIndex];
        channel->AddRow(CurrentRow);
        if (channel->GetCurrentSize() > Config.BlockSize) {
            AddBlock(channelIndex);
        }
    }

    // NB: here you can extract sample key and other 
    // meta required for master

    ++CurrentRowIndex;
    CurrentRow.clear();
}

void TTableWriter::AddBlock(int channelIndex)
{
    auto* blockInfo = ChunkMeta.MutableChannels(channelIndex)->AddBlocks();
    blockInfo->SetBlockIndex(CurrentBlockIndex);
    blockInfo->SetEndRow(CurrentRowIndex);

    auto channel = ChannelWriters[channelIndex];
    auto data = Compressor->Compress(channel->FlushBlock());
    ChunkWriter->WriteBlock(data);
    ++CurrentBlockIndex;
}

void TTableWriter::Close()
{
    YASSERT(CurrentRow.empty());

    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++ channelIndex) {
        auto channel = ChannelWriters[channelIndex];
        if (channel->HasData()) {
            AddBlock(channelIndex);
        }
    }

    ChunkMeta.SetCompressor(Compressor->GetId());

    TBlob metaBlock(ChunkMeta.ByteSize());
    YASSERT(ChunkMeta.SerializeToArray(metaBlock.begin(), metaBlock.size()));

    ChunkWriter->WriteBlock(TSharedRef(metaBlock));
    ChunkWriter->Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
