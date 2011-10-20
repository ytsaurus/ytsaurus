#include "stdafx.h"
#include "table_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    const TConfig& config, 
    IChunkWriter::TPtr chunkWriter,
    const TSchema& schema,
    ICodec* codec)
    : IsClosed(false)
    , Config(config)
    , CurrentBlockIndex(0)
    , ChunkWriter(chunkWriter)
    , Schema(schema)
    , Codec(codec)
{
    // Fill protobuf chunk meta
    FOREACH(auto channel, Schema.GetChannels()) {
        *ChunkMeta.AddChannels() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }
}

void TTableWriter::Write(const TColumn& column, TValue value)
{
    YASSERT(!UsedColumns.has(column));
    UsedColumns.insert(column);
    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->Write(column, value);
    }
}

void TTableWriter::EndRow()
{
    for (int channelIndex = 0; 
        channelIndex < ChannelWriters.size(); 
        ++channelIndex) 
    {
        auto channel = ChannelWriters[channelIndex];
        channel->EndRow();
        if (channel->GetCurrentSize() > Config.BlockSize) {
            AddBlock(channelIndex);
        }
    }

    // NB: here you can extract sample key and other 
    // meta required for master

    UsedColumns.clear();
}

// thread may block here, if chunkwriter window is overfilled
void TTableWriter::AddBlock(int channelIndex)
{
    auto channel = ChannelWriters[channelIndex];

    NProto::TBlockInfo* blockInfo = ChunkMeta.MutableChannels(channelIndex)->AddBlocks();
    blockInfo->SetBlockIndex(CurrentBlockIndex);
    blockInfo->SetRowCount(channel->GetCurrentRowCount());

    auto data = Codec->Encode(channel->FlushBlock());
    ChunkWriter->WriteBlock(data);
    ++CurrentBlockIndex;
}

void TTableWriter::Close()
{
    if (IsClosed) {
        return;
    }

    YASSERT(UsedColumns.empty());

    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++ channelIndex) {
        auto channel = ChannelWriters[channelIndex];
        if (channel->HasUnflushedData()) {
            AddBlock(channelIndex);
        }
    }

    ChunkMeta.SetCodecId(Codec->GetId());

    TBlob metaBlock(ChunkMeta.ByteSize());
    YASSERT(ChunkMeta.SerializeToArray(metaBlock.begin(), metaBlock.size()));

    ChunkWriter->WriteBlock(TSharedRef(metaBlock));
    ChunkWriter->Close();
    IsClosed = true;
}

TTableWriter::~TTableWriter()
{
    Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
