#include "stdafx.h"
#include "table_chunk_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableChunkWriter::TTableChunkWriter(
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
    YASSERT(~chunkWriter != NULL);

    // Fill protobuf chunk meta.
    FOREACH(auto channel, Schema.GetChannels()) {
        *ChunkMeta.AddChannels() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }
}

void TTableChunkWriter::Write(const TColumn& column, TValue value)
{
    YASSERT(!UsedColumns.has(column));
    UsedColumns.insert(column);
    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->Write(column, value);
    }
}

void TTableChunkWriter::EndRow()
{
    for (int channelIndex = 0; 
        channelIndex < ChannelWriters.ysize(); 
        ++channelIndex) 
    {
        auto channel = ChannelWriters[channelIndex];
        channel->EndRow();
        if (channel->GetCurrentSize() > static_cast<size_t>(Config.BlockSize)) {
            AddBlock(channelIndex);
        }
    }

    // NB: here you can extract sample key and other 
    // meta required for master

    UsedColumns.clear();
}

// thread may block here, if chunkwriter window is overfilled
void TTableChunkWriter::AddBlock(int channelIndex)
{
    auto channel = ChannelWriters[channelIndex];

    NProto::TBlockInfo* blockInfo = ChunkMeta.MutableChannels(channelIndex)->AddBlocks();
    blockInfo->SetBlockIndex(CurrentBlockIndex);
    blockInfo->SetRowCount(channel->GetCurrentRowCount());

    auto data = Codec->Encode(channel->FlushBlock());
    ChunkWriter->WriteBlock(data);
    ++CurrentBlockIndex;
}

void TTableChunkWriter::Close()
{
    if (IsClosed) {
        return;
    }

    YASSERT(UsedColumns.empty());

    for (int channelIndex = 0; channelIndex < ChannelWriters.ysize(); ++ channelIndex) {
        auto channel = ChannelWriters[channelIndex];
        if (channel->HasUnflushedData()) {
            AddBlock(channelIndex);
        }
    }

    ChunkMeta.SetCodecId(Codec->GetId());

    TBlob metaBlock(ChunkMeta.ByteSize());
    YASSERT(ChunkMeta.SerializeToArray(metaBlock.begin(), static_cast<int>(metaBlock.size())));

    ChunkWriter->WriteBlock(TSharedRef(MoveRV(metaBlock)));
    ChunkWriter->Close();
    IsClosed = true;
}

TTableChunkWriter::~TTableChunkWriter()
{
    Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
