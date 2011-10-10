#include "table_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    const TConfig& config, 
    IChunkWriter::TPtr chunkWriter,
    const TSchema& schema,
    ICodec::TPtr codec)
    : IsClosed(false)
    , Config(config)
    , CurrentBlockIndex(0)
    , CurrentRowIndex(0)
    , ChunkWriter(chunkWriter)
    , Schema(schema)
    , Codec(codec)
{
    // Fill protobuf chunk meta
    FOREACH(auto channel, Schema.Channels()) {
        auto* protoChannel = ChunkMeta.AddChannels();
        channel.FillProto(protoChannel);
        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }
}

TTableWriter& TTableWriter::Write(TColumn column, TValue value)
{
    YASSERT(!SetColumns.has(column));
    SetColumns.insert(column);
    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->Write(column, value);
    }
    return *this;
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

    SetColumns.clear();
    ++CurrentRowIndex;
}

void TTableWriter::AddBlock(int channelIndex)
{
    NProto::TBlockInfo* blockInfo = ChunkMeta.MutableChannels(channelIndex)->AddBlocks();
    blockInfo->SetBlockIndex(CurrentBlockIndex);
    blockInfo->SetLastRow(CurrentRowIndex);

    auto channel = ChannelWriters[channelIndex];
    auto data = Codec->Compress(channel->FlushBlock());
    ChunkWriter->WriteBlock(data);
    ++CurrentBlockIndex;
}

void TTableWriter::Close()
{
    if (IsClosed) {
        return;
    }

    YASSERT(SetColumns.empty());

    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++ channelIndex) {
        auto channel = ChannelWriters[channelIndex];
        if (channel->HasUnflushedData()) {
            AddBlock(channelIndex);
        }
    }

    ChunkMeta.SetCodec(Codec->GetId());

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
