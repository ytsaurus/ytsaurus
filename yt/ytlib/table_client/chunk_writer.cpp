#include "stdafx.h"
#include "chunk_writer.h"
#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/chunk_client/writer_thread.h>
#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkHolder::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriter::TChunkWriter(
    TConfig* config, 
    NChunkClient::IAsyncWriter* chunkWriter)
    : Config(config)
    , ChunkWriter(chunkWriter)
    , IsOpen(false)
    , IsClosed(false)
    , CurrentBlockIndex(0)
    , LastSampleSize(0)
    , SentSize(0)
    , CurrentSize(0)
    , UncompressedSize(0)
{
    YASSERT(chunkWriter);
    Codec = GetCodec(ECodecId(Config->CodecId));
}

TAsyncError TChunkWriter::AsyncOpen(
    const NProto::TTableChunkAttributes& attributes)
{
    // No thread affinity check here - 
    // TChunkSequenceWriter may call it from different threads.
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    IsOpen = true;

    Attributes = attributes;
    Attributes.set_codec_id(Config->CodecId);
    Attributes.set_row_count(0);

    return MakeFuture(TError());
}

TAsyncError TChunkWriter::AsyncEndRow(
    const TKey& key,
    const std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    CurrentSize = SentSize;

    Attributes.set_row_count(Attributes.row_count() + 1);

    std::vector<TSharedRef> completedBlocks;
    for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
        auto& channel = channels[channelIndex];
        CurrentSize += channel->GetCurrentSize();

        if (channel->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
            auto block = PrepareBlock(channel, channelIndex);
            completedBlocks.push_back(block);
        } 
    }

    LastKey = key;

    // Keys sampling
    if (LastSampleSize == 0 || 
        (CurrentSize - LastSampleSize > Config->SamplingSize))
    {
        LastSampleSize = CurrentSize;
        AddKeySample();
    }

    return ChunkWriter->AsyncWriteBlocks(MoveRV(completedBlocks));
}

TSharedRef TChunkWriter::PrepareBlock(
    TChannelWriter::TPtr channel, 
    int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProto::TBlockInfo* blockInfo = Attributes.mutable_chunk_channels(channelIndex)->add_blocks();
    blockInfo->set_block_index(CurrentBlockIndex);
    blockInfo->set_row_count(channel->GetCurrentRowCount());

    auto block = channel->FlushBlock();
    UncompressedSize += block.Size();

    auto data = Codec->Compress(block);

    SentSize += data.Size();
    ++CurrentBlockIndex;

    return data;
}

TChunkWriter::~TChunkWriter()
{ }

i64 TChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

TAsyncError TChunkWriter::AsyncClose(
    const std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    IsClosed = true;

    std::vector<TSharedRef> completedBlocks;
    for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
        auto& channel = channels[channelIndex];

        if (channel->HasUnflushedData()) {
            auto block = PrepareBlock(channel, channelIndex);
            completedBlocks.push_back(block);
        } 
    }

    CurrentSize = SentSize;

    // Sample last key (if it is not already sampled by coincidence).
    auto& lastSample = *(--Attributes.key_samples().end());
    if (Attributes.row_count() > lastSample.row_index() + 1) {
        AddKeySample();
    }

    Attributes.set_uncompressed_size(UncompressedSize);
    TChunkAttributes attributes;
    attributes.set_type(EChunkType::Table);
    *attributes.MutableExtension(NProto::TTableChunkAttributes::table_attributes) = Attributes;

    return ChunkWriter->AsyncClose(MoveRV(completedBlocks), attributes);
}

TChunkId TChunkWriter::GetChunkId() const
{
    return ChunkWriter->GetChunkId();
}

void TChunkWriter::AddKeySample()
{
    auto* sample = Attributes.add_key_samples();

    //ToDo: use ToProto here when std::vector will be supported.
    auto* protoKey = sample->mutable_key();
    FOREACH (auto& keyPart, LastKey) {
        protoKey->add_values(keyPart);
    }

    sample->set_row_index(Attributes.row_count() - 1);

    YASSERT(Attributes.key_samples_size() <= Attributes.row_count());
}

NChunkServer::TChunkYPathProxy::TReqConfirm::TPtr 
TChunkWriter::GetConfirmRequest()
{
    YASSERT(IsClosed);
    return ChunkWriter->GetConfirmRequest();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
