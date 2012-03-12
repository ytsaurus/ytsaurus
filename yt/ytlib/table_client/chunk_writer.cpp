#include "stdafx.h"
#include "chunk_writer.h"
#include "table_chunk_meta.pb.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/chunk_client/writer_thread.h>
#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

using namespace std;
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
    , SentSize(0)
    , CurrentSize(0)
    , UncompressedSize(0)
{
    YASSERT(chunkWriter);
    Codec = GetCodec(ECodecId(Config->CodecId));
}

TAsyncError::TPtr TChunkWriter::AsyncOpen(
    const NProto::TTableChunkAttributes& attributes)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    IsOpen = true;

    Attributes = attributes;
    Attributes.set_codec_id(Config->CodecId);
    Attributes.set_row_count(0);

    return New<TAsyncError>();
}

TAsyncError::TPtr TChunkWriter::AsyncEndRow(
    TKey& key,
    std::vector<TChannelWriter::TPtr>& channels)
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

    // Keys sampling
    if (LastSampleSize == 0 || 
        (CurrentSize - LastSampleSize > Config->SamplingSize))
    {
        LastSampleSize = CurrentSize;
        AddKeySample(key);
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
{
}

i64 TChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

TAsyncError::TPtr TChunkWriter::AsyncClose(
    TKey& lastKey,
    std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    IsClosed = true;

    std::vector<TSharedRef> completedBlocks;
    for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
        auto& channel = channels[channelIndex];
        CurrentSize += channel->GetCurrentSize();

        if (channel->HasUnflushedData()) {
            auto block = PrepareBlock(channel, channelIndex);
            completedBlocks.push_back(block);
        } 
    }

    // Sample last key (if it is not already sampled by coincidence).
    if (CurrentSize > LastSampleSize) {
        AddKeySample(lastKey);
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

void TChunkWriter::AddKeySample(const TKey& key)
{
    auto* keySample = Attributes.add_key_samples();
    FOREACH (auto& keyPart, key) {
        keySample->mutable_key()->add_values(keyPart);
    }

    keySample->set_row_index(Attributes.row_count());
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
