#include "stdafx.h"
#include "chunk_writer.h"
#include "table_chunk_meta.pb.h"

#include "../actions/action_util.h"
#include "../chunk_client/writer_thread.h"
#include "../misc/assert.h"
#include "../misc/serialize.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkHolder::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriter::TChunkWriter(
    TConfig* config, 
    NChunkClient::IAsyncWriter::TPtr chunkWriter,
    const TSchema& schema)
    : Config(config)
    , Schema(schema)
    , ChunkWriter(chunkWriter)
    , CurrentBlockIndex(0)
    , SentSize(0)
    , CurrentSize(0)
    , UncompressedSize(0)
{
    YASSERT(chunkWriter);
    
    Attributes.set_codec_id(Config->CodecId);
    Attributes.set_row_count(0);

    // Fill protobuf chunk meta.
    FOREACH(auto channel, Schema.GetChannels()) {
        *Attributes.add_chunk_channels()->mutable_channel() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }

    Codec = GetCodec(ECodecId(Config->CodecId));
}

void TChunkWriter::Write(const TColumn& column, TValue value)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    YASSERT(!UsedColumns.has(column));

    UsedColumns.insert(column);
    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->Write(column, value);
    }
}

void TChunkWriter::ContinueEndRow(
    TError error,
    int channelIndex)
{
    if (error.IsOK()) {
        while (channelIndex < ChannelWriters.ysize()) {
            auto channel = ChannelWriters[channelIndex];
            channel->EndRow();
            CurrentSize += channel->GetCurrentSize();

            if (channel->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
                auto block = PrepareBlock(channelIndex);
                ChunkWriter->AsyncWriteBlock(block)->Subscribe(
                    FromMethod(
                        &TChunkWriter::ContinueEndRow,
                        TPtr(this),
                        channelIndex + 1)
                    ->Via(WriterThread->GetInvoker()));
                return;
            } 
            ++channelIndex;
        }
    }

    State.FinishOperation(error);
}

TAsyncError::TPtr TChunkWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    CurrentSize = SentSize;
    UsedColumns.clear();
    
    Attributes.set_row_count(Attributes.row_count() + 1);

    State.StartOperation();
    ContinueEndRow(State.GetCurrentError(), 0);

    return State.GetOperationError();
}

TSharedRef TChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = ChannelWriters[channelIndex];

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
    YASSERT(!State.IsActive());
}

i64 TChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

void TChunkWriter::OnClosed(TError error)
{
    State.Finish(error);
}

void TChunkWriter::ContinueClose(
    TError error,
    int startChannelIndex /* = 0 */)
{
    // ToDo: consider separate thread for this background blocks 
    // processing. As far as this function is usually driven by 
    // window of RemoteChunkWriter using it for table processing 
    // slows window shifts.
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.FinishOperation(error);
        return;
    }

    // Flush trailing blocks.
    int channelIndex = startChannelIndex;
    while (channelIndex < ChannelWriters.ysize()) {
        auto channel = ChannelWriters[channelIndex];
        if (channel->HasUnflushedData()) {
            break;
        }
        ++channelIndex;
    }

    if (channelIndex < ChannelWriters.ysize()) {
        auto data = PrepareBlock(channelIndex);
        ChunkWriter->AsyncWriteBlock(data)->Subscribe(FromMethod(
            &TChunkWriter::ContinueClose,
            TPtr(this),
            channelIndex + 1));
        return;
    }

    // Write attribute
    Attributes.set_uncompressed_size(UncompressedSize);

    TChunkAttributes attributes;
    attributes.set_type(EChunkType::Table);
    *attributes.MutableExtension(NProto::TTableChunkAttributes::table_attributes) = Attributes;
    
    ChunkWriter->AsyncClose(attributes)->Subscribe(FromMethod(
        &TChunkWriter::OnClosed,
        TPtr(this)));
}

TAsyncError::TPtr TChunkWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YASSERT(UsedColumns.empty());
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    State.StartOperation();

    ContinueClose(State.GetCurrentError(), 0);

    return State.GetOperationError();
}

void TChunkWriter::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    State.Cancel(error);
    ChunkWriter->Cancel(error);
}

TChunkId TChunkWriter::GetChunkId() const
{
    return ChunkWriter->GetChunkId();
}

TAsyncError::TPtr TChunkWriter::AsyncOpen()
{
    // Stub to implement IWriter interface.
    VERIFY_THREAD_AFFINITY(ClientThread);
    return State.GetOperationError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
