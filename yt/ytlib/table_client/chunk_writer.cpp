#include "stdafx.h"
#include "chunk_writer.h"

#include "../actions/action_util.h"
#include "../chunk_client/writer_thread.h"
#include "../misc/assert.h"
#include "../misc/serialize.h"

namespace NYT {
namespace NTableClient {

using NChunkClient::WriterThread;
using NChunkClient::TChunkId;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriter::TChunkWriter(
    const TConfig& config, 
    NChunkClient::IAsyncWriter::TPtr chunkWriter,
    const TSchema& schema,
    ECodecId codecId)
    : Config(config)
    , Schema(schema)
    , CodecId(codecId)
    , ChunkWriter(chunkWriter)
    , CurrentBlockIndex(0)
    , SentSize(0)
    , CurrentSize(0)
{
    YASSERT(~chunkWriter != NULL);
    
    // Fill protobuf chunk meta.
    FOREACH(auto channel, Schema.GetChannels()) {
        *ChunkMeta.AddChannels() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }
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
    TAsyncStreamState::TResult result,
    int channelIndex)
{
    if (result.IsOK) {
        while (channelIndex < ChannelWriters.ysize()) {
            auto channel = ChannelWriters[channelIndex];
            channel->EndRow();
            CurrentSize += channel->GetCurrentSize();

            if (channel->GetCurrentSize() > static_cast<size_t>(Config.BlockSize)) {
                auto data = PrepareBlock(channelIndex);
                ChunkWriter->AsyncWriteBlock(data)->Subscribe(FromMethod(
                    &TChunkWriter::ContinueEndRow,
                    TPtr(this),
                    channelIndex + 1)->Via(WriterThread->GetInvoker()));

                return;
            } 
            ++channelIndex;
        }
    }

    State.FinishOperation(result);
}

TAsyncStreamState::TAsyncResult::TPtr TChunkWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    CurrentSize = SentSize;
    UsedColumns.clear();

    State.StartOperation();
    ContinueEndRow(State.GetCurrentResult(), 0);

    return State.GetOperationResult();
}

TSharedRef TChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = ChannelWriters[channelIndex];

    NProto::TBlockInfo* blockInfo = ChunkMeta.MutableChannels(channelIndex)->AddBlocks();
    blockInfo->SetBlockIndex(CurrentBlockIndex);
    blockInfo->SetRowCount(channel->GetCurrentRowCount());

    auto& codec = ICodec::GetCodec(CodecId);
    auto data = codec.Encode(channel->FlushBlock());

    SentSize += data.Size();
    ++CurrentBlockIndex;

    return data;
}

TChunkWriter::~TChunkWriter()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.IsActive());
}

i64 TChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

void TChunkWriter::OnClosed(TAsyncStreamState::TResult result)
{
    State.Finish(result);
}

void TChunkWriter::ContinueClose(
    TAsyncStreamState::TResult result,
    int channelIndex)
{
    // ToDo: consider separate thread for this background blocks 
    // processing. As far as this function is usually driven by 
    // window of RemoteChunkWriter using it for table processing 
    // slows window shifts.
    VERIFY_THREAD_AFFINITY_ANY();

    if (!result.IsOK) {
        State.FinishOperation(result);
        return;
    }

    for (; channelIndex < ChannelWriters.ysize(); ++ channelIndex) {
        auto channel = ChannelWriters[channelIndex];
        if (channel->HasUnflushedData()) {
            auto data = PrepareBlock(channelIndex);
            ChunkWriter->AsyncWriteBlock(data)->Subscribe(FromMethod(
                &TChunkWriter::ContinueClose,
                TPtr(this),
                channelIndex + 1));
            return;
        }
    }

    ChunkMeta.SetCodecId(CodecId);

    TBlob metaBlob;
    if (!SerializeProtobuf(&ChunkMeta, &metaBlob)) {
        LOG_FATAL("Failed to serialize table chunk meta");
    }
    
    ChunkWriter->AsyncWriteBlock(MoveRV(metaBlob))->Subscribe(FromMethod(
        &TChunkWriter::FinishClose,
        TPtr(this)));
}

TAsyncStreamState::TAsyncResult::TPtr TChunkWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YASSERT(UsedColumns.empty());
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    State.StartOperation();

    ContinueClose(State.GetCurrentResult(), 0);

    return State.GetOperationResult();
}

void TChunkWriter::Cancel(const Stroka& errorMessage)
{
    VERIFY_THREAD_AFFINITY_ANY();

    State.Cancel(errorMessage);
    ChunkWriter->Cancel(errorMessage);
}

TChunkId TChunkWriter::GetChunkId() const
{
    return ChunkWriter->GetChunkId();
}

void TChunkWriter::FinishClose(TAsyncStreamState::TResult result)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!result.IsOK) {
        State.FinishOperation(result);
        return;
    }

    // ToDo: create real master meta!!!
    // At least: number of rows (required for job distribution in maps).
    TSharedRef masterMeta;
    ChunkWriter->AsyncClose(masterMeta)->Subscribe(FromMethod(
        &TChunkWriter::OnClosed,
        TPtr(this)));
}

TAsyncStreamState::TAsyncResult::TPtr TChunkWriter::AsyncOpen()
{
    // Stub to implement IWriter interface.
    VERIFY_THREAD_AFFINITY(ClientThread);
    return State.GetOperationResult();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
