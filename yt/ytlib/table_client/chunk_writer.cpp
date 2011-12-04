#include "stdafx.h"
#include "chunk_writer.h"

#include "../actions/action_util.h"
#include "../chunk_client/writer_thread.h"
#include "../misc/assert.h"
#include "../misc/serialize.h"
#include "table_chunk_meta.pb.h"

namespace NYT {
namespace NTableClient {

using NChunkClient::WriterThread;
using NChunkClient::TChunkId;
using NChunkClient::EChunkType;
using namespace NChunkServer::NProto;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriter::TChunkWriter(
    const TConfig& config, 
    NChunkClient::IAsyncWriter::TPtr chunkWriter,
    const TSchema& schema)
    : Config(config)
    , Schema(schema)
    , ChunkWriter(chunkWriter)
    , CurrentBlockIndex(0)
    , SentSize(0)
    , CurrentSize(0)
{
    YASSERT(~chunkWriter != NULL);
    
    Attributes.SetCodecId(Config.CodecId);
    Attributes.SetRowCount(0);

    // Fill protobuf chunk meta.
    FOREACH(auto channel, Schema.GetChannels()) {
        *Attributes.AddChannels() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel));
    }

    Codec = GetCodec(Config.CodecId);
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
    
    Attributes.SetRowCount(Attributes.GetRowCount() + 1);

    State.StartOperation();
    ContinueEndRow(State.GetCurrentResult(), 0);

    return State.GetOperationResult();
}

TSharedRef TChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = ChannelWriters[channelIndex];

    NProto::TBlockInfo* blockInfo = Attributes.MutableChannels(channelIndex)->AddBlocks();
    blockInfo->SetBlockIndex(CurrentBlockIndex);
    blockInfo->SetRowCount(channel->GetCurrentRowCount());

    auto data = Codec->Compress(channel->FlushBlock());

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
    int startChannelIndex /* = 0 */)
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

    // Write attributes.
    TChunkAttributes attributes;
    attributes.SetType(EChunkType::Table);
    *attributes.MutableExtension(TTableChunkAttributes::TableAttributes) = Attributes;
    
    ChunkWriter->AsyncClose(attributes)->Subscribe(FromMethod(
        &TChunkWriter::OnClosed,
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

TAsyncStreamState::TAsyncResult::TPtr TChunkWriter::AsyncOpen()
{
    // Stub to implement IWriter interface.
    VERIFY_THREAD_AFFINITY(ClientThread);
    return State.GetOperationResult();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
