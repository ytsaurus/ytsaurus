#ifndef MULTI_CHUNK_READER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_reader_base.h"
#endif
#undef MULTI_CHUNK_READER_BASE_INL_H_

#include "private.h"
#include "config.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TMultiChunkReaderBase<TChunkReader>::TMultiChunkReaderBase(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks,
    const TProviderPtr& readerProvider)
    : Config(config)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , InputChunks(inputChunks)
    , ReaderProvider(readerProvider)
    , FetchingCompleteAwaiter(New<TParallelAwaiter>())
    , Logger(TableReaderLogger)
    , LastPreparedReader(-1)
    , ItemIndex_(0)
    , ItemCount_(0)
    , IsFetchingComplete_(false)
{
    FOREACH(const auto& inputChunk, InputChunks) {
        ItemCount_ += inputChunk.row_count();
    }
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::PrepareNextChunk()
{
    int chunkSlicesSize = static_cast<int>(InputChunks.size());

    LastPreparedReader = std::min(LastPreparedReader + 1, chunkSlicesSize);
    if (LastPreparedReader == chunkSlicesSize) {
        return;
    }

    const auto& inputChunk = InputChunks[LastPreparedReader];
    auto chunkId = NChunkServer::TChunkId::FromProto(inputChunk.slice().chunk_id());

    LOG_DEBUG("Opening chunk (ChunkIndex: %d, ChunkId: %s)", 
        LastPreparedReader,
        ~chunkId.ToString());

    auto remoteReader = CreateRemoteReader(
        Config,
        BlockCache,
        MasterChannel,
        chunkId,
        FromProto<Stroka>(inputChunk.node_addresses()));

    auto chunkReader = ReaderProvider->CreateNewReader(inputChunk, remoteReader);
    chunkReader->AsyncOpen().Subscribe(BIND(
        &TMultiChunkReaderBase<TChunkReader>::OnReaderOpened,
        MakeWeak(this),
        chunkReader,
        LastPreparedReader).Via(NChunkClient::ReaderThread->GetInvoker()));
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::ProcessOpenedReader(
    const TReaderPtr& reader, 
    int chunkIndex)
{
    LOG_DEBUG("Chunk opened (ChunkIndex: %d)", chunkIndex);
    ItemCount_ += reader->GetRowCount() - InputChunks[chunkIndex].row_count();
    FetchingCompleteAwaiter->Await(reader->GetFetchingCompleteEvent());
    if (FetchingCompleteAwaiter->GetRequestCount() == InputChunks.size()) {
        auto this_ = MakeStrong(this);
        FetchingCompleteAwaiter->Complete(BIND([=]() { 
            this_->IsFetchingComplete_ = true; 
        }));
    }
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::ProcessFinishedReader(const TReaderPtr& reader)
{
    ItemCount_ += reader->GetRowIndex() - reader->GetRowCount();
}

template <class TChunkReader>
TAsyncError TMultiChunkReaderBase<TChunkReader>::GetReadyEvent()
{
    return State.GetOperationError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
