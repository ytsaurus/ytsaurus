#ifndef MULTI_CHUNK_READER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_reader_base.h"
#endif
#undef MULTI_CHUNK_READER_BASE_INL_H_

#include "private.h"
#include "config.h"
#include "chunk_replica.h"
#include "block_cache.h"
#include "replication_reader.h"
#include "async_reader.h"
#include "dispatcher.h"
#include "chunk_meta_extensions.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/rpc/channel.h>

#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TMultiChunkReaderBase<TChunkReader>::TMultiChunkReaderBase(
    TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    std::vector<NChunkClient::NProto::TInputChunk>&& inputChunks,
    const TProviderPtr& readerProvider)
    : IsFetchingComplete_(false)
    , Config(config)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , NodeDirectory(nodeDirectory)
    , InputChunks(inputChunks)
    , ReaderProvider(readerProvider)
    , LastPreparedReader(-1)
    , FetchingCompleteAwaiter(New<TParallelAwaiter>())
    , Logger(ChunkReaderLogger)
{
    std::vector<i64> chunkDataSizes;
    chunkDataSizes.reserve(InputChunks.size());

    FOREACH (const auto& inputChunk, InputChunks) {
        i64 dataSize;
        NChunkClient::GetStatistics(inputChunk, &dataSize);
        chunkDataSizes.push_back(dataSize);
    }

    if (ReaderProvider->KeepInMemory()) {
        PrefetchWindow = MaxPrefetchWindow;
    } else {
        std::sort(chunkDataSizes.begin(), chunkDataSizes.end(), std::greater<i64>());

        PrefetchWindow = 0;
        i64 bufferSize = 0;
        while (PrefetchWindow < chunkDataSizes.size()) {
            bufferSize += std::min(
                chunkDataSizes[PrefetchWindow],
                config->WindowSize) + ChunkReaderMemorySize;
            if (bufferSize > Config->MaxBufferSize) {
                break;
            } else {
                ++PrefetchWindow;
            }
        }

        PrefetchWindow = std::min(PrefetchWindow, MaxPrefetchWindow);
        PrefetchWindow = std::max(PrefetchWindow, 1);
    }
    LOG_DEBUG("Preparing reader (PrefetchWindow: %d)",
        PrefetchWindow);
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::PrepareNextChunk()
{
    int inputChunksCount = static_cast<int>(InputChunks.size());

    int chunkIndex = -1;

    {
        TGuard<TSpinLock> guard(NextChunkLock);
        LastPreparedReader = std::min(LastPreparedReader + 1, inputChunksCount);
        if (LastPreparedReader == inputChunksCount) {
            return;
        }
        chunkIndex = LastPreparedReader;
    }

    TSession session;
    session.ChunkIndex = chunkIndex;
    const auto& inputChunk = InputChunks[chunkIndex];
    auto chunkId = NYT::FromProto<NChunkClient::TChunkId>(inputChunk.chunk_id());
    auto replicas = NYT::FromProto<NChunkClient::TChunkReplica, NChunkClient::TChunkReplicaList>(inputChunk.replicas());

    LOG_DEBUG("Opening chunk (ChunkIndex: %d, ChunkId: %s)",
        chunkIndex,
        ~ToString(chunkId));

    auto remoteReader = CreateReplicationReader(
        Config,
        BlockCache,
        MasterChannel,
        NodeDirectory,
        Null,
        chunkId,
        replicas);

    session.Reader = ReaderProvider->CreateReader(inputChunk, remoteReader);

    session.Reader->AsyncOpen()
        .Subscribe(BIND(
            &TMultiChunkReaderBase<TChunkReader>::OnReaderOpened,
            MakeWeak(this),
            session)
        .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::ProcessOpenedReader(const TSession& session)
{
    LOG_DEBUG("Chunk opened (ChunkIndex: %d)", session.ChunkIndex);
    ReaderProvider->OnReaderOpened(session.Reader, InputChunks[session.ChunkIndex]);

    FetchingCompleteAwaiter->Await(session.Reader->GetFetchingCompleteEvent());
    if (FetchingCompleteAwaiter->GetRequestCount() == InputChunks.size()) {
        auto this_ = MakeStrong(this);
        FetchingCompleteAwaiter->Complete(BIND([=]() {
            this_->IsFetchingComplete_ = true;
        }));
    }
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::ProcessFinishedReader(const TSession& session)
{
    ReaderProvider->OnReaderFinished(session.Reader);
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::AddFailedChunk(const TSession& session)
{
    const auto& inputChunk = InputChunks[session.ChunkIndex];
    auto chunkId = NYT::FromProto<NChunkClient::TChunkId>(inputChunk.chunk_id());
    LOG_DEBUG("Failed chunk added (ChunkId: %s)", ~ToString(chunkId));
    TGuard<TSpinLock> guard(FailedChunksLock);
    FailedChunks.push_back(chunkId);
}

template <class TChunkReader>
std::vector<NChunkClient::TChunkId> TMultiChunkReaderBase<TChunkReader>::GetFailedChunks() const
{
    TGuard<TSpinLock> guard(FailedChunksLock);
    return FailedChunks;
}

template <class TChunkReader>
TAsyncError TMultiChunkReaderBase<TChunkReader>::GetReadyEvent()
{
    return State.GetOperationError();
}

template <class TChunkReader>
auto TMultiChunkReaderBase<TChunkReader>::GetFacade() const -> const TFacade*
{
    YASSERT(!State.HasRunningOperation());
    if (CurrentSession.Reader) {
        return CurrentSession.Reader->GetFacade();
    } else {
        return nullptr;
    }
}

template <class TChunkReader>
auto TMultiChunkReaderBase<TChunkReader>::GetProvider() -> TProviderPtr
{
    return ReaderProvider;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
