#ifndef MULTI_CHUNK_READER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_reader_base.h"
#endif
#undef MULTI_CHUNK_READER_BASE_INL_H_

#include "private.h"
#include "config.h"
#include "block_cache.h"
#include "async_reader.h"
#include "dispatcher.h"
#include "chunk_meta_extensions.h"
#include "replication_reader.h"
#include "erasure_reader.h"

#include <core/actions/invoker_util.h>

#include <core/erasure/codec.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/rpc/channel.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

static int GetBufferSize(const NProto::TChunkSpec& chunkSpec, const TMultiChunkReaderConfigPtr& config)
{
    i64 currentSize;
    NChunkClient::GetStatistics(chunkSpec, &currentSize);
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.extensions());

    // block that possibly exceeds group size + block used by upper level chunk reader.
    i64 chunkBufferSize = ChunkReaderMemorySize + 2 * miscExt.max_block_size();

    if (currentSize > miscExt.max_block_size()) {
        chunkBufferSize += config->WindowSize + config->GroupSize;
    } 

    return chunkBufferSize;
}

static TMultiChunkReaderConfigPtr PatchReaderConfig(TMultiChunkReaderConfigPtr config, int bufferSize)
{
    auto newConfig = config;
    if (bufferSize < config->WindowSize + config->GroupSize) {
        // Patch config to ensure that we don't eat too much memory.
        newConfig = CloneYsonSerializable(config);
        newConfig->WindowSize = std::max(bufferSize / 2, 1);
        newConfig->GroupSize = std::max(bufferSize / 2, 1);
    }

    return newConfig;
}

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TMultiChunkReaderBase<TChunkReader>::TMultiChunkReaderBase(
    TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
    const TProviderPtr& readerProvider)
    : IsFetchingComplete_(false)
    , Config(config)
    , FreeBufferSize(Config->MaxBufferSize)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , NodeDirectory(nodeDirectory)
    , ReaderProvider(readerProvider)
    , NextReaderIndex(0)
    , FetchingCompleteAwaiter(New<NConcurrency::TParallelAwaiter>(GetSyncInvoker()))
    , Logger(ChunkReaderLogger)
{
    if (chunkSpecs.empty()) {
        return;
    }

    FOREACH (const auto& chunkSpec, chunkSpecs) {
        if (IsUnavailable(chunkSpec)) {
            auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
            FailedChunks.push_back(chunkId);

            auto error = TError(
                "Chunk %s is unavailable",
                ~ToString(chunkId));
            LOG_ERROR(error);
            State.Fail(error);
            return;
        }

        Chunks.emplace_back(TChunk{chunkSpec, GetBufferSize(chunkSpec, config)});
    }
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::PrepareNextChunks()
{
    TGuard<TSpinLock> guard(NextChunkLock);
    for (; NextReaderIndex < Chunks.size(); ++NextReaderIndex) {
        if (Chunks[NextReaderIndex].BufferSize > FreeBufferSize &&
            ActiveReaderCount > 0 &&
            !ReaderProvider->KeepInMemory()) 
        {
            return;
        }

        if (ActiveReaderCount > MaxPrefetchWindow) {
            return;
        }

        ++ActiveReaderCount;
        FreeBufferSize -= Chunks[NextReaderIndex].BufferSize;
        TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
            &TMultiChunkReaderBase<TChunkReader>::DoPrepareChunk,
            MakeWeak(this),
            NextReaderIndex));
    }
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::DoPrepareChunk(int chunkIndex)
{
    if (!State.IsActive()) {
        return;
    }

    TSession session;
    session.ChunkIndex = chunkIndex;
    const auto& chunkSpec = Chunks[chunkIndex].ChunkSpec;

    auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
    auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());

    LOG_DEBUG("Opening chunk (ChunkIndex: %d, ChunkId: %s)",
        chunkIndex,
        ~ToString(chunkId));

    auto config = PatchReaderConfig(Config, Chunks[chunkIndex].BufferSize);
    IAsyncReaderPtr asyncReader;
    if (IsErasureChunkId(chunkId)) {
        auto erasureCodecId = NErasure::ECodec(chunkSpec.erasure_codec());
        auto* erasureCodec = NErasure::GetCodec(erasureCodecId);
        auto readers = CreateErasureDataPartsReaders(
            config,
            BlockCache,
            MasterChannel,
            NodeDirectory,
            chunkId,
            replicas,
            erasureCodec);
        asyncReader = CreateNonReparingErasureReader(readers);
    } else {
        asyncReader = CreateReplicationReader(
            config,
            BlockCache,
            MasterChannel,
            NodeDirectory,
            Null,
            chunkId,
            replicas);
    }

    session.Reader = ReaderProvider->CreateReader(chunkSpec, asyncReader);

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
    ReaderProvider->OnReaderOpened(session.Reader, Chunks[session.ChunkIndex].ChunkSpec);

    FetchingCompleteAwaiter->Await(session.Reader->GetFetchingCompleteEvent());
    if (FetchingCompleteAwaiter->GetRequestCount() == Chunks.size()) {
        FetchingCompleteAwaiter->Complete(BIND(
            &TMultiChunkReaderBase<TChunkReader>::OnFetchingComplete,
            MakeWeak(this)));
    }
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::OnFetchingComplete()
{
    IsFetchingComplete_ = true;
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::ProcessFinishedReader(const TSession& session)
{
    ReaderProvider->OnReaderFinished(session.Reader);
    --ActiveReaderCount;
    FreeBufferSize += Chunks[session.ChunkIndex].BufferSize;
}

template <class TChunkReader>
void TMultiChunkReaderBase<TChunkReader>::AddFailedChunk(const TSession& session)
{
    const auto& chunkSpec = Chunks[session.ChunkIndex].ChunkSpec;
    auto chunkId = NYT::FromProto<NChunkClient::TChunkId>(chunkSpec.chunk_id());
    LOG_DEBUG("Failed chunk added (ChunkId: %s)", ~ToString(chunkId));
    TGuard<TSpinLock> guard(FailedChunksLock);
    FailedChunks.push_back(chunkId);
}

template <class TChunkReader>
std::vector<NChunkClient::TChunkId> TMultiChunkReaderBase<TChunkReader>::GetFailedChunkIds() const
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
