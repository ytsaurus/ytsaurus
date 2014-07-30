#ifndef MULTI_CHUNK_READER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include old_multi_chunk_reader_base.h"
#endif
#undef MULTI_CHUNK_READER_BASE_INL_H_

#include "private.h"
#include "config.h"
#include "block_cache.h"
#include "reader.h"
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

template <class TChunkReader>
TOldMultiChunkReaderBase<TChunkReader>::TOldMultiChunkReaderBase(
    TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
    const TProviderPtr& readerProvider)
    : IsFetchingComplete_(false)
    , Config(config)
    , PrefetchWindow(0)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , NodeDirectory(nodeDirectory)
    , ChunkSpecs(chunkSpecs)
    , ReaderProvider(readerProvider)
    , LastPreparedReader(-1)
    , FetchingCompleteAwaiter(New<NConcurrency::TParallelAwaiter>(GetSyncInvoker()))
    , Logger(ChunkClientLogger)
{
    if (ChunkSpecs.empty()) {
        return;
    }

    for (const auto& chunkSpec : ChunkSpecs) {
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
    }

    if (ReaderProvider->KeepInMemory()) {
        PrefetchWindow = MaxPrefetchWindow;
    } else {
        auto sortedChunkSpecs = ChunkSpecs;
        std::sort(sortedChunkSpecs.begin(), sortedChunkSpecs.end(), [] (
            const NChunkClient::NProto::TChunkSpec& lhs,
            const NChunkClient::NProto::TChunkSpec& rhs)
        {
            i64 lhsDataSize, rhsDataSize;
            NChunkClient::GetStatistics(lhs, &lhsDataSize);
            NChunkClient::GetStatistics(rhs, &rhsDataSize);

            return lhsDataSize > rhsDataSize;
        });

        i64 smallestDataSize;
        NChunkClient::GetStatistics(sortedChunkSpecs.back(), &smallestDataSize);

        if (smallestDataSize < config->WindowSize + config->GroupSize) {
            // Patch config to ensure that we don't eat too much memory.
            Config->WindowSize = std::max(smallestDataSize / 2, (i64) 1);
            Config->GroupSize = std::max(smallestDataSize / 2, (i64) 1);
        }

        PrefetchWindow = 0;
        i64 bufferSize = 0;
        while (PrefetchWindow < sortedChunkSpecs.size()) {
            auto& chunkSpec = sortedChunkSpecs[PrefetchWindow];
            i64 currentSize;   
            NChunkClient::GetStatistics(chunkSpec, &currentSize);
            auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());

            // block that possibly exceeds group size + block used by upper level chunk reader.
            i64 chunkBufferSize = ChunkReaderMemorySize + 2 * miscExt.max_block_size();

            if (currentSize > miscExt.max_block_size()) {
                chunkBufferSize += config->WindowSize + config->GroupSize;
            } 

            if (bufferSize + chunkBufferSize > Config->MaxBufferSize) {
                break;
            } else {
                bufferSize += chunkBufferSize;
                ++PrefetchWindow;
            }
        }
        // Don't allow overcommit during prefetching, so exclude the last chunk.
        PrefetchWindow = std::max(PrefetchWindow - 1, 0);
        PrefetchWindow = std::min(PrefetchWindow, MaxPrefetchWindow);
    }

    LOG_DEBUG("Preparing reader (PrefetchWindow: %d)", PrefetchWindow);
}

template <class TChunkReader>
void TOldMultiChunkReaderBase<TChunkReader>::PrepareNextChunk()
{
    if (!State.IsActive()) {
        return;
    }
    
    int chunkSpecsCount = static_cast<int>(ChunkSpecs.size());

    int chunkIndex = -1;

    {
        TGuard<TSpinLock> guard(NextChunkLock);
        LastPreparedReader = std::min(LastPreparedReader + 1, chunkSpecsCount);
        if (LastPreparedReader == chunkSpecsCount) {
            return;
        }
        chunkIndex = LastPreparedReader;
    }

    TSession session;
    session.ChunkIndex = chunkIndex;
    const auto& chunkSpec = ChunkSpecs[chunkIndex];

    auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
    auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());

    LOG_DEBUG("Opening chunk (ChunkIndex: %v, ChunkId: %v)",
        chunkIndex,
        chunkId);

    IReaderPtr asyncReader;
    if (IsErasureChunkId(chunkId)) {
        auto erasureCodecId = NErasure::ECodec(chunkSpec.erasure_codec());
        auto* erasureCodec = NErasure::GetCodec(erasureCodecId);
        auto readers = CreateErasureDataPartsReaders(
            Config,
            BlockCache,
            MasterChannel,
            NodeDirectory,
            chunkId,
            replicas,
            erasureCodec);
        asyncReader = CreateNonReparingErasureReader(readers);
    } else {
        asyncReader = CreateReplicationReader(
            Config,
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
            &TOldMultiChunkReaderBase<TChunkReader>::OnReaderOpened,
            MakeWeak(this),
            session)
        .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
}

template <class TChunkReader>
void TOldMultiChunkReaderBase<TChunkReader>::ProcessOpenedReader(const TSession& session)
{
    LOG_DEBUG("Chunk opened (ChunkIndex: %d)", session.ChunkIndex);
    ReaderProvider->OnReaderOpened(session.Reader, ChunkSpecs[session.ChunkIndex]);

    FetchingCompleteAwaiter->Await(session.Reader->GetFetchingCompleteEvent());
    if (FetchingCompleteAwaiter->GetRequestCount() == ChunkSpecs.size()) {
        FetchingCompleteAwaiter->Complete(BIND(
            &TOldMultiChunkReaderBase<TChunkReader>::OnFetchingComplete,
            MakeWeak(this)));
    }
}

template <class TChunkReader>
void TOldMultiChunkReaderBase<TChunkReader>::OnFetchingComplete()
{
    IsFetchingComplete_ = true;
}

template <class TChunkReader>
void TOldMultiChunkReaderBase<TChunkReader>::ProcessFinishedReader(const TSession& session)
{
    ReaderProvider->OnReaderFinished(session.Reader);
}

template <class TChunkReader>
void TOldMultiChunkReaderBase<TChunkReader>::AddFailedChunk(const TSession& session)
{
    const auto& chunkSpec = ChunkSpecs[session.ChunkIndex];
    auto chunkId = NYT::FromProto<NChunkClient::TChunkId>(chunkSpec.chunk_id());
    LOG_DEBUG("Failed chunk added (ChunkId: %v)", chunkId);
    TGuard<TSpinLock> guard(FailedChunksLock);
    FailedChunks.push_back(chunkId);
}

template <class TChunkReader>
std::vector<NChunkClient::TChunkId> TOldMultiChunkReaderBase<TChunkReader>::GetFailedChunkIds() const
{
    TGuard<TSpinLock> guard(FailedChunksLock);
    return FailedChunks;
}

template <class TChunkReader>
TAsyncError TOldMultiChunkReaderBase<TChunkReader>::GetReadyEvent()
{
    return State.GetOperationError();
}

template <class TChunkReader>
auto TOldMultiChunkReaderBase<TChunkReader>::GetFacade() const -> const TFacade*
{
    YASSERT(!State.HasRunningOperation());
    if (CurrentSession.Reader) {
        return CurrentSession.Reader->GetFacade();
    } else {
        return nullptr;
    }
}

template <class TChunkReader>
auto TOldMultiChunkReaderBase<TChunkReader>::GetProvider() -> TProviderPtr
{
    return ReaderProvider;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
