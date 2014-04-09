#include "stdafx.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

int CalculatePrefetchWindow(const std::vector<TChunkSpec>& sortedChunkSpecs, TMultiChunkReaderConfigPtr config)
{
    int prefetchWindow = 0;
    i64 bufferSize = 0;
    while (prefetchWindow < sortedChunkSpecs.size()) {
        auto& chunkSpec = sortedChunkSpecs[prefetchWindow];
        i64 currentSize;
        GetStatistics(chunkSpec, &currentSize);
        auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());

        // block that possibly exceeds group size + block used by upper level chunk reader.
        i64 chunkBufferSize = ChunkReaderMemorySize + 2 * miscExt.max_block_size();

        if (currentSize > miscExt.max_block_size()) {
            chunkBufferSize += config->WindowSize + config->GroupSize;
        } 

        if (bufferSize + chunkBufferSize > config->MaxBufferSize) {
            break;
        } else {
            bufferSize += chunkBufferSize;
            ++prefetchWindow;
        }
    }
    // Don't allow overcommit during prefetching, so exclude the last chunk.
    prefetchWindow = std::max(prefetchWindow - 1, 0);
    prefetchWindow = std::min(prefetchWindow, MaxPrefetchWindow);
    return prefetchWindow;
}

////////////////////////////////////////////////////////////////////////////////

TNontemplateMultiChunkReaderBase::TNontemplateMultiChunkReaderBase(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const std::vector<NProto::TChunkSpec>& chunkSpecs)
    : Logger(ChunkReaderLogger)
    , ChunkSpecs_(chunkSpecs)
    , Options_(options)
    , MasterChannel_(masterChannel)
    , BlockCache_(blockCache)
    , NodeDirectory_(nodeDirectory)
    , IsOpen_(false)
    , LastOpenedReaderIndex_(-1)
{
    Config_ = CloneYsonSerializable(config);

    if (ChunkSpecs_.empty()) {
        LOG_DEBUG("Preparing reader - no chunk specs");
        CompletionError_.Set(TError());
        return;
    }

    if (Options_->KeepInMemory) {
        PrefetchWindow_ = MaxPrefetchWindow;
    } else {
        auto sortedChunkSpecs = ChunkSpecs_;
        std::sort(
            sortedChunkSpecs.begin(), 
            sortedChunkSpecs.end(), 
            [] (const TChunkSpec& lhs, const TChunkSpec& rhs)
            {
                i64 lhsDataSize, rhsDataSize;
                GetStatistics(lhs, &lhsDataSize);
                GetStatistics(rhs, &rhsDataSize);

                return lhsDataSize > rhsDataSize;
            });

        i64 smallestDataSize;
        GetStatistics(sortedChunkSpecs.back(), &smallestDataSize);

        if (smallestDataSize < Config_->WindowSize + Config_->GroupSize) {
            // Here we limit real consumption to correspond the estimated.
            Config_->WindowSize = std::max(smallestDataSize / 2, (i64) 1);
            Config_->GroupSize = std::max(smallestDataSize / 2, (i64) 1);
        }

        PrefetchWindow_ = CalculatePrefetchWindow(sortedChunkSpecs, Config_);
    }

    LOG_DEBUG("Preparing reader (PrefetchWindow: %d)", PrefetchWindow);
}

TAsyncError TNontemplateMultiChunkReaderBase::Open()
{
    YCHECK(!IsOpen_);
    if (CompletionError_.IsSet())
        return CompletionError_.ToFuture();

    return BIND(&TNontemplateMultiChunkReaderBase::DoOpen, MakeWeak(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}


////////////////////////////////////////////////////////////////////////////////

TAsyncError TNontemplateSequentialMultiChunkReaderBase::DoOpen()
{

    
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
