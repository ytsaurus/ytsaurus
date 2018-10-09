#include "blob_chunk.h"
#include "private.h"
#include "blob_reader_cache.h"
#include "chunk_block_manager.h"
#include "chunk_cache.h"
#include "location.h"
#include "chunk_meta_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/file_reader.h>
#include <yt/ytlib/chunk_client/file_writer.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TBlobChunkBase::TBlobChunkBase(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TChunkMeta* meta)
    : TChunkBase(
        bootstrap,
        location,
        descriptor.Id)
{
    Info_.set_disk_space(descriptor.DiskSpace);

    if (meta) {
        InitBlocksExt(*meta);

        auto chunkMetaManager = Bootstrap_->GetChunkMetaManager();
        chunkMetaManager->PutCachedMeta(Id_, New<TRefCountedChunkMeta>(*meta));
    }
}

TChunkInfo TBlobChunkBase::GetInfo() const
{
    return Info_;
}

bool TBlobChunkBase::IsActive() const
{
    return false;
}

TFuture<TRefCountedChunkMetaPtr> TBlobChunkBase::ReadMeta(
    const TBlockReadOptions& options,
    const TNullable<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunkMetaManager = Bootstrap_->GetChunkMetaManager();
    auto cookie = chunkMetaManager->BeginInsertCachedMeta(Id_);
    auto result = cookie.GetValue();

    auto priority = options.WorkloadDescriptor.GetPriority();
    try {
        if (cookie.IsActive()) {
            auto readGuard = TChunkReadGuard::AcquireOrThrow(this);

            auto callback = BIND(
                &TBlobChunkBase::DoReadMeta,
                MakeStrong(this),
                Passed(std::move(readGuard)),
                Passed(std::move(cookie)),
                options);

            Bootstrap_
                ->GetChunkBlockManager()->GetReaderInvoker()
                ->Invoke(callback, priority);
        }
    } catch (const std::exception& ex) {
        cookie.Cancel(ex);
    }

    return result.Apply(BIND([=] (TCachedChunkMetaPtr cachedMeta) {
        return FilterMeta(cachedMeta->GetMeta(), extensionTags);
    })
       .AsyncVia(CreateFixedPriorityInvoker(Bootstrap_->GetChunkBlockManager()->GetReaderInvoker(), priority)));
}

TFuture<void> TBlobChunkBase::LoadBlocksExt(const TBlockReadOptions& options)
{
    {
        // Shortcut.
        TReaderGuard guard(CachedBlocksExtLock_);
        if (HasCachedBlocksExt_) {
            return VoidFuture;
        }
    }

    return ReadMeta(options).Apply(
        BIND([=, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
            InitBlocksExt(*meta);
        }));
}

const TBlocksExt& TBlobChunkBase::GetBlocksExt()
{
    TReaderGuard guard(CachedBlocksExtLock_);
    YCHECK(HasCachedBlocksExt_);
    return CachedBlocksExt_;
}

void TBlobChunkBase::InitBlocksExt(const TChunkMeta& meta)
{
    TWriterGuard guard(CachedBlocksExtLock_);
    // NB: Avoid redundant updates since readers access CachedBlocksExt_ by const ref
    // and use no locking.
    if (!HasCachedBlocksExt_) {
        CachedBlocksExt_ = GetProtoExtension<TBlocksExt>(meta.extensions());
        HasCachedBlocksExt_ = true;
    }
}

bool TBlobChunkBase::IsFatalError(const TError& error) const
{
    if (error.FindMatching(NChunkClient::EErrorCode::BlockOutOfRange) || 
        error.FindMatching(NYT::EErrorCode::Canceled)) 
    {
        return false;
    }

    return true;
}

void TBlobChunkBase::DoReadMeta(
    TChunkReadGuard /*readGuard*/,
    TCachedChunkMetaCookie cookie,
    const TBlockReadOptions& options)
{
    const auto& Profiler = Location_->GetProfiler();
    LOG_DEBUG("Started reading chunk meta (ChunkId: %v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
        Id_,
        Location_->GetId(),
        options.WorkloadDescriptor,
        options.ReadSessionId);

    TChunkMeta meta;
    PROFILE_TIMING("/meta_read_time") {
        try {
            auto readerCache = Bootstrap_->GetBlobReaderCache();
            auto reader = readerCache->GetReader(this);
            meta = WaitFor(reader->GetMeta(options))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            cookie.Cancel(ex);
            return;
        }
    }

    LOG_DEBUG("Finished reading chunk meta (ChunkId: %v, LocationId: %v, ReadSessionId: %v)",
        Id_,
        Location_->GetId(),
        options.ReadSessionId);

    auto cachedMeta = New<TCachedChunkMeta>(
        Id_,
        New<TRefCountedChunkMeta>(std::move(meta)),
        Bootstrap_->GetMemoryUsageTracker());
    cookie.EndInsert(cachedMeta);
}

TFuture<void> TBlobChunkBase::OnBlocksExtLoaded(
    TReadBlockSetSessionPtr session)
{
    // Prepare to serve the request: compute pending data size.
    i64 cachedDataSize = 0;
    i64 pendingDataSize = 0;
    int cachedBlockCount = 0;
    int pendingBlockCount = 0;

    auto config = Bootstrap_->GetConfig()->DataNode;
    const auto& blocksExt = GetBlocksExt();

    for (int index = 0; index < session->Entries.size(); ++index) {
        const auto& entry = session->Entries[index];
        auto blockDataSize = blocksExt.blocks(entry.BlockIndex).size();

        if (entry.Cached) {
            cachedDataSize += blockDataSize;
            ++cachedBlockCount;
        } else {
            pendingDataSize += blockDataSize;
            ++pendingBlockCount;
            if (pendingDataSize >= config->MaxBytesPerRead ||
                pendingBlockCount >= config->MaxBlocksPerRead)
            {
                break;
            }
        }
    }

    int totalBlockCount = cachedBlockCount + pendingBlockCount;
    session->Entries.resize(totalBlockCount);
    session->Blocks.resize(totalBlockCount);

    const auto& outThrottler = Location_->GetOutThrottler(session->Options.WorkloadDescriptor);
    auto throttleFuture = VoidFuture;
    if (!outThrottler->TryAcquire(pendingDataSize)) {
        LOG_DEBUG("Disk read throttling is active (PendingDataSize: %v, WorkloadDescriptor: %v)", 
            pendingDataSize, 
            session->Options.WorkloadDescriptor);
        throttleFuture = outThrottler->Throttle(pendingDataSize);
    }

    // Actually serve the request: delegate to the appropriate thread.
    return
        throttleFuture.Apply(BIND([=, this_ = MakeStrong(this)] {
            auto pendingIOGuard = Location_->IncreasePendingIOSize(
                EIODirection::Read,
                session->Options.WorkloadDescriptor,
                pendingDataSize);
            // Note that outer Apply checks that the return value is of type
            // TError and returns the TFuture<void> instead of TFuture<TError> here.
            TBlobChunkBase::DoReadBlockSet(
                session,
                std::move(pendingIOGuard));
        }).AsyncVia(CreateFixedPriorityInvoker(
            Bootstrap_->GetChunkBlockManager()->GetReaderInvoker(),
            session->Options.WorkloadDescriptor.GetPriority())));
}

void TBlobChunkBase::DoReadBlockSet(
    TReadBlockSetSessionPtr session,
    TPendingIOGuard /*pendingIOGuard*/)
{
    auto reader = Bootstrap_->GetBlobReaderCache()->GetReader(this);

    int currentIndex = 0;
    while (currentIndex < session->Entries.size()) {
        if (session->Entries[currentIndex].Cached) {
            ++currentIndex;
            continue;
        }

        int beginIndex = currentIndex;
        int endIndex = currentIndex;
        int firstBlockIndex = session->Entries[beginIndex].BlockIndex;

        while (
            endIndex < session->Entries.size() &&
            !session->Entries[endIndex].Cached &&
            session->Entries[endIndex].BlockIndex == firstBlockIndex + (endIndex - beginIndex))
        {
            ++endIndex;
        }

        int blocksToRead = endIndex - beginIndex;

        LOG_DEBUG("Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
            Id_,
            firstBlockIndex + beginIndex,
            firstBlockIndex + endIndex - 1,
            Location_->GetId(),
            session->Options.WorkloadDescriptor,
            session->Options.ReadSessionId);

        TWallTimer timer;
        auto blocksOrError = WaitFor(reader->ReadBlocks(
            session->Options,
            firstBlockIndex,
            blocksToRead,
            Null));
        auto readTime = timer.GetElapsedTime();

        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading blob chunk %v",
                Id_)
                << TError(blocksOrError);
            if (IsFatalError(blocksOrError)) {
                Location_->Disable(error);
                Y_UNREACHABLE();
            }
            THROW_ERROR error;
        }

        const auto& blocks = blocksOrError.Value();
        YCHECK(blocks.size() == blocksToRead);

        i64 bytesRead = 0;
        for (int index = beginIndex; index < endIndex; ++index) {
            auto data = blocks[index - beginIndex];
            bytesRead += data.Size();

            auto& entry = session->Entries[index];

            session->Blocks[entry.LocalIndex] = data;

            if (entry.Cookie.IsActive()) {
                struct TCachedBlobChunkBlockTag {};

                // NB: Prevent cache from holding the whole block sequence.
                if (blocks.size() > 1) {
                    data.Data = TSharedRef::MakeCopy<TCachedBlobChunkBlockTag>(data.Data);
                }

                auto blockId = TBlockId(Id_, entry.BlockIndex);
                auto cachedBlock = New<TCachedBlock>(blockId, std::move(data), Null);
                entry.Cookie.EndInsert(cachedBlock);
            }
        }

        LOG_DEBUG("Finished reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BytesRead: %v, "
            "Time: %v, ReadSessionId: %v)",
            Id_,
            firstBlockIndex + beginIndex,
            firstBlockIndex + endIndex - 1,
            Location_->GetId(),
            bytesRead,
            readTime,
            session->Options.ReadSessionId);

        const auto& locationProfiler = Location_->GetProfiler();
        auto& performanceCounters = Location_->GetPerformanceCounters();
        locationProfiler.Update(performanceCounters.BlobBlockReadSize, bytesRead);
        locationProfiler.Update(performanceCounters.BlobBlockReadTime, NProfiling::DurationToValue(readTime));
        locationProfiler.Update(performanceCounters.BlobBlockReadThroughput, bytesRead * 1000000 / (1 + readTime.MicroSeconds()));

        Location_->IncreaseCompletedIOSize(EIODirection::Read, session->Options.WorkloadDescriptor, bytesRead);

        currentIndex = endIndex;
    }
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadBlockSetSession>();
    session->Entries.resize(blockIndexes.size());
    session->Blocks.resize(blockIndexes.size());
    session->Options = options;

    bool diskFetchNeeded = false;
    std::vector<TFuture<void>> asyncResults;
    for (int localIndex = 0; localIndex < blockIndexes.size(); ++localIndex) {
        auto& entry = session->Entries[localIndex];
        entry.LocalIndex = localIndex;
        entry.BlockIndex = blockIndexes[localIndex];

        auto blockId = TBlockId(Id_, entry.BlockIndex);
        auto block = options.FetchFromCache && options.BlockCache
            ? options.BlockCache->Find(blockId, EBlockType::CompressedData)
            : TBlock();
        if (block) {
            session->Options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
            session->Blocks[entry.LocalIndex] = std::move(block);
            entry.Cached = true;
        } else if (options.FetchFromDisk && options.PopulateCache) {
            auto chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            entry.Cookie = chunkBlockManager->BeginInsertCachedBlock(blockId);
            if (!entry.Cookie.IsActive()) {
                entry.Cached = true;
                auto asyncCachedBlock = entry.Cookie.GetValue().Apply(
                    BIND([session, localIndex] (const TCachedBlockPtr& cachedBlock) {
                        auto block = cachedBlock->GetData();
                        session->Options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
                        session->Blocks[localIndex] = std::move(block);
                    }));
                asyncResults.emplace_back(std::move(asyncCachedBlock));
            }
        }

        if (!entry.Cached) {
            diskFetchNeeded = true;
        }
    }

    // Fast path: we can serve request right away.
    if (!diskFetchNeeded && asyncResults.empty()) {
        return MakeFuture(std::move(session->Blocks));
    }

    // Slow path: either read data from chunk or wait for the cache to be filled.
    if (options.FetchFromDisk && diskFetchNeeded) {
        // Reorder blocks sequentially to improve read performance.
        std::sort(
            session->Entries.begin(),
            session->Entries.end(),
            [] (const TReadBlockSetSession::TBlockEntry& lhs, const TReadBlockSetSession::TBlockEntry& rhs) {
                return lhs.BlockIndex < rhs.BlockIndex;
            });

        auto asyncBlocksExtResult = LoadBlocksExt(options);
        auto asyncReadResult = asyncBlocksExtResult.Apply(
            BIND(&TBlobChunkBase::OnBlocksExtLoaded, MakeStrong(this), session));
        asyncResults.push_back(asyncReadResult);
    }

    auto asyncResult = Combine(asyncResults);
    return asyncResult.Apply(BIND([session = std::move(session)] () {
        return std::move(session->Blocks);
    }));
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    std::vector<int> blockIndexes;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blockIndexes.push_back(blockIndex);
    }

    return ReadBlockSet(blockIndexes, options);
}

void TBlobChunkBase::SyncRemove(bool force)
{
    auto readerCache = Bootstrap_->GetBlobReaderCache();
    readerCache->EvictReader(this);

    Location_->RemoveChunkFiles(Id_, force);
}

TFuture<void> TBlobChunkBase::AsyncRemove()
{
    return BIND(&TBlobChunkBase::SyncRemove, MakeStrong(this), false)
        .AsyncVia(Location_->GetWritePoolInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

TStoredBlobChunk::TStoredBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TChunkMeta* meta)
    : TBlobChunkBase(
        bootstrap,
        location,
        descriptor,
        meta)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedBlobChunk::TCachedBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TChunkMeta* meta,
    const TArtifactKey& key,
    TClosure destroyed)
    : TBlobChunkBase(
        bootstrap,
        location,
        descriptor,
        meta)
    , TAsyncCacheValueBase<TArtifactKey, TCachedBlobChunk>(key)
    , Destroyed_(destroyed)
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    Destroyed_.Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
