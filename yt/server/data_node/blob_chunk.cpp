#include "blob_chunk.h"
#include "private.h"
#include "blob_reader_cache.h"
#include "chunk_block_manager.h"
#include "chunk_cache.h"
#include "location.h"
#include "chunk_meta_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/misc/memory_usage_tracker.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/file_reader.h>
#include <yt/ytlib/chunk_client/file_writer.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/common.h>

#include <yt/core/profiling/scoped_timer.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

static NProfiling::TSimpleCounter DiskBlobReadByteCounter("/blob_block_read_bytes");

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
    const TWorkloadDescriptor& workloadDescriptor,
    const TNullable<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunkMetaManager = Bootstrap_->GetChunkMetaManager();
    auto cookie = chunkMetaManager->BeginInsertCachedMeta(Id_);
    auto result = cookie.GetValue();

    try {
        if (cookie.IsActive()) {
            auto readGuard = TChunkReadGuard::AcquireOrThrow(this);

            auto callback = BIND(
                &TBlobChunkBase::DoReadMeta,
                MakeStrong(this),
                Passed(std::move(readGuard)),
                Passed(std::move(cookie)));

            auto priority = workloadDescriptor.GetPriority();
            Location_
                ->GetMetaReadInvoker()
                ->Invoke(callback, priority);
        }
    } catch (const std::exception& ex) {
        cookie.Cancel(ex);
    }

    return result.Apply(BIND([=] (TCachedChunkMetaPtr cachedMeta) {
        return FilterMeta(cachedMeta->GetMeta(), extensionTags);
    }));
}

TFuture<void> TBlobChunkBase::LoadBlocksExt(const TWorkloadDescriptor& workloadDescriptor)
{
    {
        // Shortcut.
        TReaderGuard guard(CachedBlocksExtLock_);
        if (HasCachedBlocksExt_) {
            return VoidFuture;
        }
    }

    return ReadMeta(workloadDescriptor).Apply(
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

void TBlobChunkBase::DoReadMeta(
    TChunkReadGuard /*readGuard*/,
    TCachedChunkMetaCookie cookie)
{
    const auto& Profiler = Location_->GetProfiler();
    LOG_DEBUG("Started reading chunk meta (LocationId: %v, ChunkId: %v)",
        Location_->GetId(),
        Id_);

    NChunkClient::TFileReaderPtr reader;
    PROFILE_TIMING("/meta_read_time") {
        auto readerCache = Bootstrap_->GetBlobReaderCache();
        try {
            reader = readerCache->GetReader(this);
        } catch (const std::exception& ex) {
            cookie.Cancel(ex);
            return;
        }
    }

    LOG_DEBUG("Finished reading chunk meta (LocationId: %v, ChunkId: %v)",
        Location_->GetId(),
        Id_);

    const auto& meta = reader->GetMeta();
    auto cachedMeta = New<TCachedChunkMeta>(
        Id_,
        New<TRefCountedChunkMeta>(meta),
        Bootstrap_->GetMemoryUsageTracker());
    cookie.EndInsert(cachedMeta);
}

TFuture<void> TBlobChunkBase::OnBlocksExtLoaded(
    TReadBlockSetSessionPtr session,
    const TWorkloadDescriptor& workloadDescriptor)
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
                pendingBlockCount >= config->MaxBlocksPerRead) {
                break;
            }
        }
    }

    int totalBlockCount = cachedBlockCount + pendingBlockCount;
    session->Entries.resize(totalBlockCount);
    session->Blocks.resize(totalBlockCount);

    auto pendingIOGuard = Location_->IncreasePendingIOSize(
        EIODirection::Read,
        workloadDescriptor,
        pendingDataSize);

    // Actually serve the request: delegate to the appropriate thread.
    auto priority = workloadDescriptor.GetPriority();
    auto invoker = CreateFixedPriorityInvoker(Location_->GetDataReadInvoker(), priority);
    return
        BIND(
            &TBlobChunkBase::DoReadBlockSet,
            MakeStrong(this),
            session,
            workloadDescriptor,
            Passed(std::move(pendingIOGuard)))
        .AsyncVia(std::move(invoker))
        .Run();
}

void TBlobChunkBase::DoReadBlockSet(
    TReadBlockSetSessionPtr session,
    const TWorkloadDescriptor& workloadDescriptor,
    TPendingIOGuard /*pendingIOGuard*/)
{
    auto& locationProfiler = Location_->GetProfiler();
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

        LOG_DEBUG(
            "Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            firstBlockIndex + beginIndex,
            firstBlockIndex + endIndex - 1,
            Location_->GetId());

        NProfiling::TScopedTimer timer;
        // NB: The reader is synchronous.
        auto blocksOrError = reader->ReadBlocks(firstBlockIndex, blocksToRead).Get();
        auto readTime = timer.GetElapsed();

        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading blob chunk %v",
                Id_)
                << TError(blocksOrError);
            Location_->Disable(error);
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
                    data = TSharedRef::MakeCopy<TCachedBlobChunkBlockTag>(data);
                }

                auto blockId = TBlockId(Id_, entry.BlockIndex);
                auto cachedBlock = New<TCachedBlock>(blockId, std::move(data), Null);
                entry.Cookie.EndInsert(cachedBlock);
            }
        }

        LOG_DEBUG(
            "Finished reading blob chunk blocks (BlockIds: %v:%v-%v, BytesRead: %v, LocationId: %v)",
            Id_,
            firstBlockIndex + beginIndex,
            firstBlockIndex + endIndex - 1,
            bytesRead,
            Location_->GetId());

        locationProfiler.Enqueue("/blob_block_read_size", bytesRead);
        locationProfiler.Enqueue("/blob_block_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/blob_block_read_throughput", bytesRead * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskBlobReadByteCounter, bytesRead);

        currentIndex = endIndex;
    }
}

TFuture<std::vector<TSharedRef>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TWorkloadDescriptor& workloadDescriptor,
    bool populateCache,
    IBlockCachePtr blockCache)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunkBlockManager = Bootstrap_->GetChunkBlockManager();

    bool canServeFromCache = true;

    auto session = New<TReadBlockSetSession>();
    session->Entries.resize(blockIndexes.size());
    session->Blocks.resize(blockIndexes.size());

    std::vector<TFuture<void>> asyncResults;

    for (int localIndex = 0; localIndex < blockIndexes.size(); ++localIndex) {
        auto& entry = session->Entries[localIndex];
        entry.LocalIndex = localIndex;
        entry.BlockIndex = blockIndexes[localIndex];

        auto blockId = TBlockId(Id_, entry.BlockIndex);
        auto block = blockCache->Find(blockId, EBlockType::CompressedData);
        if (block) {
            session->Blocks[entry.LocalIndex] = std::move(block);
            entry.Cached = true;
        } else if (populateCache) {
            entry.Cookie = chunkBlockManager->BeginInsertCachedBlock(blockId);
            if (!entry.Cookie.IsActive()) {
                entry.Cached = true;
                auto asyncCachedBlock = entry.Cookie.GetValue().Apply(
                    BIND([session, localIndex] (const TCachedBlockPtr& cachedBlock) {
                        session->Blocks[localIndex] = cachedBlock->GetData();
                    }));
                asyncResults.emplace_back(std::move(asyncCachedBlock));
            }
        }

        if (!entry.Cached) {
            canServeFromCache = false;
        }
    }

    // Fast path: we can serve request right away.
    if (canServeFromCache && asyncResults.empty()) {
        return MakeFuture(std::move(session->Blocks));
    }

    // Slow path: either read data from chunk or wait for the cache to be filled.
    if (!canServeFromCache) {
        // Reorder blocks sequentially to improve read performance.
        std::sort(
            session->Entries.begin(),
            session->Entries.end(),
            [] (const TReadBlockSetSession::TBlockEntry& lhs, const TReadBlockSetSession::TBlockEntry& rhs) {
                return lhs.BlockIndex < rhs.BlockIndex;
            });

        auto asyncBlocksExtResult = LoadBlocksExt(workloadDescriptor);
        auto asyncReadResult = asyncBlocksExtResult.Apply(
            BIND(&TBlobChunkBase::OnBlocksExtLoaded, MakeStrong(this), session, workloadDescriptor));
        asyncResults.push_back(asyncReadResult);
    }

    auto asyncResult = Combine(asyncResults);
    return asyncResult.Apply(BIND([session] () { return std::move(session->Blocks); }));
}

TFuture<std::vector<TSharedRef>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TWorkloadDescriptor& workloadDescriptor,
    bool populateCache,
    IBlockCachePtr blockCache)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    std::vector<int> blockIndexes;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blockIndexes.push_back(blockIndex);
    }

    return ReadBlockSet(
        blockIndexes,
        workloadDescriptor,
        populateCache,
        std::move(blockCache));
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
