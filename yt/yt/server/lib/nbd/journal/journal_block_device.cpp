#include "journal_block_device.h"

#include "block_flusher.h"
#include "block_map.h"
#include "block_store.h"
#include "config.h"
#include "dirty_block_pool.h"
#include "snapshot_reader.h"
#include "snapshot_writer.h"

#include <yt/yt/server/lib/nbd/journal/records/snapshot_block.record.h>

#include <yt/yt/server/lib/nbd/block_device_detail.h>
#include <yt/yt/server/lib/nbd/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCleanBlock
    : public TAsyncCacheValueBase<TStoredBlockId, TCleanBlock>
{
public:
    TCleanBlock(TStoredBlockId blockId, TSharedRef payload)
        : TAsyncCacheValueBase(blockId)
        , Payload_(std::move(payload))
    { }

    const TSharedRef& GetPayload() const
    {
        return Payload_;
    }

private:
    const TSharedRef Payload_;
};

using TCleanBlockPtr = TIntrusivePtr<TCleanBlock>;

////////////////////////////////////////////////////////////////////////////////

class TBlockCache
    : public TAsyncSlruCacheBase<TStoredBlockId, TCleanBlock>
{
public:
    explicit TBlockCache(TSlruCacheConfigPtr config)
        : TAsyncSlruCacheBase(std::move(config))
    { }

protected:
    i64 GetWeight(const TCleanBlockPtr& value) const override
    {
        return value->GetPayload().Size();
    }
};

using TBlockCachePtr = TIntrusivePtr<TBlockCache>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJournalBlockDevice)

class TJournalBlockDevice
    : public TBlockDeviceBase
    , public IJournalBlockDevice
{
public:
    TJournalBlockDevice(
        std::string deviceId,
        TJournalBlockDeviceConfigPtr config,
        TJournalBlockDeviceOptionsPtr options,
        TTransactionId transactionId,
        NChunkClient::TChunkListId chunkListId,
        std::optional<TSnapshotLoadSpec> snapshotReadSpec,
        NNative::IClientPtr client,
        NLogging::TLogger logger)
        : DeviceId_(std::move(deviceId))
        , Config_(std::move(config))
        , Options_(std::move(options))
        , ThreadPool_(CreateThreadPool(Config_->ThreadPoolSize, "JournalNbd"))
        , Invoker_(ThreadPool_->GetInvoker())
        , Logger(std::move(logger))
        , Client_(std::move(client))
        , SnapshotReadSpec_(std::move(snapshotReadSpec))
        , ExternalCellTag_(CellTagFromId(transactionId))
        , Geometry_(MakeGeometry(Options_->Size, Config_->BlockSize))
        , BlockMap_(CreateBlockMap(Geometry_.BlockCount))
        , DirtyPool_(CreateDirtyBlockPool(
            static_cast<int>(Config_->BlockFlusher->DirtyBlockPoolCapacity / Geometry_.BlockSize)))
        , BlockCache_(New<TBlockCache>(Config_->BlockCache))
        , EmptyBlock_(MakeEmptyBlock(Geometry_.BlockSize))
        , BlockStore_(CreateJournalBlockStore(
            Config_->BlockStore,
            Geometry_,
            Options_,
            Client_,
            transactionId,
            chunkListId,
            Invoker_,
            Logger))
        , BlockFlusher_(CreateBlockFlusher(
            Config_->BlockFlusher,
            DirtyPool_,
            BlockStore_,
            Invoker_,
            Logger))
    { }

    i64 GetTotalSize() const final
    {
        return Geometry_.GetByteSize();
    }

    i64 GetBlockSize() const final
    {
        // The journal chunks are block-granular; advertising this lets the kernel align I/O to it
        // and do any sub-block read-modify-write itself, so the device only ever sees whole blocks.
        return Geometry_.BlockSize;
    }

    bool IsReadOnly() const final
    {
        return false;
    }

    std::string GetDescription() const final
    {
        return Format("Journal{Size: %v}",
            Geometry_.GetByteSize());
    }

    std::string GetProfileSensorTag() const final
    {
        return DeviceId_;
    }

    TFuture<void> Initialize() final
    {
        return BIND(&TJournalBlockDevice::DoInitialize, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> Finalize() final
    {
        BlockFlusher_->Stop();

        YT_LOG_INFO("Journal block device finalized");

        return OKFuture;
    }

    TFuture<TReadResponse> Read(
        i64 offset,
        i64 length,
        const TReadOptions& options) final;

    TFuture<TWriteResponse> Write(
        i64 offset,
        const TSharedRef& data,
        const TWriteOptions& options) final;

    TFuture<void> Flush(const TFlushOptions& /*options*/) final
    {
        return OKFuture;
    }

    TCellTag GetExternalCellTag() const override
    {
        return ExternalCellTag_;
    }

    TFuture<void> SaveSnapshot(const TSnapshotSaveSpec& spec) override
    {
        return BIND(&TJournalBlockDevice::DoSaveSnapshot, MakeStrong(this), spec)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    class TReadSession;
    class TWriteSession;
    class TSnapshotSession;

    const std::string DeviceId_;
    const TJournalBlockDeviceConfigPtr Config_;
    const TJournalBlockDeviceOptionsPtr Options_;
    const NConcurrency::IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    const NNative::IClientPtr Client_;
    //! If set, the snapshot the device restores from on initialization.
    const std::optional<TSnapshotLoadSpec> SnapshotReadSpec_;
    //! The cell hosting the device's chunks (derived from its transaction); a snapshot table must be
    //! co-located here so it can reference them.
    const TCellTag ExternalCellTag_;

    const TBlockDeviceGeometry Geometry_;
    const IBlockMapPtr BlockMap_;
    const IDirtyBlockPoolPtr DirtyPool_;
    const TBlockCachePtr BlockCache_;
    const TSharedRef EmptyBlock_;
    const IBlockStorePtr BlockStore_;
    const IBlockFlusherPtr BlockFlusher_;

    struct TSnapshotInfo
    {
        NYPath::TYPath Path;
        TInstant Timestamp;
    };

    //! The path and completion time of the most recent #SaveSnapshot. Written on Invoker_ (in
    //! #DoSaveSnapshot) and read from the orchid producer (on an arbitrary thread).
    NThreading::TAtomicObject<std::optional<TSnapshotInfo>> LatestSnapshotInfo_;

    void DoBuildOrchid(NYson::IYsonConsumer* consumer) const override
    {
        auto snapshotInfo = LatestSnapshotInfo_.Load();

        NYTree::BuildYsonMapFragmentFluently(consumer)
            .Item("block_count").Value(Geometry_.BlockCount)
            .Item("used_block_count").Value(BlockMap_->GetUsedBlockCount())
            .Item("dirty_pool_size").Value(DirtyPool_->GetSize())
            .Item("dirty_pool_capacity").Value(DirtyPool_->GetCapacity())
            .Item("chunks").DoMap([this] (NYTree::TFluentMap fluent) {
                BlockStore_->BuildChunksOrchid(fluent);
            })
            // Whether the device was restored from a snapshot, and the latest one it has saved.
            .Item("restored_from_snapshot").Value(SnapshotReadSpec_.has_value())
            .DoIf(snapshotInfo.has_value(), [&] (NYTree::TFluentMap fluent) {
                fluent
                    .Item("latest_snapshot_path").Value(snapshotInfo->Path)
                    .Item("latest_snapshot_timestamp").Value(snapshotInfo->Timestamp);
            });
    }

    void DoInitialize()
    {
        if (SnapshotReadSpec_) {
            RestoreFromSnapshot(*SnapshotReadSpec_);
        }

        BlockFlusher_->SubscribeBlockFlushed(BIND(&TJournalBlockDevice::OnBlockFlushed, MakeWeak(this)));
        BlockFlusher_->SubscribeFailed(BIND(&TJournalBlockDevice::OnFlushFailed, MakeWeak(this)));
        BlockMap_->SubscribeStoredBlockDied(BIND(&TJournalBlockDevice::OnStoredBlockDied, MakeWeak(this)));

        BlockFlusher_->Start();

        YT_LOG_INFO("Journal block device initialized (BlockSize: %v, BlockCount: %v, DirtyPoolCapacity: %v)",
            Geometry_.BlockSize,
            Geometry_.BlockCount,
            DirtyPool_->GetCapacity());
    }

    //! Runs on Invoker_. Loads a previously saved snapshot.
    void RestoreFromSnapshot(const TSnapshotLoadSpec& readSpec)
    {
        YT_LOG_INFO("Restoring journal block device from snapshot");

        auto snapshotBlocks = ReadJournalSnapshot(Client_, readSpec, Logger);

        std::vector<TStoredBlockRef> blockRefs;
        blockRefs.reserve(snapshotBlocks.size());
        for (const auto& block : snapshotBlocks) {
            THROW_ERROR_EXCEPTION_UNLESS(
                0 <= block.Index && block.Index < Geometry_.BlockCount,
                "Snapshot block index %v is out of range [0, %v) for a device of size %v",
                block.Index,
                Geometry_.BlockCount,
                Geometry_.GetByteSize());
            blockRefs.push_back(block.Ref);
        }

        auto storedBlockIds = WaitFor(BlockStore_->RestoreBlocks(blockRefs))
            .ValueOrThrow();

        TBlockMapSnapshot snapshot;
        snapshot.Blocks.reserve(snapshotBlocks.size());
        for (int index = 0; index < std::ssize(snapshotBlocks); ++index) {
            snapshot.Blocks.emplace_back(snapshotBlocks[index].Index, ToMappedBlockId(storedBlockIds[index]));
        }
        BlockMap_->LoadSnapshot(snapshot);

        YT_LOG_INFO("Journal block device restored from snapshot (BlockCount: %v)",
            snapshotBlocks.size());
    }

    //! Runs on Invoker_. Saves a crash-consistent point-in-time snapshot of the device to a Cypress
    //! table, concurrently with ongoing writes.
    //! Defined out of line, below TSnapshotSession, which it instantiates.
    void DoSaveSnapshot(const TSnapshotSaveSpec& spec);

    struct TEmptyBlockTag
    { };

    static TSharedRef MakeEmptyBlock(i64 blockSize)
    {
        return TSharedMutableRef::Allocate<TEmptyBlockTag>(blockSize, {.InitializeStorage = true});
    }

    //! Validates the requested size against the block size and derives the device geometry. The block
    //! size (a positive power of two) is validated separately by the config postprocessor.
    static TBlockDeviceGeometry MakeGeometry(i64 byteSize, i64 blockSize)
    {
        THROW_ERROR_EXCEPTION_UNLESS(
            byteSize % blockSize == 0,
            "Journal device size %v must be divisible by block size %v",
            byteSize,
            blockSize);

        constexpr i64 MaxBlockCount = 1LL << 30;
        auto blockCount = byteSize / blockSize;
        THROW_ERROR_EXCEPTION_UNLESS(
            blockCount < MaxBlockCount,
            "Journal device block count %v must be less than %v",
            blockCount,
            MaxBlockCount);

        return TBlockDeviceGeometry{
            .BlockSize = blockSize,
            .BlockCount = blockCount,
        };
    }

    void OnBlockFlushed(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId)
    {
        // Publish as clean only if no newer write superseded this block since it was drained;
        // otherwise the newer (dirty) version stays and the flushed copy is left orphaned.
        if (BlockMap_->TryMakeClean(block->BlockIndex, block->BlockId, storedBlockId)) {
            if (auto cookie = BlockCache_->BeginInsert(storedBlockId); cookie.IsActive()) {
                cookie.EndInsert(New<TCleanBlock>(storedBlockId, block->Payload));
            }
        }
    }

    void OnFlushFailed(const TError& error)
    {
        SetError(error);
    }

    void OnStoredBlockDied(TStoredBlockId storedBlockId)
    {
        BlockStore_->ReleaseBlock(storedBlockId);
    }
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockDevice)

////////////////////////////////////////////////////////////////////////////////

class TJournalBlockDevice::TReadSession
    : public TRefCounted
{
public:
    TReadSession(TJournalBlockDevicePtr owner, i64 offset, i64 length)
        : Owner_(std::move(owner))
        , Offset_(offset)
        , Length_(length)
    { }

    TFuture<TReadResponse> Run()
    {
        try {
            DoRun();
        } catch (const std::exception& ex) {
            Promise_.TrySet(TError(ex));
        }
        return Promise_;
    }

private:
    const TJournalBlockDevicePtr Owner_;
    const i64 Offset_;
    const i64 Length_;

    const TPromise<TReadResponse> Promise_ = NewPromise<TReadResponse>();

    // (position within Payloads_, stored block id) of clean blocks missing from the cache.
    std::vector<std::pair<int, TStoredBlockId>> Misses_;

    std::vector<TSharedRef> Payloads_;

    void DoRun()
    {
        if (Length_ == 0) {
            Promise_.Set(TReadResponse{});
            return;
        }

        auto blockSize = Owner_->Geometry_.BlockSize;
        ValidateBlockRequest(Offset_, Length_, Owner_->Geometry_);

        int firstBlockIndex = Offset_ / blockSize;
        int lastBlockIndex = (Offset_ + Length_ - 1) / blockSize;
        int blockCount = lastBlockIndex - firstBlockIndex + 1;
        Payloads_.resize(blockCount);
        for (int index = 0; index < blockCount; ++index) {
            Payloads_[index] = ResolveBlock(firstBlockIndex + index, index);
        }

        if (Misses_.empty()) {
            Promise_.Set(MakeResponse());
            return;
        }

        std::vector<TStoredBlockId> blockIds;
        blockIds.reserve(Misses_.size());
        for (auto [position, blockId] : Misses_) {
            blockIds.push_back(blockId);
        }

        NChunkClient::TClientChunkReadOptions options{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive),
        };
        Owner_->BlockStore_->ReadBlocks(blockIds, options)
            .AsUnique()
            .Subscribe(
                BIND(&TReadSession::OnBlocksRead, MakeStrong(this))
                    .Via(Owner_->Invoker_));
    }

    //! Resolves a single block to its full payload, or returns null and records a cache miss.
    TSharedRef ResolveBlock(int blockIndex, int position)
    {
        // NB: A dirty block may be drained between reading the map and querying the pool; in
        // that case the map has already moved on (to clean or a newer dirty id), so re-read.
        // The flusher publishes the clean slot before evicting the drained block, so this
        // always converges.
        for (;;) {
            auto mappedBlockId = Owner_->BlockMap_->FindBlock(blockIndex);
            if (mappedBlockId == EmptyMappedBlockId) {
                return Owner_->EmptyBlock_;
            }
            if (IsDirtyMappedBlockId(mappedBlockId)) {
                if (auto block = Owner_->DirtyPool_->Find(ToDirtyBlockId(mappedBlockId), blockIndex)) {
                    return block->Payload;
                }
                continue;
            }

            auto storedBlockId = ToStoredBlockId(mappedBlockId);
            if (auto cachedBlock = Owner_->BlockCache_->Find(storedBlockId)) {
                return cachedBlock->GetPayload();
            }

            Misses_.emplace_back(position, storedBlockId);
            return {};
        }
    }

    struct TCachedBlockTag
    { };

    void OnBlocksRead(TErrorOr<std::vector<TSharedRef>>&& resultOrError)
    {
        if (!resultOrError.IsOK()) {
            auto error = TError("Block read failed") << resultOrError;
            Owner_->SetError(error);
            Promise_.Set(error);
            return;
        }

        auto payloads = std::move(resultOrError.Value());
        YT_VERIFY(std::ssize(payloads) == std::ssize(Misses_));
        for (int index = 0; index < std::ssize(Misses_); ++index) {
            auto [position, blockId] = Misses_[index];
            auto payload = TSharedRef::MakeCopy<TCachedBlockTag>(payloads[index]);
            if (auto cookie = Owner_->BlockCache_->BeginInsert(blockId); cookie.IsActive()) {
                cookie.EndInsert(New<TCleanBlock>(blockId, payload));
            }
            Payloads_[position] = std::move(payload);
        }

        Promise_.Set(MakeResponse());
    }

    struct TResponseDataTag
    { };

    TReadResponse MakeResponse()
    {
        return {
            .Data = MergeRefsToRef<TResponseDataTag>(Payloads_),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalBlockDevice::TWriteSession
    : public TRefCounted
{
public:
    TWriteSession(TJournalBlockDevicePtr owner, i64 offset, TSharedRef data)
        : Owner_(std::move(owner))
        , Offset_(offset)
        , Data_(std::move(data))
    { }

    TFuture<TWriteResponse> Run()
    {
        try {
            DoRun();
        } catch (const std::exception& ex) {
            Promise_.TrySet(TError(ex));
        }
        return Promise_;
    }

private:
    const TJournalBlockDevicePtr Owner_;
    const i64 Offset_;
    const TSharedRef Data_;

    const TPromise<TWriteResponse> Promise_ = NewPromise<TWriteResponse>();

    std::vector<TDirtyBlockPtr> Blocks_;
    int WrittenBlockCount_ = 0;

    struct TBlockTag
    { };

    void DoRun()
    {
        i64 length = std::ssize(Data_);
        if (length == 0) {
            Promise_.Set(TWriteResponse{});
            return;
        }

        ValidateBlockRequest(Offset_, length, Owner_->Geometry_);
        BuildBlocks(Offset_, Data_);
        PutMore();
    }

    //! Splits |data| (length a multiple of the block size) into dirty blocks starting at |offset|.
    void BuildBlocks(i64 offset, const TSharedRef& data)
    {
        auto blockSize = Owner_->Geometry_.BlockSize;
        int firstBlockIndex = offset / blockSize;
        int blockCount = std::ssize(data) / blockSize;
        Blocks_.reserve(blockCount);
        for (int index = 0; index < blockCount; ++index) {
            auto payload = TSharedRef::MakeCopy<TBlockTag>(data.Slice(index * blockSize, (index + 1) * blockSize));
            Blocks_.push_back(New<TDirtyBlock>(firstBlockIndex + index, std::move(payload)));
        }
    }

    void PutMore()
    {
        while (WrittenBlockCount_ < std::ssize(Blocks_)) {
            auto remainingBlocks = TRange(Blocks_).Slice(WrittenBlockCount_, Blocks_.size());
            auto future = Owner_->DirtyPool_->Put(remainingBlocks);
            auto result = future.TryGet();
            if (!result) {
                // The pool is full and the put is waiting for space; kick the flusher out of
                // band (rather than idling until the next periodic tick) and resume via a
                // subscription once space frees up.
                Owner_->BlockFlusher_->RequestFlush();
                future.Subscribe(BIND(&TWriteSession::OnPut, MakeStrong(this)));
                return;
            }
            if (!result->IsOK()) {
                Promise_.Set(TError(*result));
                return;
            }
            ApplyPut(result->Value());
        }
        Promise_.Set(TWriteResponse{});
    }

    void OnPut(const TErrorOr<std::vector<TDirtyBlockId>>& resultOrError)
    {
        if (!resultOrError.IsOK()) {
            Promise_.Set(TError(resultOrError));
            return;
        }

        ApplyPut(resultOrError.Value());
        PutMore();
    }

    void ApplyPut(const std::vector<TDirtyBlockId>& blockIds)
    {
        for (int index = 0; index < std::ssize(blockIds); ++index) {
            const auto& block = Blocks_[WrittenBlockCount_ + index];
            Owner_->BlockMap_->PutDirty(block->BlockIndex, blockIds[index]);
        }
        WrittenBlockCount_ += std::ssize(blockIds);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalBlockDevice::TSnapshotSession
    : public TRefCounted
{
public:
    explicit TSnapshotSession(TJournalBlockDevicePtr owner)
        : Owner_(std::move(owner))
    { }

    //! Runs on Owner_->Invoker_. Snapshots a consistent point-in-time cut of the block map and returns a
    //! stored location for every used block, blocking until each snapshotted dirty version is flushed.
    /*!
     *  Clean blocks resolve straight from the snapshot; dirty ones resolve to the location their flush
     *  yields. We subscribe to BlockFlushObserved before arming the scan and the map fires it strictly
     *  after updating the slot, so no flush the snapshot may reference is missed: a block captured dirty is
     *  either already in Flushed_ (not registered as pending) or still Pending_ and waited for.
     */
    std::vector<std::pair<int, TStoredBlockId>> Run()
    {
        auto onBlockFlushObserved = BIND(&TSnapshotSession::OnBlockFlushObserved, MakeStrong(this));
        auto onFlushFailed = BIND(&TSnapshotSession::OnFlushFailed, MakeStrong(this));
        Owner_->BlockMap_->SubscribeBlockFlushObserved(onBlockFlushObserved);
        Owner_->BlockFlusher_->SubscribeFailed(onFlushFailed);
        auto unsubscribeGuard = Finally([&] {
            Owner_->BlockMap_->UnsubscribeBlockFlushObserved(onBlockFlushObserved);
            Owner_->BlockFlusher_->UnsubscribeFailed(onFlushFailed);
        });

        auto snapshot = Owner_->BlockMap_->TakeSnapshot();

        // Register the snapshotted dirty versions not already flushed as pending; each one's flush
        // yields its stored location via OnBlockFlushObserved.
        {
            auto guard = Guard(Lock_);
            for (const auto& [blockIndex, mappedBlockId] : snapshot.Blocks) {
                if (IsDirtyMappedBlockId(mappedBlockId) &&
                    !Flushed_.contains(ToDirtyBlockId(mappedBlockId).Underlying()))
                {
                    Pending_.insert(ToDirtyBlockId(mappedBlockId).Underlying());
                }
            }
            if (Pending_.empty()) {
                AllFlushed_.TrySet();
            }
        }

        // The snapshotted dirty versions all sit below the pool's current tail, so an eager flush drains
        // them; wait until every one has been flushed.
        Owner_->BlockFlusher_->RequestFlushAll();
        // Uncancelable so the timeout cannot cancel AllFlushed_ out from under the flusher callbacks.
        WaitFor(AllFlushed_.ToFuture()
            .ToUncancelable()
            .WithTimeout(
                Owner_->Config_->BlockStore->SnapshotFlushTimeout,
                {.Error = TError("Timed out flushing the snapshot blocks")}))
            .ThrowOnError();

        // The snapshot is in ascending block index order, so the result is too.
        std::vector<std::pair<int, TStoredBlockId>> blocks;
        blocks.reserve(snapshot.Blocks.size());
        {
            auto guard = Guard(Lock_);
            for (const auto& [blockIndex, mappedBlockId] : snapshot.Blocks) {
                auto storedBlockId = IsStoredMappedBlockId(mappedBlockId)
                    ? ToStoredBlockId(mappedBlockId)
                    : GetOrCrash(Flushed_, ToDirtyBlockId(mappedBlockId).Underlying());
                blocks.emplace_back(blockIndex, storedBlockId);
            }
        }
        return blocks;
    }

private:
    const TJournalBlockDevicePtr Owner_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    //! Stored location each block flushed during this session landed at, keyed by its dirty id.
    THashMap<ui64, TStoredBlockId> Flushed_;
    //! Captured dirty versions not yet flushed; AllFlushed_ fires once this drains.
    THashSet<ui64> Pending_;
    const TPromise<void> AllFlushed_ = NewPromise<void>();

    void OnBlockFlushObserved(TDirtyBlockId dirtyBlockId, TStoredBlockId storedBlockId)
    {
        auto guard = Guard(Lock_);
        Flushed_[dirtyBlockId.Underlying()] = storedBlockId;
        if (Pending_.erase(dirtyBlockId.Underlying()) > 0 && Pending_.empty()) {
            AllFlushed_.TrySet();
        }
    }

    void OnFlushFailed(const TError& error)
    {
        AllFlushed_.TrySet(TError("Block flush failed while taking a snapshot") << error);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockDevice::DoSaveSnapshot(const TSnapshotSaveSpec& spec)
{
    BlockStore_->BeginSnapshot();
    auto endSnapshotGuard = Finally([&] {
        BlockStore_->EndSnapshot();
    });

    // Blocks come back in ascending index order, as the sorted snapshot table requires.
    auto blocks = New<TSnapshotSession>(MakeStrong(this))->Run();

    std::vector<TStoredBlockId> storedBlockIds;
    storedBlockIds.reserve(blocks.size());
    for (const auto& [blockIndex, storedBlockId] : blocks) {
        storedBlockIds.push_back(storedBlockId);
    }
    auto blockRefs = BlockStore_->GetBlockRefs(storedBlockIds);

    // Reference exactly the chunks the snapshot uses, not every chunk the store ever created -- an
    // empty chunk is unconfirmed and could never be sealed.
    std::vector<TSnapshotBlock> snapshotBlocks;
    snapshotBlocks.reserve(blocks.size());
    THashSet<NChunkClient::TChunkId> hunkChunkIdSet;
    for (int index = 0; index < std::ssize(blocks); ++index) {
        snapshotBlocks.push_back({
            .Index = blocks[index].first,
            .Ref = blockRefs[index],
        });
        hunkChunkIdSet.insert(blockRefs[index].ChunkId);
    }
    std::vector hunkChunkIds(hunkChunkIdSet.begin(), hunkChunkIdSet.end());

    // The snapshot table's hunk chunk list only accepts sealed chunks, so seal every chunk it is about
    // to reference (which retires those still being written into).
    WaitFor(BlockStore_->SealChunks(hunkChunkIds))
        .ThrowOnError();

    // The caller has created and resolved the table (|spec|) under its transaction; write the rows into
    // it under the same transaction (the caller commits it).
    WriteJournalSnapshot(Client_, spec, snapshotBlocks, hunkChunkIds, Logger);

    LatestSnapshotInfo_.Store(TSnapshotInfo{
        .Path = spec.GetPath(),
        .Timestamp = TInstant::Now(),
    });
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TReadResponse> TJournalBlockDevice::Read(
    i64 offset,
    i64 length,
    const TReadOptions& /*options*/)
{
    return New<TReadSession>(MakeStrong(this), offset, length)
        ->Run();
}

TFuture<TWriteResponse> TJournalBlockDevice::Write(
    i64 offset,
    const TSharedRef& data,
    const TWriteOptions& /*options*/)
{
    return New<TWriteSession>(MakeStrong(this), offset, data)
        ->Run();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalBlockDevicePtr CreateJournalBlockDevice(
    NNative::IClientPtr client,
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    TJournalBlockDeviceOptionsPtr storeOptions,
    TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    std::optional<TSnapshotLoadSpec> snapshotReadSpec,
    NLogging::TLogger logger)
{
    return New<TJournalBlockDevice>(
        std::move(deviceId),
        std::move(deviceConfig),
        std::move(storeOptions),
        transactionId,
        chunkListId,
        std::move(snapshotReadSpec),
        std::move(client),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

TSnapshotLoadSpec FetchSnapshotLoadSpec(
    const NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    const TFetchSnapshotLoadTableSpecOptions& options)
{
    return NTableClient::FetchSingleTableReadSpec(
        NYPath::TRichYPath(path),
        client,
        options);
}

void CreateSnapshotTable(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    std::optional<NObjectClient::TCellTag> externalCellTag,
    const TCreateSnapshotTableOptions& options)
{
    auto adjustedOptions = options;
    if (!adjustedOptions.Attributes) {
        adjustedOptions.Attributes = NYTree::CreateEphemeralAttributes();
    }
    adjustedOptions.Attributes->Set("schema", NRecords::TSnapshotBlockDescriptor::Get()->GetSchema());
    if (externalCellTag) {
        adjustedOptions.Attributes->Set("has_hunk_chunk_list", true);
        adjustedOptions.Attributes->Set("external_cell_tag", *externalCellTag);
    }
    WaitFor(client->CreateNode(path, EObjectType::Table, adjustedOptions))
        .ThrowOnError();
}

TSnapshotSaveSpec FetchSnapshotSaveSpec(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    const TFetchSnapshotSaveTableSpecOptions& options)
{
    // The transaction is carried by the user object (and reused by SaveSnapshot), so the fallback
    // transaction id below is left null.
    NChunkClient::TUserObject userObject(NYPath::TRichYPath(path), options.TransactionId);
    NChunkClient::GetUserObjectBasicAttributes(
        client,
        {&userObject},
        /*defaultTransactionId*/ {},
        Logger(),
        NYTree::EPermission::Write,
        options);
    return userObject;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
