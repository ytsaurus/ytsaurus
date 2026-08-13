#include "journal_block_device.h"

#include "block_compactor.h"
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

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/attribute_filter.h>
#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/variant.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

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
    i64 GetWeight(const TCleanBlockPtr& value) const final
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
        TChunkListId chunkListId,
        std::optional<TYPath> snapshotPath,
        NNative::IClientPtr client,
        NLogging::TLogger logger)
        : DeviceId_(std::move(deviceId))
        , Config_(std::move(config))
        , Options_(std::move(options))
        , ThreadPool_(CreateThreadPool(Config_->ThreadPoolSize, "JournalNbd"))
        , Invoker_(ThreadPool_->GetInvoker())
        , Logger(std::move(logger))
        , Client_(std::move(client))
        , SnapshotPath_(std::move(snapshotPath))
        , ExternalCellTag_(CellTagFromId(transactionId))
        , Geometry_(MakeGeometry(Options_->DeviceSize, Options_->BlockSize))
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
        , BlockCompactor_(Config_->BlockCompactor
            ? CreateBlockCompactor(
                Config_->BlockCompactor,
                BlockMap_,
                BlockStore_,
                Invoker_,
                Logger)
            : GetNullBlockCompactor())
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
        StopBackgroundActivities();

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

    bool IsTrimSupported() const final
    {
        return true;
    }

    TFuture<void> Trim(i64 offset, i64 length, const TTrimOptions& /*options*/) final
    {
        // A whole-device discard covers every block of the map, so keep it off the connection thread.
        return BIND(&TJournalBlockDevice::DoTrim, MakeStrong(this), offset, length)
            .AsyncVia(Invoker_)
            .Run();
    }

    TJournalBlockDeviceOptionsPtr GetOptions() const final
    {
        return Options_;
    }

    TCellTag GetExternalCellTag() const final
    {
        return ExternalCellTag_;
    }

    TFuture<TSnapshotSaveResult> SaveSnapshot(const TSnapshotSaveSpec& spec) final
    {
        return BIND(&TJournalBlockDevice::DoSaveSnapshot, MakeStrong(this), spec)
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> FlushBlocks() final
    {
        return BlockFlusher_->RequestFlushBarrier();
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
    //! If set, the path of the snapshot the device restores from on initialization.
    const std::optional<TYPath> SnapshotPath_;
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
    const IBlockCompactorPtr BlockCompactor_;

    struct TSnapshotInfo
    {
        TYPath Path;
        TInstant Timestamp;
    };

    //! The path and completion time of the most recent #SaveSnapshot. Written on Invoker_ (in
    //! #DoSaveSnapshot) and read from the orchid producer (on an arbitrary thread).
    NThreading::TAtomicObject<std::optional<TSnapshotInfo>> LatestSnapshotInfo_;

    void DoBuildOrchid(NYson::IYsonConsumer* consumer) const final
    {
        auto snapshotInfo = LatestSnapshotInfo_.Load();

        BuildYsonMapFragmentFluently(consumer)
            .Item("total_block_count").Value(Geometry_.BlockCount)
            .Item("used_block_count").Value(BlockMap_->GetUsedBlockCount())
            .Item("dirty_pool_size").Value(DirtyPool_->GetSize())
            .Item("dirty_pool_capacity").Value(DirtyPool_->GetCapacity())
            .Item("chunks").DoMapFor(BlockStore_->GetChunkInfos(), [] (TFluentMap fluent, const TChunkInfo& info) {
                fluent.Item(ToString(info.ChunkId)).BeginMap()
                    .Item("index").Value(info.ChunkIndex)
                    .Item("restored").Value(info.RestoredFromSnapshot)
                    .Item("seal_state").Value(info.SealState)
                    .Item("record_count").Value(info.RecordCount)
                    .Item("data_size").Value(info.DataSize)
                    .Item("referenced_block_count").Value(info.ReferencedBlockCount)
                    .Item("written_block_count").Value(info.WrittenBlockCount)
                    .Item("droppable").Value(info.Droppable)
                .EndMap();
            })
            // Whether the device was restored from a snapshot, and the latest one it has saved.
            .Item("restored_from_snapshot").Value(SnapshotPath_.has_value())
            .DoIf(snapshotInfo.has_value(), [&] (TFluentMap fluent) {
                fluent
                    .Item("latest_snapshot_path").Value(snapshotInfo->Path)
                    .Item("latest_snapshot_timestamp").Value(snapshotInfo->Timestamp);
            });
    }

    //! Once the device has failed, its chunks may be gone for good (an aborted staging transaction
    //! destroys them), so upkeep has nothing left to converge on and would retry indefinitely.
    void StopBackgroundActivities()
    {
        BlockCompactor_->Stop();
        BlockFlusher_->Stop();
        BlockStore_->Stop();
    }

    void OnDeviceFailed(const TError& error)
    {
        YT_LOG_ERROR(error, "Journal block device failed");

        StopBackgroundActivities();
    }

    void DoInitialize()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (SnapshotPath_) {
            RestoreFromSnapshot(*SnapshotPath_);
        }

        BlockFlusher_->SubscribeBlockFlushed(BIND(&TJournalBlockDevice::OnBlockFlushed, MakeWeak(this)));
        BlockStore_->SubscribeFailed(BIND(&TJournalBlockDevice::OnFailed, MakeWeak(this)));
        BlockMap_->SubscribeStoredBlockUnreferenced(BIND(&TJournalBlockDevice::OnStoredBlockUnreferenced, MakeWeak(this)));
        SubscribeError(BIND(&TJournalBlockDevice::OnDeviceFailed, MakeWeak(this)));

        BlockStore_->Start();
        BlockFlusher_->Start();
        BlockCompactor_->Start();

        YT_LOG_INFO("Journal block device initialized (BlockSize: %v, BlockCount: %v, DirtyPoolCapacity: %v)",
            Geometry_.BlockSize,
            Geometry_.BlockCount,
            DirtyPool_->GetCapacity());
    }

    //! Loads a previously saved snapshot.
    void RestoreFromSnapshot(const TYPath& path)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_INFO("Restoring journal block device from snapshot (Path: %v)", path);

        // Take a snapshot lock so the table (and the journal chunks it pins) cannot be removed while we
        // fetch its read spec, read its rows, and fetch its chunks' sizes.
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set(
            "title",
            Format("Restoring from NBD snapshot %v", path));
        TTransactionStartOptions transactionOptions;
        transactionOptions.Attributes = std::move(transactionAttributes);
        auto transaction = WaitFor(Client_->StartTransaction(
            NTransactionClient::ETransactionType::Master,
            transactionOptions))
            .ValueOrThrow();

        TLockNodeOptions lockOptions;
        lockOptions.TransactionId = transaction->GetId();
        auto lockResult = WaitFor(Client_->LockNode(path, NCypressClient::ELockMode::Snapshot, lockOptions))
            .ValueOrThrow();

        // Address the table by the locked node's id: the snapshot lock pins the node, not the path, which a
        // concurrent move or removal could otherwise redirect. Its path stays on the object for diagnostics.
        TUserObject userObject(TRichYPath(path), transaction->GetId());
        userObject.ObjectId = lockResult.NodeId;

        TFetchSingleTableReadSpecOptions loadSpecOptions;
        loadSpecOptions.TransactionId = transaction->GetId();

        auto readSpec = FetchSingleTableReadSpec(
            &userObject,
            Client_,
            loadSpecOptions);

        // A large device's block map runs to tens of gigabytes, so it is never held whole.
        auto reader = CreateSnapshotReader(Client_, userObject, readSpec, Geometry_, Invoker_, Logger);
        WaitFor(reader->Open())
            .ThrowOnError();

        WaitFor(BlockStore_->BeginRestoreBlocks())
            .ThrowOnError();
        BlockMap_->BeginLoadSnapshot();

        i64 blockCount = 0;
        TBlockMapSnapshot blockMapSnapshot;
        while (true) {
            auto blocks = WaitFor(reader->ReadBlocks())
                .ValueOrThrow();
            if (blocks.empty()) {
                break;
            }

            // Take the indexes before handing the batch over; the store consumes it.
            blockMapSnapshot.Blocks.clear();
            blockMapSnapshot.Blocks.reserve(blocks.size());
            for (const auto& block : blocks) {
                blockMapSnapshot.Blocks.emplace_back(block.Index, EmptyMappedBlockId);
            }
            blockCount += std::ssize(blocks);

            auto storedBlockIds = WaitFor(BlockStore_->RestoreBlocks(std::move(blocks)))
                .ValueOrThrow();
            YT_VERIFY(std::ssize(storedBlockIds) == std::ssize(blockMapSnapshot.Blocks));
            for (int index = 0; index < std::ssize(storedBlockIds); ++index) {
                blockMapSnapshot.Blocks[index].second = ToMappedBlockId(storedBlockIds[index]);
            }
            BlockMap_->LoadSnapshotPart(blockMapSnapshot);
        }

        BlockMap_->EndLoadSnapshot();

        auto chunkBlockCounts = WaitFor(reader->GetChunkBlockCounts())
            .ValueOrThrow();
        WaitFor(BlockStore_->EndRestoreBlocks(chunkBlockCounts))
            .ThrowOnError();

        YT_LOG_INFO("Journal block device restored from snapshot (BlockCount: %v)",
            blockCount);
    }

    //! Saves a crash-consistent point-in-time snapshot of the device to a Cypress table, concurrently
    //! with ongoing writes.
    //! Defined out of line, below TSnapshotSession, which it instantiates.
    TSnapshotSaveResult DoSaveSnapshot(const TSnapshotSaveSpec& spec);

    //! Empties the block map over the trimmed range, so the blocks read back as zeroes and their stored
    //! payloads become unreferenced -- which is what eventually lets a chunk go dead.
    /*!
     *  The clean-block cache is left as is: a discarded block's stored id is now unreachable from the
     *  map, and stored ids are never reused, so a stale entry can never be read back.
     */
    void DoTrim(i64 offset, i64 length)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto deviceSize = Geometry_.GetByteSize();
        THROW_ERROR_EXCEPTION_UNLESS(
            offset >= 0 && length >= 0 && offset <= deviceSize && length <= deviceSize - offset,
            "Trim request at offset %v of length %v is out of range for a device of size %v",
            offset,
            length,
            deviceSize);

        auto blockSize = Geometry_.BlockSize;
        // Only the blocks the range fully covers are discarded; a partially covered head or tail block
        // keeps its content, which is within a trim's latitude.
        int beginBlockIndex = (offset + blockSize - 1) / blockSize;
        int endBlockIndex = (offset + length) / blockSize;

        // Do not hog Invoker_ for seconds.
        auto yielder = CreatePeriodicYielder(TDuration::MilliSeconds(100));
        int discardedBlockCount = 0;
        for (int blockIndex = beginBlockIndex; blockIndex < endBlockIndex; ++blockIndex) {
            yielder.TryYield();
            if (BlockMap_->DiscardBlock(blockIndex)) {
                ++discardedBlockCount;
            }
        }

        YT_LOG_DEBUG("Blocks discarded (Offset: %v, Length: %v, BlockRange: [%v, %v), DiscardedBlockCount: %v)",
            offset,
            length,
            beginBlockIndex,
            endBlockIndex,
            discardedBlockCount);
    }

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

        auto blockCount = byteSize / blockSize;
        THROW_ERROR_EXCEPTION_UNLESS(
            blockCount < MaxBlocksPerDevice,
            "Journal device block count %v must be less than %v",
            blockCount,
            MaxBlocksPerDevice);

        return TBlockDeviceGeometry{
            .BlockSize = blockSize,
            .BlockCount = blockCount,
        };
    }

    void OnBlockFlushed(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId)
    {
        // Publish as clean only if no newer write superseded this block since it was drained;
        // otherwise the newer (dirty) version stays and the flushed copy is left orphaned.
        if (BlockMap_->TryPutBlock(block->BlockIndex, ToMappedBlockId(block->BlockId), storedBlockId)) {
            if (auto cookie = BlockCache_->BeginInsert(storedBlockId); cookie.IsActive()) {
                cookie.EndInsert(New<TCleanBlock>(storedBlockId, block->Payload));
            }
        }
    }

    void OnFailed(const TError& error)
    {
        SetError(error);
    }

    void OnStoredBlockUnreferenced(TStoredBlockId storedBlockId)
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

        Owner_->BlockStore_->ReadBlocks(blockIds, EWorkloadCategory::UserInteractive)
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
        YT_ASSERT_INVOKER_AFFINITY(Owner_->Invoker_);

        if (!resultOrError.IsOK()) {
            auto error = TError("Block read failed").With(resultOrError);
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
                Owner_->BlockFlusher_->RequestFlush(/*force*/ true);
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
            Owner_->BlockMap_->PutBlock(block->BlockIndex, blockIds[index]);
        }
        WrittenBlockCount_ += std::ssize(blockIds);

        Owner_->BlockFlusher_->RequestFlush();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Walks an open snapshot in parts of |partSize| blocks, handing each to |onPart|. Bounds what a caller
//! holds at once to one part, whatever the device's size.
template <class TOnPart>
void ScanSnapshot(const IBlockMapPtr& blockMap, int partSize, const TOnPart& onPart)
{
    YT_VERIFY(partSize > 0);

    int blockCount = blockMap->GetBlockCount();
    for (int begin = 0; begin < blockCount; begin += partSize) {
        onPart(blockMap->ScanSnapshotPart(begin, std::min(begin + partSize, blockCount)));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TJournalBlockDevice::TSnapshotSession
    : public TRefCounted
{
public:
    explicit TSnapshotSession(TJournalBlockDevicePtr owner)
        : Owner_(std::move(owner))
    { }

    //! Registers every dirty version the open snapshot captures and blocks until each is flushed,
    //! returning how many blocks the cut holds.
    /*!
     *  Clean blocks resolve straight from the cut; dirty ones resolve to the location their flush
     *  yields. We subscribe to BlockFlushObserved before arming the cut and the map fires it strictly
     *  after updating the slot, so no flush the snapshot may reference is missed: a block captured dirty is
     *  either already in Flushed_ (not registered as pending) or still Pending_ and waited for.
     *
     *  Leaves the cut armed for the caller to scan; ends it if this throws.
     */
    i64 Run()
    {
        YT_ASSERT_INVOKER_AFFINITY(Owner_->Invoker_);

        auto onBlockFlushObserved = BIND(&TSnapshotSession::OnBlockFlushObserved, MakeStrong(this));
        auto onFailed = BIND(&TSnapshotSession::OnFailed, MakeStrong(this));
        Owner_->BlockMap_->SubscribeBlockFlushObserved(onBlockFlushObserved);
        Owner_->SubscribeError(onFailed);
        auto unsubscribeGuard = Finally([&] {
            Owner_->BlockMap_->UnsubscribeBlockFlushObserved(onBlockFlushObserved);
            Owner_->UnsubscribeError(onFailed);
        });

        // Register the captured dirty versions not already flushed as pending; each one's flush yields
        // its stored location via OnBlockFlushObserved. Retains nothing per block: the caller re-scans
        // the same cut to emit the rows once the flushes have landed.
        Owner_->BlockMap_->BeginSnapshot();
        auto endSnapshotGuard = Finally([&] {
            Owner_->BlockMap_->EndSnapshot();
        });

        i64 blockCount = 0;
        // Bounded by the dirty pool's capacity, unlike the cut itself.
        std::vector<ui64> dirtyBlockIds;
        ScanSnapshot(Owner_->BlockMap_, Owner_->Config_->SnapshotBlocksPerBatch, [&] (const TBlockMapSnapshot& part) {
            blockCount += std::ssize(part.Blocks);
            for (auto [blockIndex, mappedBlockId] : part.Blocks) {
                if (IsDirtyMappedBlockId(mappedBlockId)) {
                    dirtyBlockIds.push_back(ToDirtyBlockId(mappedBlockId).Underlying());
                }
            }
        });

        {
            auto guard = Guard(Lock_);
            for (auto dirtyBlockId : dirtyBlockIds) {
                if (!Flushed_.contains(dirtyBlockId)) {
                    Pending_.insert(dirtyBlockId);
                }
            }
            if (Pending_.empty()) {
                AllFlushedPromise_.TrySet();
            }
        }

        // The snapshotted dirty versions all sit below the pool's current tail, so an eager flush drains
        // them; wait until every one has been flushed.
        auto flushBarrierFuture = Owner_->BlockFlusher_->RequestFlushBarrier();

        // Neither the timeout nor a failed barrier may cancel AllFlushedPromise_ out from under the flusher
        // callbacks.
        WaitFor(AllSucceeded(
            std::vector{flushBarrierFuture, AllFlushedPromise_.ToFuture()},
            {.PropagateCancelationToInput = false, .CancelInputOnShortcut = false})
            .WithTimeout(
                Owner_->Config_->BlockStore->SnapshotFlushTimeout,
                {.Error = TError("Timed out flushing the snapshot blocks")}))
            .ThrowOnError();

        endSnapshotGuard.Release();
        return blockCount;
    }

    //! Resolves a snapshotted mapped id to where its content ended up. Only after #Run has returned,
    //! by which point every snapshotted dirty version has been flushed.
    TStoredBlockId ResolveStoredBlockId(TMappedBlockId mappedBlockId)
    {
        if (IsStoredMappedBlockId(mappedBlockId)) {
            return ToStoredBlockId(mappedBlockId);
        }
        auto guard = Guard(Lock_);
        return GetOrCrash(Flushed_, ToDirtyBlockId(mappedBlockId).Underlying());
    }

private:
    const TJournalBlockDevicePtr Owner_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    //! Stored location each block flushed during this session landed at, keyed by its dirty id.
    THashMap<ui64, TStoredBlockId> Flushed_;
    //! Captured dirty versions not yet flushed; AllFlushedPromise_ fires once this drains.
    THashSet<ui64> Pending_;
    const TPromise<void> AllFlushedPromise_ = NewPromise<void>();

    void OnBlockFlushObserved(TDirtyBlockId dirtyBlockId, TStoredBlockId storedBlockId)
    {
        auto guard = Guard(Lock_);
        Flushed_[dirtyBlockId.Underlying()] = storedBlockId;
        if (Pending_.erase(dirtyBlockId.Underlying()) > 0 && Pending_.empty()) {
            AllFlushedPromise_.TrySet();
        }
    }

    void OnFailed(const TError& error)
    {
        AllFlushedPromise_.TrySet(TError("Device failed while taking a snapshot").With(error));
    }
};

////////////////////////////////////////////////////////////////////////////////

IJournalBlockDevice::TSnapshotSaveResult TJournalBlockDevice::DoSaveSnapshot(const TSnapshotSaveSpec& spec)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    BlockStore_->BeginSnapshot();
    auto endStoreSnapshotGuard = Finally([&] {
        BlockStore_->EndSnapshot();
    });

    auto session = New<TSnapshotSession>(MakeStrong(this));
    auto blockCount = session->Run();

    // Run leaves the cut armed so that the second pass below reads it too.
    auto endMapSnapshotGuard = Finally([&] {
        BlockMap_->EndSnapshot();
    });

    // Reference exactly the chunks the snapshot uses, not every chunk the store ever created -- an
    // empty chunk is unconfirmed and could never be sealed.
    std::vector<TChunkId> hunkChunkIds;
    if (blockCount > 0) {
        // The caller has created and resolved the table (|spec|) under its transaction; write the rows
        // into it under the same transaction (the caller commits it).
        auto writer = CreateSnapshotWriter(Client_, spec, Logger);
        WaitFor(writer->Open())
            .ThrowOnError();

        // Emit the rows a batch at a time, re-scanning the cut rather than holding it in memory: a
        // materialized cut costs O(block count), which a large device makes prohibitive. Blocks come
        // back in ascending index order, as the sorted snapshot table requires.
        std::vector<TStoredBlockId> storedBlockIds;
        std::vector<TSnapshotBlock> snapshotBlocks;
        i64 writtenBlockCount = 0;

        ScanSnapshot(BlockMap_, Config_->SnapshotBlocksPerBatch, [&] (const TBlockMapSnapshot& part) {
            if (part.Blocks.empty()) {
                return;
            }

            storedBlockIds.clear();
            for (auto [blockIndex, mappedBlockId] : part.Blocks) {
                storedBlockIds.push_back(session->ResolveStoredBlockId(mappedBlockId));
            }
            auto blockRefs = BlockStore_->GetBlockRefs(storedBlockIds);

            snapshotBlocks.clear();
            for (int index = 0; index < std::ssize(part.Blocks); ++index) {
                snapshotBlocks.push_back({
                    .Index = part.Blocks[index].first,
                    .Ref = blockRefs[index],
                });
            }
            WaitFor(writer->WriteBlocks(snapshotBlocks))
                .ThrowOnError();

            writtenBlockCount += std::ssize(part.Blocks);
        });

        endMapSnapshotGuard.Release();
        BlockMap_->EndSnapshot();

        // Both scans see the same cut, so the rows written must match what the first pass counted.
        if (writtenBlockCount != blockCount) {
            YT_LOG_ALERT_AND_THROW("Snapshot wrote an unexpected number of blocks (Written: %v, Expected: %v)",
                writtenBlockCount,
                blockCount);
        }

        hunkChunkIds = writer->GetReferencedChunkIds();

        // The snapshot table's hunk chunk list only accepts sealed chunks, so seal every chunk it is
        // about to reference (which retires those still being written into).
        WaitFor(BlockStore_->SealChunks(hunkChunkIds))
            .ThrowOnError();

        WaitFor(writer->Close())
            .ThrowOnError();
    }

    LatestSnapshotInfo_.Store(TSnapshotInfo{
        .Path = spec.GetPath(),
        .Timestamp = TInstant::Now(),
    });

    return {
        .BlockCount = blockCount,
        .ChunkCount = static_cast<int>(hunkChunkIds.size()),
    };
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

namespace {

//! Reconstructs a restored device's options from the snapshot table: its geometry from @device_params
//! and the rest from the table's own account and primary medium.
TJournalBlockDeviceOptionsPtr FetchDeviceOptions(
    const NNative::IClientPtr& client,
    const TYPath& snapshotPath,
    const NLogging::TLogger& Logger)
{
    // TODO(babenko): fetch this under the same transaction that loads the snapshot.
    YT_LOG_INFO("Fetching device params from snapshot table (SnapshotPath: %v)",
        snapshotPath);

    TGetNodeOptions options;
    options.Attributes = {
        "type",
        "device_params",
        "account",
        "primary_medium",
    };
    auto nodeYson = WaitFor(client->GetNode(snapshotPath, options))
        .ValueOrThrow();
    auto node = ConvertToNode(nodeYson);
    const auto& attributes = node->Attributes();

    auto type = attributes.Get<EObjectType>("type");
    THROW_ERROR_EXCEPTION_UNLESS(type == EObjectType::Table,
        "Invalid snapshot %v type: expected %Qlv, got %Qlv",
        snapshotPath,
        EObjectType::Table,
        type);

    auto params = attributes.Get<TSerializableDeviceParams>("device_params");

    auto deviceOptions = New<TJournalBlockDeviceOptions>();
    deviceOptions->DeviceSize = params.DeviceSize;
    deviceOptions->BlockSize = params.BlockSize;
    deviceOptions->Account = attributes.Get<std::string>("account");
    deviceOptions->MediumName = attributes.Get<std::string>("primary_medium");
    deviceOptions->Postprocess();
    return deviceOptions;
}

IJournalBlockDevicePtr DoCreateJournalBlockDevice(
    const NNative::IClientPtr& client,
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    const TDeviceCreationDescriptor& creationDescriptor,
    TTransactionId transactionId,
    TChunkListId chunkListId,
    NLogging::TLogger logger)
{
    std::optional<TYPath> snapshotPath;
    TJournalBlockDeviceOptionsPtr deviceOptions;
    Visit(creationDescriptor,
        [&] (const TFreshDeviceCreationDescriptor& freshDescriptor) {
            deviceOptions = freshDescriptor.Options;
        },
        [&] (const TRestoredDeviceCreationDescriptor& restoredDescriptor) {
            // A restored device does not carry options: they are reconstructed from the snapshot table's
            // persisted params, so its geometry matches the device the snapshot was taken from.
            snapshotPath = restoredDescriptor.SnapshotPath;
            deviceOptions = FetchDeviceOptions(client, restoredDescriptor.SnapshotPath, logger);
        });

    return New<TJournalBlockDevice>(
        std::move(deviceId),
        std::move(deviceConfig),
        std::move(deviceOptions),
        transactionId,
        chunkListId,
        std::move(snapshotPath),
        client,
        std::move(logger));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<IJournalBlockDevicePtr> CreateJournalBlockDevice(
    NNative::IClientPtr client,
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    TDeviceCreationDescriptor creationDescriptor,
    TTransactionId transactionId,
    TChunkListId chunkListId,
    NLogging::TLogger logger)
{
    const auto& invoker = client->GetConnection()->GetInvoker();
    return BIND(
        &DoCreateJournalBlockDevice,
        std::move(client),
        std::move(deviceId),
        std::move(deviceConfig),
        std::move(creationDescriptor),
        transactionId,
        chunkListId,
        std::move(logger))
        .AsyncVia(invoker)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoCreateSnapshotTable(
    const IClientPtr& client,
    const TYPath& path,
    std::optional<TCellTag> externalCellTag,
    const TJournalBlockDeviceOptionsPtr& deviceOptions,
    const TCreateSnapshotTableOptions& options)
{
    TSerializableDeviceParams deviceParams;
    deviceParams.DeviceSize = deviceOptions->DeviceSize;
    deviceParams.BlockSize = deviceOptions->BlockSize;

    auto adjustedOptions = options;
    if (!adjustedOptions.Attributes) {
        adjustedOptions.Attributes = CreateEphemeralAttributes();
    }
    adjustedOptions.Attributes->Set("schema", NRecords::TSnapshotBlockDescriptor::Get()->GetSchema());
    adjustedOptions.Attributes->Set("primary_medium", deviceOptions->MediumName);
    adjustedOptions.Attributes->Set("account", deviceOptions->Account);
    adjustedOptions.Attributes->Set("device_params", deviceParams);
    if (externalCellTag) {
        adjustedOptions.Attributes->Set("has_hunk_chunk_list", true);
        adjustedOptions.Attributes->Set("external_cell_tag", *externalCellTag);
    }
    WaitFor(client->CreateNode(path, EObjectType::Table, adjustedOptions))
        .ThrowOnError();
}

TSnapshotSaveSpec DoFetchSnapshotSaveSpec(
    const NNative::IClientPtr& client,
    const TYPath& path,
    const TFetchSnapshotSaveTableSpecOptions& options)
{
    // The transaction is carried by the user object (and reused by SaveSnapshot), so the fallback
    // transaction id below is left null.
    TUserObject userObject(TRichYPath(path), options.TransactionId);
    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        /*defaultTransactionId*/ {},
        Logger(),
        EPermission::Write,
        options);
    return userObject;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<void> CreateSnapshotTable(
    const NApi::IClientPtr& client,
    const TYPath& path,
    std::optional<NObjectClient::TCellTag> externalCellTag,
    const TJournalBlockDeviceOptionsPtr& deviceOptions,
    const TCreateSnapshotTableOptions& options)
{
    return BIND(&DoCreateSnapshotTable, client, path, externalCellTag, deviceOptions, options)
        .AsyncVia(client->GetConnection()->GetInvoker())
        .Run();
}

TFuture<TSnapshotSaveSpec> FetchSnapshotSaveSpec(
    const NApi::NNative::IClientPtr& client,
    const TYPath& path,
    const TFetchSnapshotSaveTableSpecOptions& options)
{
    return BIND(&DoFetchSnapshotSaveSpec, client, path, options)
        .AsyncVia(client->GetConnection()->GetInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
