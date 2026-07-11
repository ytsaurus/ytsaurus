#include "journal_block_device.h"

#include "block_flusher.h"
#include "block_map.h"
#include "block_store.h"
#include "config.h"
#include "dirty_block_pool.h"

#include <yt/yt/server/lib/nbd/block_device_detail.h>
#include <yt/yt/server/lib/nbd/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

//! A clean (flushed) block cached by its stored block id.
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
{
public:
    TJournalBlockDevice(
        std::string deviceId,
        TJournalBlockDeviceConfigPtr config,
        TJournalBlockDeviceOptionsPtr options,
        TTransactionId transactionId,
        NChunkClient::TChunkListId chunkListId,
        NNative::IClientPtr client,
        NLogging::TLogger logger)
        : DeviceId_(std::move(deviceId))
        , Config_(std::move(config))
        , Options_(std::move(options))
        , ThreadPool_(CreateThreadPool(Config_->ThreadPoolSize, "JournalNbd"))
        , Invoker_(ThreadPool_->GetInvoker())
        , Logger(std::move(logger))
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
            std::move(client),
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
        return Format("Journal{Size: %v, BlockSize: %v}",
            Geometry_.GetByteSize(),
            Geometry_.BlockSize);
    }

    std::string GetProfileSensorTag() const final
    {
        return DeviceId_;
    }

    TFuture<void> Initialize() final
    {
        BlockFlusher_->SubscribeBlockFlushed(BIND(&TJournalBlockDevice::OnBlockFlushed, MakeWeak(this)));
        BlockFlusher_->SubscribeFailed(BIND(&TJournalBlockDevice::OnFlushFailed, MakeWeak(this)));
        BlockFlusher_->Start();

        YT_LOG_INFO("Journal block device initialized (BlockSize: %v, BlockCount: %v, DirtyPoolCapacity: %v)",
            Geometry_.BlockSize,
            Geometry_.BlockCount,
            DirtyPool_->GetCapacity());

        return OKFuture;
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

    TFuture<void> Flush() final
    {
        return OKFuture;
    }

private:
    class TReadSession;
    class TWriteSession;

    const std::string DeviceId_;
    const TJournalBlockDeviceConfigPtr Config_;
    const TJournalBlockDeviceOptionsPtr Options_;
    const NConcurrency::IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    const TBlockDeviceGeometry Geometry_;
    const IBlockMapPtr BlockMap_;
    const IDirtyBlockPoolPtr DirtyPool_;
    const TBlockCachePtr BlockCache_;
    const TSharedRef EmptyBlock_;
    const IBlockStorePtr BlockStore_;
    const IBlockFlusherPtr BlockFlusher_;

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
            auto state = Owner_->BlockMap_->Find(blockIndex);
            if (std::holds_alternative<TEmptyBlock>(state)) {
                return Owner_->EmptyBlock_;
            }
            if (auto* dirtyBlockId = std::get_if<TDirtyBlockId>(&state)) {
                if (auto block = Owner_->DirtyPool_->Find(*dirtyBlockId, blockIndex)) {
                    return block->Payload;
                }
                continue;
            }

            auto storedBlockId = std::get<TStoredBlockId>(state);
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

IBlockDevicePtr CreateJournalBlockDevice(
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    TJournalBlockDeviceOptionsPtr storeOptions,
    TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    NNative::IClientPtr client,
    NLogging::TLogger logger)
{
    return New<TJournalBlockDevice>(
        std::move(deviceId),
        std::move(deviceConfig),
        std::move(storeOptions),
        transactionId,
        chunkListId,
        std::move(client),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
