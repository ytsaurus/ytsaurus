#include "block_store.h"
#include "config.h"
#include "block_store_helpers.h"

#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <util/random/random.h>

#include <algorithm>
#include <cstring>
#include <utility>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NJournalClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBlockStore)

class TBlockStore
    : public IBlockStore
{
public:
    TBlockStore(
        TJournalBlockStoreConfigPtr config,
        TBlockDeviceGeometry geometry,
        TJournalBlockDeviceOptionsPtr options,
        NNative::IClientPtr client,
        TTransactionId transactionId,
        TChunkListId chunkListId,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , Geometry_(geometry)
        , Options_(std::move(options))
        , Client_(std::move(client))
        , TransactionId_(transactionId)
        , ChunkListId_(chunkListId)
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
        , FragmentReader_(CreateChunkFragmentReader(
            Config_->ChunkReader,
            New<TChunkReaderHost>(Client_),
            /*profiler*/ {}))
        , MaintenanceExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TBlockStore::OnMaintenanceTick, MakeWeak(this)),
            Config_->ChunkMaintenancePeriod))
        , ChunkCreationBackoff_(Config_->ChunkCreationBackoff)
    { }

    void InitializeRefCounted()
    {
        MaintenanceExecutor_->Start();
        MaintenanceExecutor_->ScheduleOutOfBand();
    }

    TFuture<std::vector<TStoredBlockId>> WriteBlocks(TRange<TSharedRef> blocks) final
    {
        {
            auto guard = Guard(WriteLock_);
            if (!FatalWriteError_.IsOK()) {
                return MakeFuture<std::vector<TStoredBlockId>>(FatalWriteError_);
            }
        }

        int blockCount = std::ssize(blocks);
        if (blockCount == 0) {
            return MakeFuture(std::vector<TStoredBlockId>{});
        }

        // Coalesce the blocks into records of up to MaxBlocksPerRecord and submit each as its
        // own write session; the records are written concurrently.
        auto blockSize = Geometry_.BlockSize;
        std::vector<TFuture<std::vector<TStoredBlockId>>> recordFutures;
        for (int start = 0; start < blockCount; start += MaxBlocksPerRecord) {
            int perRecordBlockCount = std::min<int>(MaxBlocksPerRecord, blockCount - start);
            struct TRecordTag
            { };
            auto buffer = TSharedMutableRef::Allocate<TRecordTag>(
                blockSize * perRecordBlockCount,
                {.InitializeStorage = false});
            for (int index = 0; index < perRecordBlockCount; ++index) {
                const auto& block = blocks[start + index];
                if (std::ssize(block) != blockSize) {
                    return MakeFuture<std::vector<TStoredBlockId>>(TError(
                        "Invalid block size: expected %v, got %v",
                        blockSize,
                        std::ssize(block)));
                }
                std::memcpy(buffer.Begin() + index * blockSize, block.Begin(), blockSize);
            }

            auto session = New<TWriteSession>(this, std::move(buffer), perRecordBlockCount);
            recordFutures.push_back(session->Run());
        }

        return AllSucceeded(std::move(recordFutures))
            .AsUnique()
            .Apply(BIND([] (std::vector<std::vector<TStoredBlockId>>&& perRecordBlockIds) {
                // Common case: a single record; hand its ids back without copying.
                if (perRecordBlockIds.size() == 1) {
                    return std::move(perRecordBlockIds[0]);
                }
                std::vector<TStoredBlockId> result;
                for (auto& blockIds : perRecordBlockIds) {
                    result.insert(result.end(), blockIds.begin(), blockIds.end());
                }
                return result;
            }));
    }

    TFuture<std::vector<TSharedRef>> ReadBlocks(
        TRange<TStoredBlockId> blockIds,
        const TClientChunkReadOptions& options) final
    {
        auto blockSize = Geometry_.BlockSize;

        std::vector<IChunkFragmentReader::TChunkFragmentRequest> requests;
        requests.reserve(blockIds.size());
        {
            auto guard = ReaderGuard(IndexToChunkLock_);
            for (auto blockId : blockIds) {
                auto parsedBlockId = ParseStoredBlockId(blockId);
                if (parsedBlockId.ChunkIndex < 0 || parsedBlockId.ChunkIndex >= std::ssize(IndexToChunk_)) {
                    return MakeFuture<std::vector<TSharedRef>>(TError(
                        "Invalid stored block id: chunk index %v is out of range [0, %v)",
                        parsedBlockId.ChunkIndex,
                        std::ssize(IndexToChunk_)));
                }

                const auto& chunk = IndexToChunk_[parsedBlockId.ChunkIndex];
                requests.push_back({
                    .ChunkId = chunk->ChunkId,
                    .Length = blockSize,
                    .BlockIndex = parsedBlockId.RecordIndex,
                    .BlockOffset = parsedBlockId.FragmentIndex * blockSize,
                });
            }
        }

        return FragmentReader_->ReadFragments(std::move(requests), options)
            .AsUnique()
            .Apply(BIND([] (IChunkFragmentReader::TReadFragmentsResponse&& response) {
                return std::move(response.Fragments);
            }));
    }

private:
    const TJournalBlockStoreConfigPtr Config_;
    const TBlockDeviceGeometry Geometry_;
    const TJournalBlockDeviceOptionsPtr Options_;
    const NNative::IClientPtr Client_;
    const TTransactionId TransactionId_;
    const TChunkListId ChunkListId_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    const IChunkFragmentReaderPtr FragmentReader_;
    const TPeriodicExecutorPtr MaintenanceExecutor_;

    struct TChunkEntry final
    {
        TChunkEntry(
            int index,
            TChunkId chunkId,
            IJournalChunkWriterPtr writer)
            : Index(index)
            , ChunkId(chunkId)
            , Writer(std::move(writer))
        { }

        const int Index;
        const TChunkId ChunkId;

        //! Released once the chunk leaves the writable set.
        IJournalChunkWriterPtr Writer;

        i64 RecordCount = 0;
        i64 DataSize = 0;
    };

    using TChunkEntryPtr = TIntrusivePtr<TChunkEntry>;

    struct TWriteHandle
    {
        TChunkId ChunkId;
        int ChunkIndex;
        TFuture<i64> Future;
    };

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, IndexToChunkLock_);
    //! All chunks ever created, indexed by chunk index; append-only, kept around for reads.
    std::vector<TChunkEntryPtr> IndexToChunk_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, WriteLock_);
    //! Chunks currently accepting writes; retired when full or on writer failure.
    std::vector<TChunkEntryPtr> WritableChunks_;
    TError FatalWriteError_;

    //! Bounds and paces chunk-creation retries. Reset after each successful creation.
    TBackoffStrategy ChunkCreationBackoff_;
    TInstant ChunkCreationRetryDeadline_;

    class TWriteSession
        : public TRefCounted
    {
    public:
        TWriteSession(TBlockStore* owner, TSharedRef buffer, int blockCount)
            : Owner_(MakeWeak(owner))
            , Buffer_(std::move(buffer))
            , BlockCount_(blockCount)
            , Logger(owner->Logger.WithTag("WriteSessionId: %v", TGuid::Create()))
            , BackoffStrategy_(owner->Config_->WriteBackoff)
        { }

        TFuture<std::vector<TStoredBlockId>> Run()
        {
            YT_LOG_DEBUG("Write session started (BlockCount: %v)",
                BlockCount_);
            TryWrite();
            return Promise_.ToFuture().ToUncancelable();
        }

    private:
        const TWeakPtr<TBlockStore> Owner_;
        const TSharedRef Buffer_;
        const int BlockCount_;
        const NLogging::TLogger Logger;

        const TPromise<std::vector<TStoredBlockId>> Promise_ = NewPromise<std::vector<TStoredBlockId>>();

        TBackoffStrategy BackoffStrategy_;
        std::vector<TError> InnerErrors_;

        TBlockStorePtr TryLockOwner()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                Promise_.Set(TError(NYT::EErrorCode::Canceled, "Block store is destroyed"));
                return nullptr;
            }
            return owner;
        }

        void TryWrite()
        {
            try {
                auto owner = TryLockOwner();
                if (!owner) {
                    return;
                }

                auto handle = owner->TryDispatchWrite(Buffer_);
                if (!handle) {
                    // No writer available yet; wait for the refill executor.
                    YT_LOG_DEBUG("No writable chunk is available, will retry");
                    InnerErrors_.push_back(TError("No writable chunk is available"));
                    ScheduleRetry(owner);
                    return;
                }

                YT_LOG_DEBUG("Writing record (ChunkId: %v)", handle->ChunkId);
                handle->Future.Subscribe(BIND(
                    &TWriteSession::OnWritten,
                    MakeStrong(this),
                    handle->ChunkId,
                    handle->ChunkIndex));
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                YT_LOG_DEBUG(error, "Write session failed");
                Promise_.Set(std::move(error));
            }
        }

        void OnWritten(TChunkId chunkId, int chunkIndex, const TErrorOr<i64>& recordIndexOrError)
        {
            if (!recordIndexOrError.IsOK()) {
                YT_LOG_DEBUG(recordIndexOrError, "Failed to write record (ChunkId: %v)",
                    chunkId);

                auto owner = TryLockOwner();
                if (!owner) {
                    return;
                }

                InnerErrors_.push_back(recordIndexOrError);
                owner->DropChunk(chunkId, recordIndexOrError);
                ScheduleRetry(owner);
                return;
            }

            auto recordIndex = static_cast<int>(recordIndexOrError.Value());
            std::vector<TStoredBlockId> blockIds(BlockCount_);
            for (int fragmentIndex = 0; fragmentIndex < BlockCount_; ++fragmentIndex) {
                blockIds[fragmentIndex] = MakeStoredBlockId({
                    .ChunkIndex = chunkIndex,
                    .RecordIndex = recordIndex,
                    .FragmentIndex = fragmentIndex,
                });
            }

            YT_LOG_DEBUG("Write session succeeded (ChunkId: %v, RecordIndex: %v)",
                chunkId,
                recordIndex);
            Promise_.Set(std::move(blockIds));
        }

        void ScheduleRetry(const TBlockStorePtr& owner)
        {
            if (!BackoffStrategy_.Next()) {
                YT_LOG_WARNING("Write session failed, out of retries (AttemptCount: %v)",
                    BackoffStrategy_.GetInvocationCount());
                Promise_.Set(TError("Failed to write to block store")
                    << std::move(InnerErrors_));
                return;
            }

            TDelayedExecutor::Submit(
                BIND(&TWriteSession::OnRetryDeadline, MakeStrong(this)),
                BackoffStrategy_.GetBackoff(),
                owner->Invoker_);
        }

        void OnRetryDeadline(bool aborted)
        {
            if (aborted) {
                return;
            }
            TryWrite();
        }
    };

    std::optional<TWriteHandle> TryDispatchWrite(const TSharedRef& record)
    {
        TChunkEntryPtr evictedChunk;
        std::optional<TWriteHandle> result;
        {
            auto guard = Guard(WriteLock_);

            if (!FatalWriteError_.IsOK()) {
                THROW_ERROR FatalWriteError_;
            }

            if (WritableChunks_.empty()) {
                return std::nullopt;
            }

            auto index = RandomNumber<size_t>(WritableChunks_.size());
            const auto& chunk = WritableChunks_[index];

            chunk->RecordCount += 1;
            chunk->DataSize += std::ssize(record);

            result = TWriteHandle{
                .ChunkId = chunk->ChunkId,
                .ChunkIndex = chunk->Index,
                .Future = chunk->Writer->WriteRecord(record),
            };

            // Retire the chunk once its record index space is used up.
            if (chunk->RecordCount >= MaxRecordsPerChunk) {
                evictedChunk = chunk;
                std::swap(WritableChunks_[index], WritableChunks_.back());
                WritableChunks_.pop_back();
            }
        }

        if (evictedChunk) {
            YT_LOG_INFO("Block store chunk reached record count limit (ChunkId: %v, RecordCount: %v)",
                evictedChunk->ChunkId,
                evictedChunk->RecordCount);
            ReleaseWriter(evictedChunk);
        }

        return result;
    }

    void OnMaintenanceTick()
    {
        RetireOversizedChunks();
        RefillWritableChunks();
    }

    void RetireOversizedChunks()
    {
        std::vector<TChunkEntryPtr> chunksToRetire;
        {
            auto guard = Guard(WriteLock_);
            int index = 0;
            while (index < std::ssize(WritableChunks_)) {
                if (WritableChunks_[index]->DataSize >= Config_->MaxChunkDataSize) {
                    chunksToRetire.push_back(WritableChunks_[index]);
                    std::swap(WritableChunks_[index], WritableChunks_.back());
                    WritableChunks_.pop_back();
                } else {
                    ++index;
                }
            }
        }

        for (const auto& chunk : chunksToRetire) {
            YT_LOG_INFO("Block store chunk reached data size limit (ChunkId: %v, DataSize: %v)",
                chunk->ChunkId,
                chunk->DataSize);
            ReleaseWriter(chunk);
        }
    }

    bool HasNoWritableChunks()
    {
        auto guard = Guard(WriteLock_);
        return WritableChunks_.empty();
    }

    void RefillWritableChunks()
    {
        while (true) {
            {
                auto guard = Guard(WriteLock_);
                if (!FatalWriteError_.IsOK()) {
                    return;
                }
                if (std::ssize(WritableChunks_) >= Config_->WriteParallelism) {
                    break;
                }
            }

            // Hold off until the backoff from a recent creation failure has elapsed.
            if (TInstant::Now() < ChunkCreationRetryDeadline_) {
                break;
            }

            try {
                CreateChunk();
                ChunkCreationBackoff_.Restart();
            } catch (const std::exception& ex) {
                // Out of retries and no writable chunk left: give up so writes fail fast instead of
                // blocking forever on a chunk that will never appear. While some writable chunk
                // remains it can still serve writes, so keep retrying (capped at the max backoff).
                if (!ChunkCreationBackoff_.Next() && HasNoWritableChunks()) {
                    auto error = TError("Failed to create block store chunk, out of retries")
                        << ex;
                    YT_LOG_ERROR(error);
                    {
                        auto guard = Guard(WriteLock_);
                        FatalWriteError_ = error;
                    }
                    return;
                }

                auto backoff = ChunkCreationBackoff_.GetBackoff();
                YT_LOG_WARNING(ex, "Failed to create block store chunk, will retry (Backoff: %v)",
                    backoff);
                ChunkCreationRetryDeadline_ = TInstant::Now() + backoff;
                break;
            }
        }
    }

    void CreateChunk()
    {
        {
            auto guard = Guard(WriteLock_);
            if (!FatalWriteError_.IsOK()) {
                THROW_ERROR FatalWriteError_;
            }
        }

        int chunkCount;
        {
            auto guard = ReaderGuard(IndexToChunkLock_);
            chunkCount = std::ssize(IndexToChunk_);
        }
        if (chunkCount >= MaxChunksPerDevice) {
            auto error = TError("Block store exhausted its chunk index space");
            YT_LOG_ERROR(error);
            {
                auto guard = Guard(WriteLock_);
                FatalWriteError_ = error;
            }
            THROW_ERROR error;
        }

        const auto& options = Options_;
        YT_LOG_INFO("Creating block store chunk (Account: %v, MediumName: %v)",
            options->Account,
            options->MediumName);

        auto sessionId = CreateJournalChunk();

        YT_LOG_INFO("Block store chunk created (ChunkId: %v)",
            sessionId.ChunkId);

        auto writerOptions = New<TJournalChunkWriterOptions>();
        writerOptions->ReplicationFactor = Config_->ReplicationFactor;
        writerOptions->ReadQuorum = Config_->ReadQuorum;
        writerOptions->WriteQuorum = Config_->WriteQuorum;
        writerOptions->ErasureCodec = NErasure::ECodec::None;

        auto writer = CreateJournalChunkWriter(
            Client_,
            sessionId,
            std::move(writerOptions),
            Config_->ChunkWriter,
            /*counters*/ {},
            Invoker_,
            /*targets*/ std::nullopt,
            EChunkFormat::JournalDefault,
            Logger);

        WaitFor(writer->Open())
            .ThrowOnError();

        // The chunk index equals its position in IndexToChunk_ (serial appends). Publish the
        // chunk to the read map first, then to the writable set.
        TChunkEntryPtr chunk;
        {
            auto guard = WriterGuard(IndexToChunkLock_);
            int index = std::ssize(IndexToChunk_);
            chunk = New<TChunkEntry>(index, sessionId.ChunkId, std::move(writer));
            IndexToChunk_.push_back(chunk);
        }
        {
            auto guard = Guard(WriteLock_);
            WritableChunks_.push_back(chunk);
        }

        // Reactively drop the writer once it fails (single-shot; fires in situ if the
        // writer has already failed by the time we subscribe).
        chunk->Writer->SubscribeFailed(
            BIND(&TBlockStore::DropChunk, MakeWeak(this), chunk->ChunkId)
                .Via(Invoker_));

        YT_LOG_INFO("Block store chunk writer opened (ChunkId: %v, Index: %v)",
            sessionId.ChunkId,
            chunk->Index);
    }

    TSessionId CreateJournalChunk()
    {
        const auto& options = Options_;

        auto channel = Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(TransactionId_));
        TChunkServiceProxy proxy(std::move(channel));

        auto req = proxy.CreateChunk();
        GenerateMutationId(req);
        req->set_type(ToProto(EObjectType::JournalChunk));
        req->set_account(options->Account);
        ToProto(req->mutable_transaction_id(), TransactionId_);
        req->set_replication_factor(Config_->ReplicationFactor);
        req->set_erasure_codec(ToProto(NErasure::ECodec::None));
        req->set_medium_name(options->MediumName);
        req->set_read_quorum(Config_->ReadQuorum);
        req->set_write_quorum(Config_->WriteQuorum);
        req->set_movable(true);
        req->set_vital(true);
        // If a chunk list is given, the master attaches the new chunk to it atomically.
        if (ChunkListId_) {
            ToProto(req->mutable_chunk_list_id(), ChunkListId_);
        }

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        return FromProto<TSessionId>(rsp->session_id());
    }

    void DropChunk(TChunkId chunkId, const TError& error)
    {
        TChunkEntryPtr droppedChunk;
        {
            auto guard = Guard(WriteLock_);
            auto it = std::ranges::find_if(WritableChunks_, [&] (const auto& chunk) {
                return chunk->ChunkId == chunkId;
            });
            if (it != WritableChunks_.end()) {
                droppedChunk = *it;
                std::swap(*it, WritableChunks_.back());
                WritableChunks_.pop_back();
            }
        }

        if (droppedChunk) {
            YT_LOG_WARNING(error, "Dropped block store chunk writer (ChunkId: %v)",
                droppedChunk->ChunkId);
            ReleaseWriter(droppedChunk);
        }
    }

    void ReleaseWriter(const TChunkEntryPtr& chunk)
    {
        IJournalChunkWriterPtr writer;
        {
            auto guard = Guard(WriteLock_);
            writer = std::exchange(chunk->Writer, nullptr);
        }
        if (writer) {
            YT_UNUSED_FUTURE(writer->Close());
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockStore)

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateJournalBlockStore(
    TJournalBlockStoreConfigPtr config,
    TBlockDeviceGeometry geometry,
    TJournalBlockDeviceOptionsPtr options,
    NNative::IClientPtr client,
    TTransactionId transactionId,
    TChunkListId chunkListId,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TBlockStore>(
        std::move(config),
        geometry,
        std::move(options),
        std::move(client),
        transactionId,
        chunkListId,
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
