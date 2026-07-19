#include "block_store.h"
#include "config.h"
#include "block_store_helpers.h"

#include <yt/yt/ytlib/journal_client/helpers.h>
#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/journal_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/hash_set.h>

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
using namespace NTableClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBlockStore)

//! Stores each device block in a journal record as a block-with-header: a THunkPayloadHeader followed
//! by the block payload bytes.
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

        // Coalesce the blocks into records of up to MaxBlocksPerRecord, each written as its own session.
        auto blockSize = Geometry_.BlockSize;
        i64 blockWithHeaderSize = sizeof(THunkPayloadHeader) + blockSize;
        std::vector<TFuture<std::vector<TStoredBlockId>>> recordFutures;
        for (int start = 0; start < blockCount; start += MaxBlocksPerRecord) {
            int perRecordBlockCount = std::min<int>(MaxBlocksPerRecord, blockCount - start);
            struct TRecordTag
            { };
            auto buffer = TSharedMutableRef::Allocate<TRecordTag>(
                blockWithHeaderSize * perRecordBlockCount,
                {.InitializeStorage = false});
            for (int index = 0; index < perRecordBlockCount; ++index) {
                const auto& block = blocks[start + index];
                if (std::ssize(block) != blockSize) {
                    return MakeFuture<std::vector<TStoredBlockId>>(TError(
                        "Invalid block size: expected %v, got %v",
                        blockSize,
                        std::ssize(block)));
                }
                // Lay the block out as a block-with-header: [THunkPayloadHeader][block bytes].
                auto* dst = buffer.Begin() + index * blockWithHeaderSize;
                reinterpret_cast<THunkPayloadHeader*>(dst)->Checksum = GetChecksum(block);
                std::memcpy(dst + sizeof(THunkPayloadHeader), block.Begin(), blockSize);
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
        i64 blockWithHeaderSize = sizeof(THunkPayloadHeader) + blockSize;

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
                // The block is stored as [THunkPayloadHeader][block bytes]; read both, strip below.
                requests.push_back({
                    .ChunkId = chunk->ChunkId,
                    .Length = blockWithHeaderSize,
                    .BlockIndex = parsedBlockId.RecordIndex,
                    .BlockOffset = parsedBlockId.FragmentIndex * blockWithHeaderSize,
                });
            }
        }

        return FragmentReader_->ReadFragments(std::move(requests), options)
            .AsUnique()
            .Apply(BIND([] (IChunkFragmentReader::TReadFragmentsResponse&& response) {
                // Drop the THunkPayloadHeader prefix from each fragment, yielding the block payload.
                for (auto& fragment : response.Fragments) {
                    fragment = fragment.Slice(sizeof(THunkPayloadHeader), fragment.Size());
                }
                return std::move(response.Fragments);
            }));
    }

    TFuture<void> SealChunks(TRange<TChunkId> chunkIds) final
    {
        std::vector<TChunkEntryPtr> chunksToAbandon;
        std::vector<TFuture<void>> sealedFutures;
        {
            auto guard = ReaderGuard(IndexToChunkLock_);
            for (auto chunkId : chunkIds) {
                auto it = ChunkIdToChunk_.find(chunkId);
                if (it == ChunkIdToChunk_.end()) {
                    return MakeFuture(TError("Chunk %v does not belong to the block store", chunkId));
                }
                chunksToAbandon.push_back(it->second);
                sealedFutures.push_back(it->second->SealedFuture);
            }
        }

        // A chunk the snapshot references may still be writable -- most likely it is, the latest blocks
        // having been flushed into it -- and nothing would ever seal it unless we abandon it here.
        for (const auto& chunk : chunksToAbandon) {
            AbandonChunk(chunk);
        }
        MaintenanceExecutor_->ScheduleOutOfBand();

        // Sealing itself retries forever; bound the wait here so that a stuck seal fails only the
        // snapshot at hand, leaving a later one free to succeed once sealing recovers.
        return AllSucceeded(std::move(sealedFutures))
            .WithTimeout(
                Config_->SnapshotSealTimeout,
                {.Error = TError("Timed out sealing the snapshot chunks")});
    }

    std::vector<TStoredBlockRef> GetBlockRefs(TRange<TStoredBlockId> blockIds) final
    {
        i64 blockWithHeaderSize = sizeof(THunkPayloadHeader) + Geometry_.BlockSize;

        std::vector<TStoredBlockRef> refs;
        refs.reserve(blockIds.size());

        auto guard = ReaderGuard(IndexToChunkLock_);
        for (auto blockId : blockIds) {
            auto parsedBlockId = ParseStoredBlockId(blockId);
            if (parsedBlockId.ChunkIndex < 0 || parsedBlockId.ChunkIndex >= std::ssize(IndexToChunk_)) {
                THROW_ERROR_EXCEPTION("Invalid stored block id: chunk index %v is out of range [0, %v)",
                    parsedBlockId.ChunkIndex,
                    std::ssize(IndexToChunk_));
            }

            refs.push_back({
                .ChunkId = IndexToChunk_[parsedBlockId.ChunkIndex]->ChunkId,
                .RecordIndex = parsedBlockId.RecordIndex,
                .RecordOffset = parsedBlockId.FragmentIndex * blockWithHeaderSize,
                .PayloadLength = Geometry_.BlockSize,
            });
        }
        return refs;
    }

    TFuture<std::vector<TStoredBlockId>> RestoreBlocks(TRange<TStoredBlockRef> blockRefs) final
    {
        return BIND(&TBlockStore::DoRestoreBlocks, MakeStrong(this), std::vector(blockRefs.begin(), blockRefs.end()))
            .AsyncVia(Invoker_)
            .Run();
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

    enum class ESealState
    {
        None,     // writable, or restored from an already-sealed snapshot
        Waiting,  // abandoned; a maintenance tick starts the next seal attempt once the backoff elapses
        Running,  // a seal attempt is in progress
        Done,     // sealed
    };

    struct TChunkEntry final
    {
        TChunkEntry(
            int index,
            TChunkId chunkId,
            IJournalChunkWriterPtr writer,
            const TExponentialBackoffOptions& sealBackoffOptions)
            : Index(index)
            , ChunkId(chunkId)
            , Writer(std::move(writer))
            , SealBackoff(sealBackoffOptions)
        { }

        const int Index;
        const TChunkId ChunkId;

        //! Released once the chunk leaves the writable set.
        IJournalChunkWriterPtr Writer;

        //! Guarded by WriteLock_.
        ESealState SealState = ESealState::None;

        //! Set once the chunk is sealed; a snapshot referencing the chunk waits for this.
        const TPromise<void> SealedPromise = NewPromise<void>();
        const TFuture<void> SealedFuture = SealedPromise.ToFuture().ToUncancelable();

        TBackoffStrategy SealBackoff;
        TInstant SealRetryDeadline;

        std::vector<TChunkReplicaDescriptor> Replicas;

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
    //! All chunks ever created, indexed by chunk index; append-only, kept around for reads. Both this
    //! and ChunkIdToChunk_ are guarded by IndexToChunkLock_.
    std::vector<TChunkEntryPtr> IndexToChunk_;
    THashMap<TChunkId, TChunkEntryPtr> ChunkIdToChunk_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, WriteLock_);
    //! Chunks currently accepting writes; retired when full or on writer failure.
    std::vector<TChunkEntryPtr> WritableChunks_;
    //! Chunks abandoned but not yet sealed, i.e. exactly those the maintenance tick drives. Guarded by
    //! WriteLock_.
    THashSet<TChunkEntryPtr> ChunksToSeal_;
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
            AbandonChunk(evictedChunk);
        }

        return result;
    }

    void OnMaintenanceTick()
    {
        RetireOversizedChunks();
        SealAbandonedChunks();
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
            AbandonChunk(chunk);
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
            EChunkFormat::HunkJournal,
            Logger);

        WaitFor(writer->Open())
            .ThrowOnError();

        // The chunk index equals its position in IndexToChunk_ (serial appends). Publish the
        // chunk to the read map first, then to the writable set.
        TChunkEntryPtr chunk;
        {
            auto guard = WriterGuard(IndexToChunkLock_);
            int index = std::ssize(IndexToChunk_);
            chunk = New<TChunkEntry>(index, sessionId.ChunkId, std::move(writer), Config_->SealBackoff);
            IndexToChunk_.push_back(chunk);
            EmplaceOrCrash(ChunkIdToChunk_, chunk->ChunkId, chunk);
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

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        auto sessionId = FromProto<TSessionId>(rsp->session_id());

        if (ChunkListId_) {
            AttachChunks(ChunkListId_, {sessionId.ChunkId});
        }

        return sessionId;
    }

    std::vector<TStoredBlockId> DoRestoreBlocks(std::vector<TStoredBlockRef> blockRefs)
    {
        i64 blockWithHeaderSize = sizeof(THunkPayloadHeader) + Geometry_.BlockSize;

        // Assign a chunk index to each distinct referenced chunk and register a read-only entry for
        // it (no writer) so ReadBlocks can resolve the stored block ids we hand back.
        THashMap<TChunkId, int> chunkIdToIndex;
        std::vector<TChunkId> chunksToAttach;
        std::vector<TStoredBlockId> blockIds;
        blockIds.reserve(blockRefs.size());
        {
            // The maintenance executor may already have created writable chunks; restored chunks are
            // appended after them. Chunk indices are stable (append-only), so the stored block ids we
            // return stay valid.
            auto guard = WriterGuard(IndexToChunkLock_);
            for (const auto& ref : blockRefs) {
                auto [it, inserted] = chunkIdToIndex.emplace(ref.ChunkId, std::ssize(IndexToChunk_));
                if (inserted) {
                    // New chunks are created on the transaction's cell (see #CreateChunk), so it must match
                    // the restored chunks' cell -- otherwise a later snapshot could not co-locate them all
                    // onto one destination table. The caller starting the transaction is responsible for
                    // pinning it to the snapshot's cell.
                    if (CellTagFromId(ref.ChunkId) != CellTagFromId(TransactionId_)) {
                        THROW_ERROR_EXCEPTION(
                            "Device transaction cell does not match the snapshot chunks' cell")
                            << TErrorAttribute("transaction_cell_tag", CellTagFromId(TransactionId_))
                            << TErrorAttribute("chunk_cell_tag", CellTagFromId(ref.ChunkId))
                            << TErrorAttribute("chunk_id", ref.ChunkId);
                    }
                    if (std::ssize(IndexToChunk_) >= MaxChunksPerDevice) {
                        THROW_ERROR_EXCEPTION("Snapshot references more chunks than a device may address")
                            << TErrorAttribute("max_chunks_per_device", MaxChunksPerDevice);
                    }
                    auto chunk = New<TChunkEntry>(it->second, ref.ChunkId, /*writer*/ nullptr, Config_->SealBackoff);
                    // A restored chunk is sealed by construction: it is referenced by a snapshot table's
                    // hunk chunk list, which the master only accepts sealed chunks into. Nothing will ever
                    // seal it again, so a later snapshot must not wait for that.
                    chunk->SealState = ESealState::Done;
                    chunk->SealedPromise.Set();
                    EmplaceOrCrash(ChunkIdToChunk_, chunk->ChunkId, chunk);
                    IndexToChunk_.push_back(std::move(chunk));
                    chunksToAttach.push_back(ref.ChunkId);
                }

                // The refs come from a snapshot table, which may have been written by a device with a
                // different geometry; reject such a snapshot rather than misread it (or trip the packing
                // invariants of MakeStoredBlockId, which only its internal callers are entitled to).
                if (ref.PayloadLength != Geometry_.BlockSize) {
                    THROW_ERROR_EXCEPTION("Snapshot block payload length does not match the device block size")
                        << TErrorAttribute("payload_length", ref.PayloadLength)
                        << TErrorAttribute("block_size", Geometry_.BlockSize)
                        << TErrorAttribute("chunk_id", ref.ChunkId);
                }
                if (ref.RecordOffset < 0 || ref.RecordOffset % blockWithHeaderSize != 0) {
                    THROW_ERROR_EXCEPTION("Snapshot block record offset is not a multiple of the block size")
                        << TErrorAttribute("record_offset", ref.RecordOffset)
                        << TErrorAttribute("block_with_header_size", blockWithHeaderSize)
                        << TErrorAttribute("chunk_id", ref.ChunkId);
                }

                auto fragmentIndex = ref.RecordOffset / blockWithHeaderSize;
                if (fragmentIndex >= MaxBlocksPerRecord) {
                    THROW_ERROR_EXCEPTION("Snapshot block record offset is out of range")
                        << TErrorAttribute("record_offset", ref.RecordOffset)
                        << TErrorAttribute("max_blocks_per_record", MaxBlocksPerRecord)
                        << TErrorAttribute("chunk_id", ref.ChunkId);
                }
                if (ref.RecordIndex < 0 || ref.RecordIndex >= MaxRecordsPerChunk) {
                    THROW_ERROR_EXCEPTION("Snapshot block record index is out of range")
                        << TErrorAttribute("record_index", ref.RecordIndex)
                        << TErrorAttribute("max_records_per_chunk", MaxRecordsPerChunk)
                        << TErrorAttribute("chunk_id", ref.ChunkId);
                }

                blockIds.push_back(MakeStoredBlockId({
                    .ChunkIndex = it->second,
                    .RecordIndex = ref.RecordIndex,
                    .FragmentIndex = static_cast<int>(fragmentIndex),
                }));
            }
        }

        if (!chunksToAttach.empty() && ChunkListId_) {
            AttachChunks(ChunkListId_, chunksToAttach);
        }
        return blockIds;
    }

    void AttachChunks(TChunkListId chunkListId, const std::vector<TChunkId>& chunkIds)
    {
        TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(chunkListId)));
        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        NCypressClient::SetTransactionId(batchReq, TransactionId_);

        auto* req = batchReq->add_attach_chunk_trees_subrequests();
        ToProto(req->mutable_parent_id(), chunkListId);
        ToProto(req->mutable_child_ids(), chunkIds);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Failed to attach chunks to the device chunk list %v",
            chunkListId);

        YT_LOG_INFO("Attached chunks to the device chunk list (ChunkListId: %v, ChunkCount: %v)",
            chunkListId,
            chunkIds.size());
    }

    TChunkEntryPtr FindChunk(TChunkId chunkId)
    {
        auto guard = ReaderGuard(IndexToChunkLock_);
        return GetOrDefault(ChunkIdToChunk_, chunkId);
    }

    void DropChunk(TChunkId chunkId, const TError& error)
    {
        // Concurrent write sessions may all fail on the same chunk; only the one that actually abandons
        // it reports the drop.
        auto chunk = FindChunk(chunkId);
        if (chunk && AbandonChunk(chunk)) {
            YT_LOG_WARNING(error, "Dropped block store chunk writer (ChunkId: %v)",
                chunkId);
        }
    }

    //! Takes #chunk out of the writable set for good and marks it for sealing.
    /*!
     *  Used both for a retired (full) chunk and for one whose writer failed. Either way the records the
     *  chunk already accepted stay referenced by the block map, so it must eventually be sealed for a
     *  snapshot to reference it.
     *
     *  Idempotent; returns whether this call is the one that abandoned the chunk.
     */
    bool AbandonChunk(const TChunkEntryPtr& chunk)
    {
        auto guard = Guard(WriteLock_);

        if (auto it = std::ranges::find(WritableChunks_, chunk); it != WritableChunks_.end()) {
            std::swap(*it, WritableChunks_.back());
            WritableChunks_.pop_back();
        }

        auto writer = std::exchange(chunk->Writer, nullptr);
        bool abandoned = static_cast<bool>(writer);
        if (writer) {
            chunk->Replicas = writer->GetChunkReplicaDescriptors();
            CloseWriter(chunk->ChunkId, writer);
        }

        if (chunk->SealState == ESealState::None) {
            chunk->SealState = ESealState::Waiting;
            InsertOrCrash(ChunksToSeal_, chunk);
        }

        return abandoned;
    }

    //! Closes an abandoned chunk's writer.
    /*!
     *  Best effort: a dropped chunk's writer has already failed, so its close fails too. Sealing does not
     *  depend on the close -- it aborts the replicas' sessions itself -- so the error is merely logged
     *  and nothing waits for it.
     */
    void CloseWriter(TChunkId chunkId, const IJournalChunkWriterPtr& writer)
    {
        writer->Close().Subscribe(
            BIND([this, this_ = MakeStrong(this), chunkId] (const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_WARNING(error, "Failed to close block store chunk writer (ChunkId: %v)",
                        chunkId);
                }
            })
            .Via(Invoker_));
    }

    void SealAbandonedChunks()
    {
        auto now = TInstant::Now();
        std::vector<TChunkEntryPtr> chunksToSeal;
        {
            auto guard = Guard(WriteLock_);
            for (const auto& chunk : ChunksToSeal_) {
                // Hold off until the backoff from a recent seal failure has elapsed.
                if (chunk->SealState == ESealState::Waiting && now >= chunk->SealRetryDeadline) {
                    chunk->SealState = ESealState::Running;
                    chunksToSeal.push_back(chunk);
                }
            }
        }

        for (const auto& chunk : chunksToSeal) {
            YT_UNUSED_FUTURE(BIND(&TBlockStore::SealChunk, MakeStrong(this), chunk)
                .AsyncVia(Invoker_)
                .Run());
        }
    }

    void SealChunk(const TChunkEntryPtr& chunk)
    {
        try {
            DoSealChunk(chunk);
        } catch (const std::exception& ex) {
            auto guard = Guard(WriteLock_);
            chunk->SealBackoff.Next();
            auto backoff = chunk->SealBackoff.GetBackoff();
            chunk->SealRetryDeadline = TInstant::Now() + backoff;
            chunk->SealState = ESealState::Waiting;
            guard.Release();

            YT_LOG_WARNING(ex, "Failed to seal block store chunk, will retry (ChunkId: %v, Backoff: %v)",
                chunk->ChunkId,
                backoff);
            return;
        }

        {
            auto guard = Guard(WriteLock_);
            YT_VERIFY(chunk->SealState == ESealState::Running);
            chunk->SealState = ESealState::Done;
            EraseOrCrash(ChunksToSeal_, chunk);
        }
        chunk->SealedPromise.Set();
    }

    void DoSealChunk(const TChunkEntryPtr& chunk)
    {
        YT_LOG_INFO("Sealing block store chunk (ChunkId: %v)", chunk->ChunkId);

        const auto& connection = Client_->GetNativeConnection();
        auto nodeChannelFactory = NNodeTrackerClient::CreateNodeChannelFactory(
            connection->GetChannelFactory(),
            connection->GetNetworks());

        auto abortedReplicas = WaitFor(AbortSessionsQuorum(
            chunk->ChunkId,
            chunk->Replicas,
            Config_->SealRpcTimeout,
            Config_->SealQuorumSessionDelay,
            Config_->ReadQuorum,
            nodeChannelFactory))
            .ValueOrThrow();

        auto quorumInfo = WaitFor(ComputeQuorumInfo(
            chunk->ChunkId,
            /*overlayed*/ false,
            NErasure::ECodec::None,
            Config_->ReadQuorum,
            NJournalClient::DefaultReplicaLagLimit,
            std::move(abortedReplicas),
            Config_->SealRpcTimeout,
            nodeChannelFactory))
            .ValueOrThrow();

        TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(chunk->ChunkId)));
        auto req = proxy.SealChunk();
        GenerateMutationId(req);
        ToProto(req->mutable_chunk_id(), chunk->ChunkId);
        req->mutable_info()->set_row_count(quorumInfo.RowCount);
        req->mutable_info()->set_uncompressed_data_size(quorumInfo.UncompressedDataSize);
        req->mutable_info()->set_compressed_data_size(quorumInfo.CompressedDataSize);

        WaitFor(req->Invoke())
            .ThrowOnError();

        YT_LOG_INFO("Block store chunk sealed (ChunkId: %v, RowCount: %v)",
            chunk->ChunkId,
            quorumInfo.RowCount);
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
