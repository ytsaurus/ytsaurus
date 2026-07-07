#include "partition_reader.h"

#include "config.h"
#include "public.h"
#include "record_format.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_reader.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <algorithm>

namespace NYT::NPushBasedShuffleClient {

using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NLogging;
using namespace NTableClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCreateChunkSessionReaderCallback = std::function<
    IDistributedChunkSessionReaderPtr(
        TChunkId chunkId,
        TChunkReplicaList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex)>;

////////////////////////////////////////////////////////////////////////////////

struct TBatchPayloadsHolder
    : public TSharedRangeHolder
{
    //! Backs all TUnversionedRow arrays in this batch.
    TRowBufferPtr RowBuffer;

    //! One TSharedRef per record; pins string/Any/Composite payload bytes.
    std::vector<TSharedRef> Payloads;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkReadState
{
    IDistributedChunkSessionReaderPtr ChunkSessionReader;

    //! Latest unconsumed chunk session read result. We issue the next Read()
    //! only after the previous one's callback completes, so there's never
    //! more than one in flight (no "in flight" companion needed); finished
    //! chunks are evicted from ChunkStates_ (no Finished flag needed).
    std::optional<TChunkReadResult> ReadyResult;
};

////////////////////////////////////////////////////////////////////////////////

class TPushBasedPartitionReader
    : public IPushBasedPartitionReader
{
public:
    TPushBasedPartitionReader(
        TPartitionReaderConfigPtr config,
        TCreateChunkSessionReaderCallback createDistributedChunkSessionReader,
        TRecordHeaderFilter recordHeaderFilter,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , CreateDistributedChunkSessionReader_(std::move(createDistributedChunkSessionReader))
        , RecordHeaderFilter_(std::move(recordHeaderFilter))
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(PushBasedShuffleLogger())
    {
        YT_VERIFY(Config_);
        YT_VERIFY(CreateDistributedChunkSessionReader_);

        YT_LOG_INFO(
            "Push-based shuffle reader created (Codec: %v, MaxBytesPerRead: %v, "
            "RowBufferStartChunkSize: %v, HasHeaderFilter: %v)",
            Config_->Codec,
            Config_->MaxBytesPerRead,
            Config_->RowBufferStartChunkSize,
            static_cast<bool>(RecordHeaderFilter_));
    }

    TFuture<TShuffleReadBatchPtr> Read() override
    {
        auto promise = NewPromise<TShuffleReadBatchPtr>();
        auto future = promise.ToFuture().ToUncancelable();
        SerializedInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TPushBasedPartitionReader::DoRead,
            MakeWeak(this),
            Passed(std::move(promise))));
        return future;
    }

    void AddChunk(
        TChunkId chunkId,
        TChunkReplicaWithMediumList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex) override
    {
        SerializedInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TPushBasedPartitionReader::DoAddChunk,
            MakeWeak(this),
            chunkId,
            std::move(replicas),
            startRecordIndex,
            rangeEndRecordIndex));
    }

    void SetNoMoreChunks() override
    {
        SerializedInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TPushBasedPartitionReader::DoSetNoMoreChunks,
            MakeWeak(this)));
    }

private:
    const TPartitionReaderConfigPtr Config_;
    const TCreateChunkSessionReaderCallback CreateDistributedChunkSessionReader_;
    const TRecordHeaderFilter RecordHeaderFilter_;
    const IInvokerPtr SerializedInvoker_;
    const TLogger Logger;

    THashMap<TChunkId, TChunkReadState> ChunkStates_;
    bool NoMoreChunks_ = false;
    bool FinishedLogged_ = false;
    TError TerminalError_;
    TPromise<TShuffleReadBatchPtr> PendingReadPromise_;

    void DoAddChunk(
        TChunkId chunkId,
        TChunkReplicaWithMediumList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        // Terminal-error check FIRST: a stale AddChunk racing after FailReader
        // (including a duplicate chunkId, or one that arrives after
        // SetNoMoreChunks already in flight) must be silently ignored per the
        // public contract — never YT_VERIFY-abort the process on a benign race.
        if (!TerminalError_.IsOK()) {
            return;
        }

        YT_VERIFY(!NoMoreChunks_);
        YT_VERIFY(!ChunkStates_.contains(chunkId));

        auto sessionReplicas = TChunkReplicaWithMedium::ToChunkReplicas(TRange(replicas));
        auto sessionReader = CreateDistributedChunkSessionReader_(
            chunkId,
            std::move(sessionReplicas),
            startRecordIndex,
            rangeEndRecordIndex);

        TChunkReadState state;
        state.ChunkSessionReader = sessionReader;
        SubscribeToChunkSessionRead(chunkId, sessionReader->Read());

        ChunkStates_[chunkId] = std::move(state);

        YT_LOG_DEBUG(
            "Chunk added (ChunkId: %v, StartRecordIndex: %v, RangeEndRecordIndex: %v)",
            chunkId,
            startRecordIndex,
            rangeEndRecordIndex);
    }

    void DoSetNoMoreChunks() noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (NoMoreChunks_) {
            return;
        }
        if (!TerminalError_.IsOK()) {
            return;
        }
        NoMoreChunks_ = true;
        for (auto& [_, state] : ChunkStates_) {
            state.ChunkSessionReader->SetAllWritersFinished();
        }
        MaybeResolveRead();

        YT_LOG_DEBUG("Shuffle reader sealed");
    }

    void DoRead(TPromise<TShuffleReadBatchPtr> promise) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(!PendingReadPromise_);
        PendingReadPromise_ = std::move(promise);
        MaybeResolveRead();
    }

    void SubscribeToChunkSessionRead(TChunkId chunkId, TFuture<TChunkReadResult> readFuture) noexcept
    {
        readFuture.Subscribe(BIND_NO_PROPAGATE(
            &TPushBasedPartitionReader::OnChunkSessionReadComplete,
            MakeWeak(this),
            chunkId)
            .Via(SerializedInvoker_));
    }

    void OnChunkSessionReadComplete(TChunkId chunkId, TErrorOr<TChunkReadResult> resultOrError) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto stateIt = ChunkStates_.find(chunkId);
        YT_VERIFY(stateIt != ChunkStates_.end());
        auto& state = stateIt->second;

        if (!resultOrError.IsOK()) {
            FailReader(resultOrError);
            return;
        }

        // Drop late successes once the reader has failed: no consumer will
        // ever drain this result (MaybeResolveRead resolves the pending
        // promise with TerminalError_ and skips the drain loop), so storing
        // it would keep the blob buffers pinned until reader teardown for
        // no purpose. Letting `resultOrError` die at function exit releases
        // them immediately.
        if (!TerminalError_.IsOK()) {
            return;
        }

        state.ReadyResult = std::move(resultOrError.Value());
        MaybeResolveRead();
    }

    void MaybeResolveRead() noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!PendingReadPromise_) {
            return;
        }
        if (!TerminalError_.IsOK()) {
            auto promise = std::move(PendingReadPromise_);
            promise.Set(TerminalError_);
            return;
        }

        // Nothing queued: skip the batch/holder/RowBuffer alloc. Also
        // covers the terminal empty Finished=true resolution once every
        // chunk has been drained and NoMoreChunks_ is set.
        bool anyReady = std::any_of(
            ChunkStates_.begin(),
            ChunkStates_.end(),
            [] (const auto& kv) { return kv.second.ReadyResult.has_value(); });
        if (!anyReady) {
            if (NoMoreChunks_ && ChunkStates_.empty()) {
                auto emptyBatch = New<TShuffleReadBatch>();
                emptyBatch->Finished = true;
                auto promise = std::move(PendingReadPromise_);
                promise.Set(std::move(emptyBatch));
                MaybeLogFinished();
            }
            return;
        }

        // Reserve from the sum of ready-chunk records: one walk avoids
        // log2(N) reallocs in the drain. Sum can be zero — a chunk session
        // may report {Records=[], Finished=true}, and the drain loop still
        // needs to run to evict the finished entry.
        i64 expectedRecordCount = 0;
        for (auto& [_, state] : ChunkStates_) {
            if (state.ReadyResult) {
                expectedRecordCount += std::ssize(state.ReadyResult->Records);
            }
        }

        auto batch = New<TShuffleReadBatch>();
        auto holder = New<TBatchPayloadsHolder>();
        holder->RowBuffer = New<TRowBuffer>(
            TDefaultRowBufferPoolTag(),
            Config_->RowBufferStartChunkSize);

        holder->Payloads.reserve(expectedRecordCount);
        batch->Records.reserve(expectedRecordCount);

        // Finished chunks are collected here and erased AFTER the drain loop:
        // erasing mid-iteration would invalidate the iterator we're holding.
        std::vector<TChunkId> finishedChunks;
        i64 drainedBytes = 0;
        for (auto& [chunkId, state] : ChunkStates_) {
            if (!state.ReadyResult) {
                continue;
            }
            auto& records = state.ReadyResult->Records;
            i64 consumed = 0;
            for (; consumed < std::ssize(records); ++consumed) {
                if (drainedBytes >= Config_->MaxBytesPerRead) {
                    break;
                }
                auto& blob = records[consumed];
                drainedBytes += std::ssize(blob);
                try {
                    if (RecordHeaderFilter_) {
                        auto header = ReadShuffleRecordHeader(TRange(&blob, 1));
                        if (!RecordHeaderFilter_(header)) {
                            continue;
                        }
                    }
                    auto record = DecompressShuffleRecord(TRange(&blob, 1), Config_->Codec);
                    auto parsed = ParseShuffleRecord(std::move(record), holder->RowBuffer->GetPool());
                    holder->Payloads.push_back(std::move(parsed.UncompressedPayload));
                    batch->Records.push_back({
                        parsed.Header,
                        TSharedRange<TUnversionedRow>(parsed.Rows, holder),
                    });
                } catch (const std::exception& ex) {
                    state.ReadyResult.reset();
                    FailReader(TError(ex));
                    return;
                }
            }
            if (consumed < std::ssize(records)) {
                // Cap hit mid-chunk; slice off consumed records and leave the
                // tail (along with any Finished=true marker) in ReadyResult
                // for the next Read to pick up.
                records.erase(records.begin(), records.begin() + consumed);
                break;
            }
            bool wasFinished = state.ReadyResult->Finished;
            state.ReadyResult.reset();
            if (wasFinished) {
                finishedChunks.push_back(chunkId);
            } else {
                SubscribeToChunkSessionRead(chunkId, state.ChunkSessionReader->Read());
            }
        }
        for (auto chunkId : finishedChunks) {
            ChunkStates_.erase(chunkId);
        }

        batch->Finished = NoMoreChunks_ && ChunkStates_.empty();
        if (batch->Finished) {
            MaybeLogFinished();
        }
        auto promise = std::move(PendingReadPromise_);
        promise.Set(std::move(batch));
    }

    void FailReader(const TError& error) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (TerminalError_.IsOK()) {
            TerminalError_ = error;
            YT_LOG_WARNING(TerminalError_, "Shuffle reader failed");

            // Wind down outstanding chunk session reads so they don't poll forever.
            for (auto& [_, state] : ChunkStates_) {
                state.ChunkSessionReader->SetAllWritersFinished();
            }
        } else {
            // First failure wins; later errors are observability noise — log them
            // at DEBUG so the diagnostic info isn't silently dropped, but don't
            // overwrite TerminalError_ (downstream code is keyed on the first).
            YT_LOG_DEBUG(error, "Shuffle reader received subsequent failure");
        }
        if (PendingReadPromise_) {
            auto promise = std::move(PendingReadPromise_);
            promise.Set(TerminalError_);
        }
    }

    void MaybeLogFinished() noexcept
    {
        if (FinishedLogged_) {
            return;
        }
        YT_LOG_INFO("Shuffle reader finished");
        FinishedLogged_ = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IPushBasedPartitionReaderPtr CreatePushBasedPartitionReaderForTesting(
    TPartitionReaderConfigPtr config,
    TCreateChunkSessionReaderCallback createDistributedChunkSessionReader,
    IInvokerPtr invoker,
    TRecordHeaderFilter recordHeaderFilter)
{
    return New<TPushBasedPartitionReader>(
        std::move(config),
        std::move(createDistributedChunkSessionReader),
        std::move(recordHeaderFilter),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

IPushBasedPartitionReaderPtr CreatePushBasedPartitionReader(
    TPartitionReaderConfigPtr config,
    IClientPtr client,
    TChunkReaderHostPtr chunkReaderHost,
    int readQuorum,
    IInvokerPtr invoker,
    TRecordHeaderFilter recordHeaderFilter)
{
    auto createSessionReader = [
        config,
        client = std::move(client),
        chunkReaderHost = std::move(chunkReaderHost),
        readQuorum,
        invoker
    ] (
        TChunkId chunkId,
        TChunkReplicaList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex)
    {
        return CreateDistributedChunkSessionReader(
            config->ChunkSessionReaderConfig,
            client,
            chunkReaderHost,
            chunkId,
            std::move(replicas),
            readQuorum,
            startRecordIndex,
            rangeEndRecordIndex,
            invoker);
    };

    return New<TPushBasedPartitionReader>(
        std::move(config),
        std::move(createSessionReader),
        std::move(recordHeaderFilter),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
