#include "partition_reader.h"

#include "config.h"
#include "public.h"
#include "record_format.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_reader.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <util/generic/hash_set.h>

#include <algorithm>
#include <deque>

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
    TFuture<TChunkReadResult> ReadFuture;
    std::optional<TChunkReadResult> ReadyResult;
    int NextRecordIndex = 0;
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
        auto future = promise.ToFuture();
        promise.OnCanceled(BIND_NO_PROPAGATE(
            &TPushBasedPartitionReader::OnReadCanceled,
            MakeWeak(this))
            .Via(SerializedInvoker_));
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
    TRecordHeaderFilter RecordHeaderFilter_;
    const IInvokerPtr SerializedInvoker_;
    const TLogger Logger;

    THashMap<TChunkId, TChunkReadState> ChunkStates_;
    std::deque<TChunkId> ReadyChunkIds_;
    THashSet<std::pair<i32, i64>> SeenRecords_;
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
        StartChunkSessionRead(chunkId, &state);

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

    void OnReadCanceled(const TError& error) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        FailReader(TError(NYT::EErrorCode::Canceled, "Partition reader canceled") << error);
    }

    void StartChunkSessionRead(TChunkId chunkId, TChunkReadState* state) noexcept
    {
        state->ReadFuture = state->ChunkSessionReader->Read();
        state->ReadFuture.Subscribe(BIND_NO_PROPAGATE(
            &TPushBasedPartitionReader::OnChunkSessionReadComplete,
            MakeWeak(this),
            chunkId)
            .Via(SerializedInvoker_));
    }

    void OnChunkSessionReadComplete(TChunkId chunkId, TErrorOr<TChunkReadResult> resultOrError) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK()) {
            if (!resultOrError.IsOK()) {
                YT_LOG_DEBUG(resultOrError, "Shuffle reader received subsequent failure");
            }
            return;
        }

        auto stateIt = ChunkStates_.find(chunkId);
        YT_VERIFY(stateIt != ChunkStates_.end());
        auto& state = stateIt->second;
        state.ReadFuture.Reset();

        if (!resultOrError.IsOK()) {
            FailReader(resultOrError);
            return;
        }

        YT_VERIFY(!state.ReadyResult);
        YT_VERIFY(state.NextRecordIndex == 0);
        state.ReadyResult = std::move(resultOrError.Value());
        ReadyChunkIds_.push_back(chunkId);
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

        if (ReadyChunkIds_.empty()) {
            if (NoMoreChunks_ && ChunkStates_.empty()) {
                auto emptyBatch = New<TShuffleReadBatch>();
                emptyBatch->Finished = true;
                ReleaseRecordFilterState();
                auto promise = std::move(PendingReadPromise_);
                promise.Set(std::move(emptyBatch));
                MaybeLogFinished();
            }
            return;
        }

        auto batch = New<TShuffleReadBatch>();
        auto holder = New<TBatchPayloadsHolder>();
        holder->RowBuffer = New<TRowBuffer>(
            TDefaultRowBufferPoolTag(),
            Config_->RowBufferStartChunkSize);

        const auto& firstState = GetOrCrash(ChunkStates_, ReadyChunkIds_.front());
        YT_VERIFY(firstState.ReadyResult);
        const auto& firstRecords = firstState.ReadyResult->Records;
        int remainingRecordCount = std::ssize(firstRecords) - firstState.NextRecordIndex;
        if (remainingRecordCount > 0) {
            constexpr int RecordSizeSampleCount = 32;
            int sampleCount = std::min(remainingRecordCount, RecordSizeSampleCount);
            i64 sampleBytes = 0;
            for (int index = 0; index < sampleCount; ++index) {
                sampleBytes += std::ssize(firstRecords[firstState.NextRecordIndex + index]);
            }
            i64 averageRecordSize = std::max<i64>(1, sampleBytes / sampleCount);
            i64 expectedRecordCount = std::min<i64>(
                remainingRecordCount,
                Config_->MaxBytesPerRead / averageRecordSize + 1);
            holder->Payloads.reserve(expectedRecordCount);
            batch->Records.reserve(expectedRecordCount);
        }

        i64 drainedBytes = 0;
        while (!ReadyChunkIds_.empty()) {
            auto chunkId = ReadyChunkIds_.front();
            auto stateIt = ChunkStates_.find(chunkId);
            YT_VERIFY(stateIt != ChunkStates_.end());
            auto& state = stateIt->second;
            YT_VERIFY(state.ReadyResult);

            auto& result = *state.ReadyResult;
            while (state.NextRecordIndex < std::ssize(result.Records)) {
                if (drainedBytes >= Config_->MaxBytesPerRead) {
                    break;
                }
                auto& blob = result.Records[state.NextRecordIndex++];
                drainedBytes += std::ssize(blob);
                try {
                    auto header = ReadShuffleRecordHeader(blob);
                    if (!SeenRecords_.emplace(header.MapperId, header.StartRow).second ||
                        RecordHeaderFilter_ &&
                        !RecordHeaderFilter_(header))
                    {
                        continue;
                    }
                    auto record = DecompressShuffleRecord(blob, Config_->Codec);
                    auto parsed = ParseShuffleRecord(std::move(record), holder->RowBuffer->GetPool());
                    holder->Payloads.push_back(std::move(parsed.UncompressedPayload));
                    batch->Records.push_back({
                        parsed.Header,
                        TSharedRange<TUnversionedRow>(parsed.Rows, holder),
                    });
                } catch (const std::exception& ex) {
                    FailReader(TError(ex));
                    return;
                }
            }
            if (state.NextRecordIndex < std::ssize(result.Records)) {
                break;
            }

            bool wasFinished = result.Finished;
            state.ReadyResult.reset();
            state.NextRecordIndex = 0;
            ReadyChunkIds_.pop_front();
            if (wasFinished) {
                ChunkStates_.erase(stateIt);
            } else {
                StartChunkSessionRead(chunkId, &state);
            }
        }

        batch->Finished = NoMoreChunks_ && ChunkStates_.empty();
        if (batch->Finished) {
            ReleaseRecordFilterState();
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

            ReadyChunkIds_.clear();
            ReleaseRecordFilterState();
            for (auto& [_, state] : ChunkStates_) {
                state.ChunkSessionReader->SetAllWritersFinished();
                if (state.ReadFuture) {
                    state.ReadFuture.Cancel(TerminalError_);
                }
            }
            ChunkStates_.clear();
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

    void ReleaseRecordFilterState() noexcept
    {
        RecordHeaderFilter_ = {};
        SeenRecords_ = THashSet<std::pair<i32, i64>>();
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
