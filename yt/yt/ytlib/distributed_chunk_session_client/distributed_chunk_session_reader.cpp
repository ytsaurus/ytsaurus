#include "distributed_chunk_session_reader.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/context_switch.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/cast.h>

#include <deque>

namespace NYT::NDistributedChunkSessionClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NErasure;
using namespace NJournalClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;

using NApi::NNative::IClientPtr;

using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

namespace {

// The reader starts in the active phase and queries replica metadata directly to
// keep up with an unsealed journal chunk. Once writers finish or replica metadata
// reports a seal, it switches to the final phase, learns the final row count if
// needed, and reads the remaining range through the regular replication reader.

DEFINE_ENUM(EPhase,
    (Active)
    (Final)
);

DEFINE_ENUM(EReplicaProgressOutcome,
    (HasData)
    (Sealed)
    (NoNewData)
    (AllNoSuchChunk)
    (AllOtherErrors)
);

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionReader
    : public IDistributedChunkSessionReader
{
public:
    TDistributedChunkSessionReader(
        TDistributedChunkSessionReaderConfigPtr config,
        IClientPtr client,
        TChunkReaderHostPtr chunkReaderHost,
        TChunkId chunkId,
        TChunkReplicaList replicas,
        int readQuorum,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , ChunkReaderHost_(std::move(chunkReaderHost))
        , ChunkId_(chunkId)
        , Replicas_(std::move(replicas))
        , ReadQuorum_(readQuorum)
        , RangeEndIndex_(rangeEndRecordIndex)
        , Cursor_(startRecordIndex)
        , Statistics_(New<TDistributedChunkSessionReaderStatistics>())
        , ErrorBackoffStrategy_(Config_->ErrorBackoff)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(DistributedChunkSessionLogger().WithTag("ChunkId: %v", ChunkId_))
    {
        YT_VERIFY(ReadQuorum_ > 0);
        YT_VERIFY(startRecordIndex >= 0);
        YT_VERIFY(!RangeEndIndex_ || *RangeEndIndex_ >= startRecordIndex);

        YT_LOG_DEBUG(
            "Created distributed chunk session reader (ReadQuorum: %v, StartRecordIndex: %v, "
            "RangeEndRecordIndex: %v, ReplicaCount: %v)",
            ReadQuorum_,
            startRecordIndex,
            RangeEndIndex_,
            Replicas_.size());
    }

    TFuture<TChunkReadResult> Read() override
    {
        return BIND(&TDistributedChunkSessionReader::DoRead, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    void SetAllWritersFinished() override
    {
        DoSetAllWritersFinished(std::nullopt, std::nullopt);
    }

    void SetAllWritersFinished(i64 finalRecordCount, i64 finalCompressedDataSize) override
    {
        DoSetAllWritersFinished(finalRecordCount, finalCompressedDataSize);
    }

    TDistributedChunkSessionReaderStatisticsConstPtr GetStatistics() const override
    {
        return Statistics_;
    }

private:
    struct TReplicaProgress
    {
        EReplicaProgressOutcome Outcome;
        std::optional<TChunkReplica> PickedReplica;
        i64 PickedRowCount = 0;
        std::vector<TError> InnerErrors;
    };

    // Phase-2 prefetch state. All mutated only on SerializedInvoker_.
    //
    // The read range is split into windows. Each window reads its own sub-range
    // sequentially, advancing by the number of records actually returned, so a
    // server-truncated read is simply continued by the next read and never leaves a
    // sliver. A window keeps at most one read in flight and buffers up to PrefetchDepth
    // reads ahead of the consumer; a window that exhausts its range steals half of the
    // window with the largest remainder.
    struct TWindow
    {
        i64 NextReadIndex = 0;
        i64 EndIndex = 0;
        // Requested end of the in-flight read, or NextReadIndex when none is in flight.
        i64 InFlightEndIndex = 0;

        TFuture<std::vector<TBlock>> InFlightFuture;

        std::deque<std::vector<TSharedRef>> ReadyBatches;

        // Per-window consecutive-failure budget. Bounds a persistently failing window
        // independently of other windows' progress and of Read() cadence; reset on a
        // successful read.
        int ErrorAttempts = 0;
        TBackoffStrategy BackoffStrategy;

        // Non-null while the window recovers from a failed read (replica refresh plus
        // backoff) on a separate fiber. The window issues no reads while it is set, and the
        // prefetch step reschedules on it.
        TFuture<void> RecoveryFuture;

        // Replica epoch the in-flight read was issued against. On failure the recovery
        // refreshes replicas only if no sibling already refreshed since (epoch unchanged),
        // so simultaneous failures do not all query master.
        i64 ReadEpoch = 0;

        bool HasInFlightRead() const
        {
            return static_cast<bool>(InFlightFuture);
        }

        i64 OutstandingReadCount() const
        {
            return std::ssize(ReadyBatches) + (HasInFlightRead() ? 1 : 0);
        }

        i64 UnreadBeginIndex() const
        {
            return HasInFlightRead() ? InFlightEndIndex : NextReadIndex;
        }

        i64 UnreadCount() const
        {
            return EndIndex - UnreadBeginIndex();
        }

        bool Exhausted() const
        {
            return !HasInFlightRead() && NextReadIndex == EndIndex;
        }
    };

    const TDistributedChunkSessionReaderConfigPtr Config_;
    const IClientPtr Client_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const TChunkId ChunkId_;
    TChunkReplicaList Replicas_;
    const int ReadQuorum_;
    const std::optional<i64> RangeEndIndex_;

    EPhase Phase_ = EPhase::Active;
    i64 Cursor_;
    std::optional<i64> ChunkRecordCount_;
    int ErrorAttempts_ = 0;
    std::vector<TError> InnerErrors_;
    bool ReadInProgress_ = false;

    IChunkReaderPtr FinalReader_;
    // Bumped each time a recovery queries master for fresh replicas. Lets simultaneously
    // failing windows refresh at most once between them rather than each hitting master.
    i64 ReplicaEpoch_ = 0;
    bool PrefetchStarted_ = false;
    std::optional<i64> CompressedDataSize_;
    i64 BlocksPerRead_ = 1;
    std::vector<TWindow> Windows_;
    int LastDeliveredWindowIndex_ = 0;

    // Signaling between the autonomous prefetch and Read().
    TPromise<void> ReaderWakeupPromise_;
    // Single wakeup for the prefetch step: a completed read, a completed recovery, or Read()
    // freeing a depth slot all set it to schedule the next step.
    TPromise<void> PrefetchWakeupPromise_;
    TError PrefetchError_;

    const TDistributedChunkSessionReaderStatisticsPtr Statistics_;
    TBackoffStrategy ErrorBackoffStrategy_;
    const IInvokerPtr SerializedInvoker_;
    const TLogger Logger;

    i64 ComputeEffectiveEnd() const
    {
        i64 end = std::numeric_limits<i64>::max();
        if (RangeEndIndex_) {
            end = std::min(end, *RangeEndIndex_);
        }
        if (ChunkRecordCount_) {
            end = std::min(end, *ChunkRecordCount_);
        }
        return end;
    }

    void DoSetAllWritersFinished(
        std::optional<i64> finalRecordCount,
        std::optional<i64> finalCompressedDataSize)
    {
        SerializedInvoker_->Invoke(BIND([
            this,
            this_ = MakeStrong(this),
            finalRecordCount,
            finalCompressedDataSize
        ] {
            YT_LOG_DEBUG(
                "All writers finished for distributed chunk session reader "
                "(FinalRecordCount: %v, FinalCompressedDataSize: %v, CurrentPhase: %v, "
                "CurrentRecordCount: %v, Cursor: %v)",
                finalRecordCount,
                finalCompressedDataSize,
                Phase_,
                ChunkRecordCount_,
                Cursor_);

            Phase_ = EPhase::Final;
            if (finalRecordCount && !ChunkRecordCount_) {
                ChunkRecordCount_ = *finalRecordCount;
                CompressedDataSize_ = *finalCompressedDataSize;
            }
        }));
    }

    TChunkReadResult DoRead()
    {
        YT_VERIFY(!ReadInProgress_);
        ReadInProgress_ = true;
        auto finally = Finally([&] {
            ReadInProgress_ = false;
            ErrorAttempts_ = 0;
            InnerErrors_.clear();
            ErrorBackoffStrategy_.Restart();
        });

        YT_LOG_DEBUG(
            "Started distributed chunk session reader read (Phase: %v, Cursor: %v, EffectiveEnd: %v)",
            Phase_,
            Cursor_,
            ComputeEffectiveEnd());

        while (ErrorAttempts_ < Config_->MaxReadAttempts) {
            if (Cursor_ >= ComputeEffectiveEnd()) {
                YT_LOG_DEBUG(
                    "Finished distributed chunk session reader read at effective end "
                    "(Cursor: %v, EffectiveEnd: %v, ErrorAttempts: %v)",
                    Cursor_,
                    ComputeEffectiveEnd(),
                    ErrorAttempts_);
                return TChunkReadResult{
                    .Records = {},
                    .Finished = true,
                };
            }

            auto result = Phase_ == EPhase::Active
                ? RunActivePhaseIteration()
                : RunFinalPhaseIteration();

            if (result) {
                return std::move(*result);
            }
        }

        YT_LOG_DEBUG(
            "Failed to read distributed chunk session records; attempts exhausted "
            "(MaxReadAttempts: %v, Cursor: %v, Phase: %v)",
            Config_->MaxReadAttempts,
            Cursor_,
            Phase_);

        THROW_ERROR_EXCEPTION("Read attempts exhausted")
            << InnerErrors_;
    }

    // Increments the error budget and sleeps.
    // Callers push any relevant errors into InnerErrors_ before calling.
    void AccountError()
    {
        ++ErrorAttempts_;
        Statistics_->ErrorAttemptCount.fetch_add(1, std::memory_order::relaxed);
        ErrorBackoffStrategy_.Next();
        TDelayedExecutor::WaitForDuration(ErrorBackoffStrategy_.GetBackoff());
    }

    void TryFetchChunkRecordCount()
    {
        YT_LOG_DEBUG(
            "Computing distributed chunk session chunk quorum info "
            "(ReadQuorum: %v, ReplicaCount: %v)",
            ReadQuorum_,
            Replicas_.size());

        Statistics_->ComputeQuorumInfoCount.fetch_add(1, std::memory_order::relaxed);

        auto quorumInfoOrError = WaitFor(ComputeQuorumInfo(
            ChunkId_,
            /*overlayed*/ false,
            ECodec::None,
            ReadQuorum_,
            Config_->ReplicaLagLimit,
            ToReplicaDescriptors(Replicas_),
            Config_->QuorumProbeTimeout,
            Client_->GetChannelFactory()));

        if (quorumInfoOrError.IsOK()) {
            ChunkRecordCount_ = quorumInfoOrError.Value().RowCount;
            CompressedDataSize_ = quorumInfoOrError.Value().CompressedDataSize;
            Statistics_->ComputeQuorumInfoSuccessCount.fetch_add(1, std::memory_order::relaxed);
            YT_LOG_DEBUG(
                "Computed distributed chunk session chunk quorum info "
                "(ChunkRecordCount: %v)",
                *ChunkRecordCount_);
            return;
        }

        YT_LOG_DEBUG(
            quorumInfoOrError,
            "Failed to compute distributed chunk session chunk quorum info "
            "(ReadQuorum: %v, ReplicaCount: %v)",
            ReadQuorum_,
            Replicas_.size());

        auto replicaUpdateResult = UpdateReplicasFromMaster();
        if (!replicaUpdateResult.IsOK()) {
            InnerErrors_.push_back(replicaUpdateResult);
        }
        InnerErrors_.push_back(quorumInfoOrError);
        AccountError();
    }

    void HandleAllNoSuchChunk(std::vector<TError> probeErrors)
    {
        auto replicaUpdateResult = UpdateReplicasFromMaster();
        if (replicaUpdateResult.IsOK() && replicaUpdateResult.Value()) {
            YT_LOG_DEBUG(
                "Switching distributed chunk session reader to final phase after replica set update "
                "(ReplicaCount: %v)",
                Replicas_.size());
            Phase_ = EPhase::Final;
            return;
        }
        for (auto& error : probeErrors) {
            InnerErrors_.push_back(std::move(error));
        }
        if (!replicaUpdateResult.IsOK()) {
            InnerErrors_.push_back(replicaUpdateResult);
        }
        AccountError();
    }

    std::optional<TChunkReadResult> ReadFromSingleReplica(TChunkReplica replica, i64 endIndex)
    {
        YT_VERIFY(endIndex > Cursor_);

        int blockCount = static_cast<int>(std::min(
            endIndex - Cursor_,
            static_cast<i64>(std::numeric_limits<int>::max())));

        YT_LOG_DEBUG(
            "Reading distributed chunk session records from single replica "
            "(Replica: %v, Cursor: %v, EndIndex: %v, BlockCount: %v)",
            replica,
            Cursor_,
            endIndex,
            blockCount);

        auto reader = MakeUnderlyingReader(TChunkReplicaList{replica});

        auto blocksOrError = WaitFor(reader->ReadBlocks(
            MakeReadOptions(),
            CheckedIntegralCast<int>(Cursor_),
            blockCount));

        Statistics_->ReadBlocksCount.fetch_add(1, std::memory_order::relaxed);

        if (blocksOrError.IsOK() && !blocksOrError.Value().empty()) {
            std::vector<TSharedRef> records;
            records.reserve(blocksOrError.Value().size());
            for (auto& block : blocksOrError.Value()) {
                records.push_back(std::move(block.Data));
            }

            // SetAllWritersFinished may have shrunk the effective end while the read was
            // in flight; trim so records past it are never delivered.
            i64 maxRecordCount = std::max<i64>(ComputeEffectiveEnd() - Cursor_, 0);
            if (std::ssize(records) > maxRecordCount) {
                records.resize(maxRecordCount);
            }

            Cursor_ += std::ssize(records);

            YT_LOG_DEBUG(
                "Read distributed chunk session records from single replica "
                "(RecordCount: %v, Cursor: %v, Finished: %v)",
                records.size(),
                Cursor_,
                Cursor_ >= ComputeEffectiveEnd());

            return TChunkReadResult{
                .Records = std::move(records),
                .Finished = Cursor_ >= ComputeEffectiveEnd(),
            };
        }

        if (!blocksOrError.IsOK()) {
            YT_LOG_DEBUG(
                blocksOrError,
                "Failed to read distributed chunk session records from single replica "
                "(Replica: %v, Cursor: %v, BlockCount: %v)",
                replica,
                Cursor_,
                blockCount);
            InnerErrors_.push_back(blocksOrError);
        } else {
            YT_LOG_DEBUG(
                "Received empty distributed chunk session read response from single replica "
                "(Replica: %v, Cursor: %v, BlockCount: %v)",
                replica,
                Cursor_,
                blockCount);
        }

        AccountError();

        return std::nullopt;
    }

    std::optional<TChunkReadResult> RunActivePhaseIteration()
    {
        auto replicaProgress = GetReplicaProgress();

        YT_LOG_DEBUG(
            "Completed distributed chunk session reader replica progress query "
            "(Outcome: %v, Cursor: %v, PickedRowCount: %v)",
            replicaProgress.Outcome,
            Cursor_,
            replicaProgress.PickedRowCount);

        switch (replicaProgress.Outcome) {
            case EReplicaProgressOutcome::HasData: {
                // Cap by the effective end, which folds in ChunkRecordCount if a
                // concurrent SetAllWritersFinished set it during the progress query.
                i64 endIndex = std::min(replicaProgress.PickedRowCount, ComputeEffectiveEnd());
                if (endIndex <= Cursor_) {
                    // The effective end is now at or below the cursor; let DoRead's
                    // top-of-loop check finish the read.
                    return std::nullopt;
                }
                return ReadFromSingleReplica(*replicaProgress.PickedReplica, endIndex);
            }
            case EReplicaProgressOutcome::Sealed:
                YT_LOG_DEBUG("Switching distributed chunk session reader to final phase after replica reported seal");
                Phase_ = EPhase::Final;
                return std::nullopt;
            case EReplicaProgressOutcome::NoNewData:
                Statistics_->PollIterationCount.fetch_add(1, std::memory_order::relaxed);
                TDelayedExecutor::WaitForDuration(Config_->PollInterval);
                return std::nullopt;
            case EReplicaProgressOutcome::AllNoSuchChunk:
                HandleAllNoSuchChunk(std::move(replicaProgress.InnerErrors));
                return std::nullopt;
            case EReplicaProgressOutcome::AllOtherErrors:
                for (auto& innerError : replicaProgress.InnerErrors) {
                    InnerErrors_.push_back(std::move(innerError));
                }
                AccountError();
                return std::nullopt;
        }
        YT_UNREACHABLE();
    }

    std::optional<TChunkReadResult> RunFinalPhaseIteration()
    {
        if (!ChunkRecordCount_) {
            TryFetchChunkRecordCount();
            return std::nullopt;
        }

        EnsurePrefetchStarted();

        // Read() is a consumer of the autonomous prefetch: return a ready batch (and free
        // a depth slot), park until it produces one, or report its terminal state.
        while (true) {
            if (!PrefetchError_.IsOK()) {
                THROW_ERROR PrefetchError_;
            }

            if (auto batch = TryDeliverBatch()) {
                MaybeWakePrefetch();
                return TChunkReadResult{
                    .Records = std::move(*batch),
                    .Finished = IsPrefetchDrained(),
                };
            }

            if (IsPrefetchDrained()) {
                return TChunkReadResult{.Records = {}, .Finished = true};
            }

            ReaderWakeupPromise_ = NewPromise<void>();
            WaitFor(ReaderWakeupPromise_.ToFuture())
                .ThrowOnError();
        }
    }

    void EnsurePrefetchStarted()
    {
        if (PrefetchStarted_) {
            return;
        }
        PrefetchStarted_ = true;
        FinalReader_ = MakeUnderlyingReader(Replicas_);
        ComputeBlocksPerRead();

        i64 begin = Cursor_;
        i64 end = ComputeEffectiveEnd();
        i64 totalRecordCount = end - begin;
        // Use at most one window per read's worth of records so a small range is not
        // split into reads smaller than SequentialReadSize.
        int windowCount = CheckedIntegralCast<int>(std::clamp<i64>(
            totalRecordCount / BlocksPerRead_,
            1,
            Config_->PrefetchWindowCount));
        Windows_.reserve(windowCount);
        for (int index = 0; index < windowCount; ++index) {
            i64 windowBegin = begin + totalRecordCount * index / windowCount;
            i64 windowEnd = begin + totalRecordCount * (index + 1) / windowCount;
            Windows_.push_back(TWindow{
                .NextReadIndex = windowBegin,
                .EndIndex = windowEnd,
                .InFlightEndIndex = windowBegin,
                .BackoffStrategy = TBackoffStrategy(Config_->ErrorBackoff),
            });
        }

        YT_LOG_DEBUG(
            "Started Phase-2 prefetch (Cursor: %v, EffectiveEnd: %v, WindowCount: %v, "
            "BlocksPerRead: %v, Depth: %v)",
            begin,
            end,
            windowCount,
            BlocksPerRead_,
            Config_->PrefetchDepth);

        // Kick off the autonomous prefetch; it runs on its own from here on.
        RunPrefetchStep(TError());
    }

    // Derives the per-read block count from SequentialReadSize and the average record
    // size. CompressedDataSize_ is known by now: ComputeQuorumInfo yields it on the usual
    // path, and SetAllWritersFinished supplies it when the record count is given directly.
    void ComputeBlocksPerRead()
    {
        YT_VERIFY(CompressedDataSize_);

        i64 recordCount = *ChunkRecordCount_;
        if (*CompressedDataSize_ > 0 && recordCount > 0) {
            i64 averageRecordSize = std::max<i64>(*CompressedDataSize_ / recordCount, 1);
            BlocksPerRead_ = std::clamp<i64>(
                Config_->SequentialReadSize / averageRecordSize,
                1,
                std::numeric_limits<int>::max());
        } else {
            BlocksPerRead_ = 1;
        }
    }

    void MaybeIssueRead(TWindow& window) noexcept
    {
        if (window.HasInFlightRead() ||
            window.RecoveryFuture ||
            window.NextReadIndex >= window.EndIndex ||
            window.OutstandingReadCount() >= Config_->PrefetchDepth)
        {
            return;
        }

        i64 begin = window.NextReadIndex;
        i64 end = std::min(begin + BlocksPerRead_, window.EndIndex);
        window.InFlightEndIndex = end;
        window.ReadEpoch = ReplicaEpoch_;
        window.InFlightFuture = FinalReader_->ReadBlocks(
            MakeReadOptions(),
            CheckedIntegralCast<int>(begin),
            CheckedIntegralCast<int>(end - begin));
        window.InFlightFuture.AsVoid().Subscribe(
            BIND_NO_PROPAGATE(&TDistributedChunkSessionReader::OnPrefetchProgress, MakeWeak(this))
                .Via(SerializedInvoker_));

        Statistics_->ReadBlocksCount.fetch_add(1, std::memory_order::relaxed);
    }

    // One step of the autonomous prefetch loop: harvest completed reads, rebalance, issue
    // new reads, wake a waiting reader, then reschedule on the next completion. All blocking
    // work (replica refresh, backoff) lives on per-window recovery fibers off this path.
    // MakeWeak rescheduling lets the reader die between steps without leaks.
    void RunPrefetchStep(const TError& /*error*/) noexcept
    {
        // The step must run atomically against Read() and the recovery fibers.
        TForbidContextSwitchGuard guard;

        for (auto& window : Windows_) {
            if (window.HasInFlightRead() && window.InFlightFuture.IsSet()) {
                ProcessCompletedRead(window);
            }
        }

        // A recovery fiber fails the prefetch (and wakes the reader) once a window's error
        // budget is exhausted; stop the chain in that case.
        if (!PrefetchError_.IsOK()) {
            return;
        }

        // Release windows whose recovery has finished so they can issue again.
        for (auto& window : Windows_) {
            if (window.RecoveryFuture && window.RecoveryFuture.IsSet()) {
                window.RecoveryFuture.Reset();
            }
        }

        for (auto& window : Windows_) {
            if (window.Exhausted()) {
                MaybeStealWork(window);
            }
        }

        for (auto& window : Windows_) {
            MaybeIssueRead(window);
        }

        if (AnyWindowHasReadyBatch()) {
            MaybeWakeReader();
        }

        // Once every window is exhausted there is no more data to read: stop the chain. Any
        // ready batches that remain are the consumer's to drain, and Read() detects final
        // drain itself.
        if (AllWindowsExhausted()) {
            MaybeWakeReader();
            return;
        }

        // Sleep until the next progress signal. Each in-flight read and each recovery pings
        // this wakeup on completion, and Read() pings it after freeing a depth slot, so one
        // promise covers every source.
        PrefetchWakeupPromise_ = NewPromise<void>();
        PrefetchWakeupPromise_.ToFuture().Subscribe(
            BIND_NO_PROPAGATE(&TDistributedChunkSessionReader::RunPrefetchStep, MakeWeak(this))
                .Via(SerializedInvoker_));
    }

    bool AnyWindowHasReadyBatch() const noexcept
    {
        for (const auto& window : Windows_) {
            if (!window.ReadyBatches.empty()) {
                return true;
            }
        }
        return false;
    }

    bool AllWindowsExhausted() const noexcept
    {
        for (const auto& window : Windows_) {
            if (!window.Exhausted()) {
                return false;
            }
        }
        return true;
    }

    // Wakes a reader parked in RunFinalPhaseIteration so it re-checks for a batch, drain,
    // or error.
    void MaybeWakeReader() noexcept
    {
        if (ReaderWakeupPromise_) {
            auto promise = std::move(ReaderWakeupPromise_);
            ReaderWakeupPromise_.Reset();
            promise.Set();
        }
    }

    // Schedules the next prefetch step. Safe to call from any progress source (a completed
    // read, a completed recovery, or Read() freeing a depth slot); calls that arrive before
    // the step runs coalesce into one.
    void MaybeWakePrefetch() noexcept
    {
        if (PrefetchWakeupPromise_) {
            auto promise = std::move(PrefetchWakeupPromise_);
            PrefetchWakeupPromise_.Reset();
            promise.Set();
        }
    }

    // Subscribed to each in-flight read and each recovery; pings the prefetch step so it
    // harvests the result on its own fiber.
    void OnPrefetchProgress(const TError& /*error*/) noexcept
    {
        MaybeWakePrefetch();
    }

    void FailPrefetch(TError error) noexcept
    {
        PrefetchError_ = std::move(error);
        MaybeWakeReader();
    }

    void ProcessCompletedRead(TWindow& window) noexcept
    {
        auto blocksOrError = window.InFlightFuture.GetOrCrash();
        window.InFlightFuture.Reset();

        // An empty but OK response is deliberately treated as a retryable failure: it
        // consumes the error budget instead of spinning forever against a replica that
        // never serves the requested tail.
        if (!blocksOrError.IsOK() || blocksOrError.Value().empty()) {
            StartWindowRecovery(window, blocksOrError);
            return;
        }

        std::vector<TSharedRef> records;
        records.reserve(blocksOrError.Value().size());
        for (const auto& block : blocksOrError.Value()) {
            records.push_back(block.Data);
        }

        // The node must not return more than the requested range. Fail the reader (no
        // retry) if it does: retrying would not help, and advancing past EndIndex would
        // corrupt the window.
        if (window.NextReadIndex + std::ssize(records) > window.InFlightEndIndex) {
            auto error = TError(
                "Chunk %v read returned more records than requested "
                "(ReturnedCount: %v, NextReadIndex: %v, RequestedEndIndex: %v)",
                ChunkId_,
                std::ssize(records),
                window.NextReadIndex,
                window.InFlightEndIndex);
            YT_LOG_ALERT(error);
            FailPrefetch(std::move(error));
            return;
        }

        // Advance by the number of records actually returned; a server-truncated read is
        // continued by the next read, leaving no sliver.
        window.NextReadIndex += std::ssize(records);
        Statistics_->PrefetchWindowCount.fetch_add(1, std::memory_order::relaxed);
        window.ReadyBatches.push_back(std::move(records));

        // Progress clears the window's consecutive-failure budget: the budget bounds reads
        // that fail in a row, not the lifetime sum of transient failures separated by
        // successful reads.
        window.ErrorAttempts = 0;
        window.BackoffStrategy.Restart();
    }

    // Kicks off a window's recovery on a separate fiber and returns at once, so the
    // prefetch step never blocks on the replica refresh or the backoff. The window issues
    // no reads until the recovery finishes; its NextReadIndex is unchanged, so it simply
    // retries the failed read.
    void StartWindowRecovery(TWindow& window, const TErrorOr<std::vector<TBlock>>& blocksOrError) noexcept
    {
        if (!blocksOrError.IsOK()) {
            YT_LOG_DEBUG(
                blocksOrError,
                "Failed to read Phase-2 prefetch window "
                "(BeginIndex: %v, EndIndex: %v)",
                window.NextReadIndex,
                window.InFlightEndIndex);
        } else {
            YT_LOG_DEBUG(
                "Received empty Phase-2 prefetch window response "
                "(BeginIndex: %v, EndIndex: %v)",
                window.NextReadIndex,
                window.InFlightEndIndex);
        }

        Statistics_->PrefetchRetryCount.fetch_add(1, std::memory_order::relaxed);

        TError readError = blocksOrError.IsOK()
            ? TError("Prefetch read returned an empty response")
            : TError(blocksOrError);

        int windowIndex = static_cast<int>(&window - Windows_.data());
        window.RecoveryFuture = BIND_NO_PROPAGATE(
            &TDistributedChunkSessionReader::RecoverWindow,
            MakeWeak(this),
            windowIndex,
            std::move(readError))
            .AsyncVia(SerializedInvoker_)
            .Run();
        window.RecoveryFuture.Subscribe(
            BIND_NO_PROPAGATE(&TDistributedChunkSessionReader::OnPrefetchProgress, MakeWeak(this))
                .Via(SerializedInvoker_));
    }

    // Runs on its own fiber: refreshes replicas and waits out the backoff without blocking
    // the prefetch step. The retry budget is per window, independent of the Phase-1 budget
    // (which DoRead resets on every call) and of sibling windows, so a window that keeps
    // failing still terminates the prefetch.
    void RecoverWindow(int windowIndex, TError readError) noexcept
    {
        auto& window = Windows_[windowIndex];

        // Refresh replicas at most once per epoch: if a sibling recovery already refreshed
        // after this window's read was issued (epoch advanced), reuse the result instead of
        // querying master again. The epoch is bumped before the yielding fetch so concurrent
        // recoveries observe the claim and skip it.
        if (window.ReadEpoch == ReplicaEpoch_) {
            ++ReplicaEpoch_;
            auto replicaUpdateResult = UpdateReplicasFromMaster();
            if (replicaUpdateResult.IsOK() && replicaUpdateResult.Value()) {
                // Replica set changed: rebuild the reader so this and later reads use the
                // new replicas.
                FinalReader_ = MakeUnderlyingReader(Replicas_);
            }
        }

        // Check the budget before backing off so the final attempt fails fast.
        ++window.ErrorAttempts;
        if (window.ErrorAttempts >= Config_->MaxReadAttempts) {
            FailPrefetch(TError("Phase-2 prefetch read attempts exhausted (AttemptCount: %v)",
                window.ErrorAttempts)
                << std::move(readError));
            return;
        }

        window.BackoffStrategy.Next();
        TDelayedExecutor::WaitForDuration(window.BackoffStrategy.GetBackoff());

        // NextReadIndex is unchanged, so the window retries this read once the prefetch step
        // observes the finished recovery.
    }

    // A window that exhausted its range steals the upper half of the window with the
    // largest remaining range, provided that half is worth at least one read.
    void MaybeStealWork(TWindow& idleWindow) noexcept
    {
        TWindow* victim = nullptr;
        i64 maxUnreadCount = 0;
        for (auto& window : Windows_) {
            if (&window == &idleWindow) {
                continue;
            }
            i64 unreadCount = window.UnreadCount();
            if (unreadCount > maxUnreadCount) {
                maxUnreadCount = unreadCount;
                victim = &window;
            }
        }

        i64 stolenCount = maxUnreadCount / 2;
        if (!victim || stolenCount < BlocksPerRead_) {
            return;
        }

        i64 victimEnd = victim->EndIndex;
        i64 splitIndex = victimEnd - stolenCount;
        victim->EndIndex = splitIndex;
        idleWindow.NextReadIndex = splitIndex;
        idleWindow.InFlightEndIndex = splitIndex;
        idleWindow.EndIndex = victimEnd;

        YT_LOG_DEBUG(
            "Prefetch window stole work (StolenCount: %v, SplitIndex: %v)",
            stolenCount,
            splitIndex);
    }

    // Returns the next batch to deliver: the window holding the most ready batches, with
    // round-robin tie-breaking so no window is favored.
    std::optional<std::vector<TSharedRef>> TryDeliverBatch()
    {
        int windowCount = std::ssize(Windows_);
        int bestIndex = -1;
        i64 bestReadyCount = 0;
        for (int offset = 1; offset <= windowCount; ++offset) {
            int index = (LastDeliveredWindowIndex_ + offset) % windowCount;
            i64 readyCount = std::ssize(Windows_[index].ReadyBatches);
            if (readyCount > bestReadyCount) {
                bestReadyCount = readyCount;
                bestIndex = index;
            }
        }

        if (bestIndex < 0) {
            return std::nullopt;
        }

        LastDeliveredWindowIndex_ = bestIndex;
        auto& window = Windows_[bestIndex];
        auto records = std::move(window.ReadyBatches.front());
        window.ReadyBatches.pop_front();
        Cursor_ += std::ssize(records);
        return records;
    }

    bool IsPrefetchDrained() const noexcept
    {
        for (const auto& window : Windows_) {
            if (window.HasInFlightRead() ||
                !window.ReadyBatches.empty() ||
                window.NextReadIndex < window.EndIndex)
            {
                return false;
            }
        }
        return true;
    }

    std::vector<TChunkReplicaDescriptor> ToReplicaDescriptors(
        const TChunkReplicaList& replicas) const
    {
        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();

        std::vector<TChunkReplicaDescriptor> result;
        result.reserve(replicas.size());
        for (auto replica : replicas) {
            result.push_back({
                .NodeDescriptor = nodeDirectory->GetDescriptor(replica),
                .ReplicaIndex = replica.GetReplicaIndex(),
            });
        }

        return result;
    }

    IChunkReaderPtr MakeUnderlyingReader(const TChunkReplicaList& seedReplicas)
    {
        auto options = New<TRemoteReaderOptions>();

        options->AllowFetchingSeedsFromMaster = false;

        return CreateReplicationReader(
            Config_->UnderlyingReaderConfig,
            std::move(options),
            ChunkReaderHost_,
            ChunkId_,
            seedReplicas);
    }

    static IChunkReader::TReadBlocksOptions MakeReadOptions()
    {
        return IChunkReader::TReadBlocksOptions{
            .ClientOptions = {
                .ReadSessionId = TGuid::Create(),
            },
        };
    }

    TReplicaProgress GetReplicaProgress()
    {
        Statistics_->ActiveReplicaProgressQueryCount.fetch_add(1, std::memory_order::relaxed);

        const auto& connection = Client_->GetNativeConnection();
        const auto& nodeDirectory = connection->GetNodeDirectory();
        const auto& channelFactory = Client_->GetChannelFactory();
        const auto& networks = connection->GetNetworks();

        std::vector<TFuture<TDataNodeServiceProxy::TRspGetChunkMetaPtr>> futures;
        futures.reserve(Replicas_.size());
        for (const auto& replica : Replicas_) {
            const auto& descriptor = nodeDirectory->GetDescriptor(replica);
            TDataNodeServiceProxy proxy(
                channelFactory->CreateChannel(descriptor.GetAddressOrThrow(networks)));
            proxy.SetDefaultTimeout(Config_->ProbeTimeout);
            auto req = proxy.GetChunkMeta();
            ToProto(req->mutable_chunk_id(), ChunkId_);
            req->set_all_extension_tags(false);
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            NRpc::SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));
            futures.push_back(req->Invoke());
        }

        auto result = WaitFor(AnySetMatching(
            std::move(futures),
            [cursor = Cursor_] (const TDataNodeServiceProxy::TErrorOrRspGetChunkMetaPtr& rspOrError) noexcept {
                if (!rspOrError.IsOK()) {
                    return false;
                }
                auto miscExt = FindProtoExtension<TMiscExt>(
                    rspOrError.Value()->chunk_meta().extensions());
                return miscExt && (miscExt->sealed() || miscExt->row_count() > cursor);
            }))
            .ValueOrThrow();

        if (result.MatchingIndex) {
            const auto replicaIndex = *result.MatchingIndex;
            auto miscExt = GetProtoExtension<TMiscExt>(
                result.Results[replicaIndex]->Value()->chunk_meta().extensions());
            if (miscExt.sealed()) {
                Statistics_->SealedDetectedCount.fetch_add(1, std::memory_order::relaxed);
                return TReplicaProgress{
                    .Outcome = EReplicaProgressOutcome::Sealed,
                };
            }
            return TReplicaProgress{
                .Outcome = EReplicaProgressOutcome::HasData,
                .PickedReplica = Replicas_[replicaIndex],
                .PickedRowCount = miscExt.row_count(),
            };
        }

        // All responses settled with no useful data — classify by error type.
        int successCount = 0;
        int noSuchChunkCount = 0;
        std::vector<TError> innerErrors;
        for (const auto& rspOrError : result.Results) {
            if (rspOrError->IsOK()) {
                ++successCount;
            } else {
                if (rspOrError->FindMatching(NChunkClient::EErrorCode::NoSuchChunk)) {
                    ++noSuchChunkCount;
                }
                innerErrors.push_back(*rspOrError);
            }
        }

        EReplicaProgressOutcome outcome;
        if (successCount > 0) {
            outcome = EReplicaProgressOutcome::NoNewData;
        } else if (noSuchChunkCount == std::ssize(result.Results)) {
            outcome = EReplicaProgressOutcome::AllNoSuchChunk;
        } else {
            outcome = EReplicaProgressOutcome::AllOtherErrors;
        }

        return TReplicaProgress{
            .Outcome = outcome,
            .InnerErrors = std::move(innerErrors),
        };
    }

    // NB(apollo1321): this method can overload master at cluster scale.
    // v1 callers don't gate it: NoSuchChunk gating, per-reader rate limit,
    // and a separate master backoff are deferred.
    TErrorOr<bool> UpdateReplicasFromMaster()
    {
        YT_LOG_DEBUG(
            "Updating distributed chunk session reader replicas from master (ReplicaCount: %v)",
            Replicas_.size());

        IChannelPtr channel;
        try {
            channel = Client_->GetMasterChannelOrThrow(
                EMasterChannelKind::Follower,
                CellTagFromId(ChunkId_));
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(
                TError(ex),
                "Failed to acquire master channel while updating distributed chunk session reader replicas");
            return TError("Failed to acquire master channel for chunk %v", ChunkId_)
                << TError(ex);
        }

        TChunkServiceProxy proxy(std::move(channel));
        proxy.SetDefaultTimeout(Config_->RefreshTimeout);
        auto req = proxy.LocateChunks();
        ToProto(req->add_subrequests(), ChunkId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(
                rspOrError,
                "Failed to locate chunk while updating distributed chunk session reader replicas");
            return TError("Failed to locate chunk %v on master", ChunkId_)
                << rspOrError;
        }
        const auto& rsp = rspOrError.Value();

        Client_->GetNativeConnection()->GetNodeDirectory()->MergeFrom(rsp->node_directory());

        YT_VERIFY(rsp->subresponses_size() == 1);
        const auto& subresponse = rsp->subresponses(0);
        if (subresponse.missing()) {
            YT_LOG_DEBUG("Chunk is missing on master while updating distributed chunk session reader replicas");
            return TError(NChunkClient::EErrorCode::NoSuchChunk,
                "Chunk %v is missing on master", ChunkId_);
        }

        auto newReplicas = FromProto<TChunkReplicaList>(subresponse.replicas());

        auto toSortedKeys = [] (const TChunkReplicaList& replicas) {
            std::vector<std::pair<TNodeId, int>> result;
            result.reserve(replicas.size());
            for (const auto& replica : replicas) {
                result.emplace_back(replica.GetNodeId(), replica.GetReplicaIndex());
            }
            std::sort(result.begin(), result.end());
            return result;
        };

        bool replicasChanged = toSortedKeys(newReplicas) != toSortedKeys(Replicas_);
        int oldReplicaCount = std::ssize(Replicas_);

        Replicas_ = std::move(newReplicas);

        Statistics_->MasterRefreshCount.fetch_add(1, std::memory_order::relaxed);

        YT_LOG_DEBUG(
            "Updated distributed chunk session reader replicas from master "
            "(ReplicasChanged: %v, OldReplicaCount: %v, NewReplicaCount: %v)",
            replicasChanged,
            oldReplicaCount,
            Replicas_.size());

        return replicasChanged;
    }
};

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionReaderPtr CreateDistributedChunkSessionReader(
    TDistributedChunkSessionReaderConfigPtr config,
    IClientPtr client,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaList replicas,
    int readQuorum,
    i64 startRecordIndex,
    std::optional<i64> rangeEndRecordIndex,
    IInvokerPtr invoker)
{
    return New<TDistributedChunkSessionReader>(
        std::move(config),
        std::move(client),
        std::move(chunkReaderHost),
        chunkId,
        std::move(replicas),
        readQuorum,
        startRecordIndex,
        rangeEndRecordIndex,
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
