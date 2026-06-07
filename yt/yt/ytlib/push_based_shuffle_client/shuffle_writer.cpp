#include "shuffle_writer.h"

#include "config.h"
#include "record_format.h"
#include "session_provider.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_writer.h>

#include <yt/yt/ytlib/table_client/partitioner.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <deque>

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NTableClient;

using TCreateDistributedChunkWriterCallback = TCallback<IDistributedChunkWriterPtr(TNodeDescriptor, TSessionId)>;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TShuffleWireRecordTag { };

struct TRecordEntry
{
    TSharedRef Record;
    int SendAttempts = 0;
};

struct TInFlightEntry
{
    TRecordEntry Entry;
    TSessionId SessionId;
};

struct TPartitionState
{
    std::optional<TShuffleRecordBuilder> Builder;
    std::deque<TRecordEntry> Pending;
    THashMap<i64, TInFlightEntry> InFlight;
    // Per-send-attempt key for InFlight; a record re-sent after retry gets a
    // new cookie, so a late-arriving ack for the old cookie no-ops in OnWriteResponse.
    i64 NextSendCookie = 0;

    std::optional<TSessionDescriptor> Session;
    IDistributedChunkWriterPtr Writer;

    TFuture<TSessionDescriptor> PendingSessionFuture;

    i64 NextRowId = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleWriter
    : public IPushBasedShuffleWriter
{
public:
    TPushBasedShuffleWriter(
        TShuffleWriterConfigPtr config,
        IPartitionWriteSessionProviderPtr sessionProvider,
        IPartitionerPtr partitioner,
        TCreateDistributedChunkWriterCallback createDistributedChunkWriter,
        i32 mapperId,
        IInvokerPtr invoker,
        THashMap<int, TSessionDescriptor> seededSessions)
        : Config_(std::move(config))
        , SessionProvider_(std::move(sessionProvider))
        , Partitioner_(std::move(partitioner))
        , CreateDistributedChunkWriter_(std::move(createDistributedChunkWriter))
        , MapperId_(mapperId)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(PushBasedShuffleLogger().WithTag("MapperId: %v", MapperId_))
        , Partitions_(Partitioner_->GetPartitionCount())
        , SeededSessions_(std::move(seededSessions))
        , BuildersBudget_(static_cast<i64>(Config_->MemoryBudget * Config_->BuildersBudgetFraction))
        , InFlightBudget_(Config_->MemoryBudget - BuildersBudget_)
    {
        YT_VERIFY(BuildersBudget_ > 0);
        YT_VERIFY(InFlightBudget_ > 0);
        YT_LOG_INFO(
            "Push-based shuffle writer created "
            "(PartitionCount: %v, MemoryBudget: %v, BuildersBudget: %v, InFlightBudget: %v, MaxSendAttempts: %v)",
            Partitions_.size(),
            Config_->MemoryBudget,
            BuildersBudget_,
            InFlightBudget_,
            Config_->MaxSendAttempts);
    }

    TFuture<void> Write(TRange<TUnversionedRow> rows) override
    {
        auto promise = NewPromise<void>();
        SerializedInvoker_->Invoke(BIND(
            [this, this_ = MakeStrong(this), rows, promise] () mutable {
                try {
                    DoWrite(rows, promise);
                } catch (const std::exception& ex) {
                    // DoWrite can throw on malformed input. Surface the error
                    // on the returned future (unless DoWrite already moved
                    // the promise into BackpressurePromise_) and fail the writer.
                    auto error = TError(ex);
                    if (promise) {
                        promise.TrySet(error);
                    }
                    FailWriter(error);
                }
            }));
        // Caller-cancel would auto-set the shared BackpressurePromise_ state
        // and trip YT_VERIFY on the next backpressured Write.
        return promise.ToFuture().ToUncancelable();
    }

    TFuture<void> Close() override
    {
        return BIND(&TPushBasedShuffleWriter::DoClose, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

private:
    const TShuffleWriterConfigPtr Config_;
    const IPartitionWriteSessionProviderPtr SessionProvider_;
    const IPartitionerPtr Partitioner_;
    const TCreateDistributedChunkWriterCallback CreateDistributedChunkWriter_;
    const i32 MapperId_;
    const IInvokerPtr SerializedInvoker_;
    const TLogger Logger;

    std::vector<TPartitionState> Partitions_;
    THashSet<int> NonEmptyPartitions_;
    // Sessions handed out by RegisterMapper, consumed (erased) on a partition's first session
    // request; failovers and unseeded partitions go through the provider.
    THashMap<int, TSessionDescriptor> SeededSessions_;

    const i64 BuildersBudget_;
    const i64 InFlightBudget_;
    i64 BuildersBytes_ = 0;
    i64 InFlightBytes_ = 0;

    // Aggregate count of "things the writer is still waiting on" — records
    // (whether in Pending or InFlight) plus outstanding session requests.
    // Maintained by FlushBuilder / RequestSession (increments) and the
    // terminal OnWriteResponse branches plus OnSessionResolved (decrements). Avoids
    // an O(partition_count) scan in MaybeFinishClose on every ack.
    i64 OutstandingWork_ = 0;

    TPromise<void> BackpressurePromise_;

    bool Closing_ = false;
    TPromise<void> ClosePromise_;

    TError TerminalError_;

    void DoWrite(TRange<TUnversionedRow> rows, TPromise<void>& writePromise)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(!Closing_);

        if (!TerminalError_.IsOK()) {
            writePromise.Set(TerminalError_);
            return;
        }

        for (auto row : rows) {
            int partitionIndex = Partitioner_->GetPartitionIndex(row);
            YT_VERIFY(partitionIndex >= 0 && partitionIndex < std::ssize(Partitions_));
            auto& partitionState = Partitions_[partitionIndex];
            if (!partitionState.Builder) {
                partitionState.Builder.emplace(MapperId_, partitionState.NextRowId);
                NonEmptyPartitions_.insert(partitionIndex);
            }
            i64 prev = partitionState.Builder->GetAllocatedDataSize();
            partitionState.Builder->AddRow(row);
            i64 delta = partitionState.Builder->GetAllocatedDataSize() - prev;
            if (delta > 0) {
                // Eviction runs per-row (only when allocation actually grew)
                // rather than once at end-of-batch. Without this, a Write with
                // many partitions can allocate `partition_count *
                // initial_reserve` worth of fresh builders before eviction
                // sees the overflow, exceeding MemoryBudget by orders of
                // magnitude.
                BuildersBytes_ += delta;
                while (BuildersBytes_ > BuildersBudget_ && !NonEmptyPartitions_.empty()) {
                    EvictLargest();
                }
            }
        }

        // Proactively request sessions for non-empty partitions so the session
        // is ready by the time Close flushes the builder.
        for (int partitionIndex : NonEmptyPartitions_) {
            auto& partitionState = Partitions_[partitionIndex];
            if (!partitionState.Session && !partitionState.PendingSessionFuture) {
                RequestSession(partitionIndex, /*excluded*/ std::nullopt);
            }
        }

        if (InFlightBytes_ > InFlightBudget_) {
            YT_VERIFY(!BackpressurePromise_);
            BackpressurePromise_ = std::move(writePromise);
            YT_LOG_DEBUG(
                "Backpressure engaged (InFlightBytes: %v, InFlightBudget: %v)",
                InFlightBytes_,
                InFlightBudget_);
            return;
        }
        writePromise.Set();
    }

    TFuture<void> DoClose()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK()) {
            return MakeFuture(TerminalError_);
        }
        if (Closing_) {
            return ClosePromise_.ToFuture();
        }
        Closing_ = true;
        ClosePromise_ = NewPromise<void>();

        YT_LOG_INFO(
            "Closing shuffle writer (NonEmptyPartitions: %v)",
            NonEmptyPartitions_.size());

        std::vector<int> nonEmptySnapshot(NonEmptyPartitions_.begin(), NonEmptyPartitions_.end());
        for (int partitionIndex : nonEmptySnapshot) {
            FlushBuilder(partitionIndex);
        }

        MaybeFinishClose();
        return ClosePromise_.ToFuture();
    }

    void EvictLargest()
    {
        // TODO(apollo1321): O(|NonEmptyPartitions_|) linear scan over the
        // partitions currently buffering rows. Replace with a max-heap keyed
        // on builder.GetAllocatedDataSize() later.
        int best = -1;
        i64 bestSize = -1;
        for (int partitionIndex : NonEmptyPartitions_) {
            i64 size = Partitions_[partitionIndex].Builder->GetAllocatedDataSize();
            if (size > bestSize) {
                bestSize = size;
                best = partitionIndex;
            }
        }
        YT_VERIFY(best >= 0);
        FlushBuilder(best);
    }

    void FlushBuilder(int partitionIndex)
    {
        auto& partitionState = Partitions_[partitionIndex];
        i64 prevAllocation = partitionState.Builder->GetAllocatedDataSize();
        auto record = partitionState.Builder->FlushRecord();
        YT_VERIFY(record);
        BuildersBytes_ -= prevAllocation;
        partitionState.NextRowId += record->Header.RowCount;
        partitionState.Builder.reset();
        NonEmptyPartitions_.erase(partitionIndex);

        // TODO(apollo1321): IDistributedChunkWriter::WriteRecord currently
        // takes a single TSharedRef, forcing a payload-sized memcpy here.
        // Switch WriteRecord to TRange<TSharedRef> and drop the merge.
        auto compressed = MergeRefsToRef<TShuffleWireRecordTag>(
            CompressShuffleRecord(*record, Config_->Codec));
        InFlightBytes_ += compressed.Size();
        partitionState.Pending.push_back({.Record = std::move(compressed), .SendAttempts = 0});
        ++OutstandingWork_;

        if (partitionState.Session) {
            DrainPending(partitionIndex);
        } else if (!partitionState.PendingSessionFuture) {
            RequestSession(partitionIndex, /*excluded*/ std::nullopt);
        }
    }

    void DrainPending(int partitionIndex)
    {
        auto& partitionState = Partitions_[partitionIndex];
        YT_VERIFY(partitionState.Session && partitionState.Writer);
        while (!partitionState.Pending.empty()) {
            auto entry = std::move(partitionState.Pending.front());
            partitionState.Pending.pop_front();
            if (entry.SendAttempts >= Config_->MaxSendAttempts) {
                --OutstandingWork_;
                FailWriter(TError("Failed to write shuffle record after exceeding max send attempts")
                    << TErrorAttribute("mapper_id", MapperId_)
                    << TErrorAttribute("partition_index", partitionIndex)
                    << TErrorAttribute("send_attempts", entry.SendAttempts));
                return;
            }
            ++entry.SendAttempts;
            i64 cookie = partitionState.NextSendCookie++;
            auto sessionId = partitionState.Session->SessionId;
            auto writeFuture = partitionState.Writer->WriteRecord(entry.Record);
            partitionState.InFlight[cookie] = TInFlightEntry{
                .Entry = std::move(entry),
                .SessionId = sessionId,
            };
            writeFuture.Subscribe(BIND_NO_PROPAGATE(&TPushBasedShuffleWriter::OnWriteResponse, MakeStrong(this), partitionIndex, cookie)
                .Via(SerializedInvoker_));
        }
    }

    void RequestSession(int partitionIndex, std::optional<TSessionId> excluded)
    {
        auto& partitionState = Partitions_[partitionIndex];
        YT_VERIFY(!partitionState.PendingSessionFuture);

        // The initial (non-excluded) request for a partition uses its RegisterMapper-seeded
        // session if present; failovers and unseeded partitions go through the provider.
        if (!excluded) {
            auto seedIt = SeededSessions_.find(partitionIndex);
            if (seedIt != SeededSessions_.end()) {
                partitionState.PendingSessionFuture = MakeFuture(seedIt->second);
                SeededSessions_.erase(seedIt);
            }
        }
        if (!partitionState.PendingSessionFuture) {
            YT_LOG_DEBUG(
                "Requesting session (PartitionIndex: %v, ExcludedSessionId: %v)",
                partitionIndex,
                excluded);
            partitionState.PendingSessionFuture = SessionProvider_->GetSession(partitionIndex, excluded);
        }
        ++OutstandingWork_;
        partitionState.PendingSessionFuture.Subscribe(BIND_NO_PROPAGATE(
            &TPushBasedShuffleWriter::OnSessionResolved,
            MakeStrong(this),
            partitionIndex,
            excluded)
            .Via(SerializedInvoker_));
    }

    void OnSessionResolved(
        int partitionIndex,
        std::optional<TSessionId> excluded,
        const TErrorOr<TSessionDescriptor>& sessionOrError)
    {
        auto& partitionState = Partitions_[partitionIndex];
        partitionState.PendingSessionFuture.Reset();
        --OutstandingWork_;
        // Reachable when an earlier callback on the serialized invoker
        // called FailWriter before this OnSessionResolved ran.
        if (!TerminalError_.IsOK()) {
            return;
        }
        if (!sessionOrError.IsOK()) {
            FailWriter(TError("Failed to acquire session for partition")
                << TErrorAttribute("mapper_id", MapperId_)
                << TErrorAttribute("partition_index", partitionIndex)
                << static_cast<const TError&>(sessionOrError));
            return;
        }
        partitionState.Session = sessionOrError.Value();
        partitionState.Writer = CreateDistributedChunkWriter_.Run(
            partitionState.Session->SequencerNode,
            partitionState.Session->SessionId);
        YT_LOG_DEBUG(
            "Session resolved (PartitionIndex: %v, SessionId: %v)",
            partitionIndex,
            partitionState.Session->SessionId);

        // Sweep order is THashMap iteration order — non-deterministic. The
        // writer makes no in-order delivery guarantee per partition (retries
        // already break ordering), and the sequencer accepts arbitrary order
        // from a single mapper.
        if (excluded) {
            std::vector<i64> toErase;
            for (auto& [cookie, inFlight] : partitionState.InFlight) {
                if (inFlight.SessionId == *excluded) {
                    toErase.push_back(cookie);
                    partitionState.Pending.push_back(std::move(inFlight.Entry));
                }
            }
            for (i64 cookie : toErase) {
                partitionState.InFlight.erase(cookie);
            }
        }
        DrainPending(partitionIndex);
        MaybeFinishClose();
    }

    void OnWriteResponse(int partitionIndex, i64 cookie, const TError& error)
    {
        auto& partitionState = Partitions_[partitionIndex];
        auto inFlightIt = partitionState.InFlight.find(cookie);
        if (inFlightIt == partitionState.InFlight.end()) {
            return;
        }
        auto inFlight = std::move(inFlightIt->second);
        partitionState.InFlight.erase(inFlightIt);

        if (!TerminalError_.IsOK()) {
            // Reachable when an earlier callback on the serialized invoker
            // called FailWriter before this OnWriteResponse ran. Drop the
            // bytes accounting; nothing else is observable post-failure.
            InFlightBytes_ -= inFlight.Entry.Record.Size();
            --OutstandingWork_;
            return;
        }

        if (error.IsOK()) {
            InFlightBytes_ -= inFlight.Entry.Record.Size();
            --OutstandingWork_;
            MaybeReleaseBackpressure();
            MaybeFinishClose();
            return;
        }

        if (inFlight.Entry.SendAttempts >= Config_->MaxSendAttempts) {
            --OutstandingWork_;
            FailWriter(TError("Failed to write shuffle record after exceeding max send attempts")
                << TErrorAttribute("mapper_id", MapperId_)
                << TErrorAttribute("partition_index", partitionIndex)
                << TErrorAttribute("send_attempts", inFlight.Entry.SendAttempts)
                << error);
            return;
        }

        if (partitionState.Session && partitionState.Session->SessionId == inFlight.SessionId) {
            YT_LOG_DEBUG(
                error,
                "Retiring session after write failure (PartitionIndex: %v, SessionId: %v)",
                partitionIndex,
                inFlight.SessionId);
            auto excluded = partitionState.Session->SessionId;
            partitionState.Session.reset();
            partitionState.Writer.Reset();
            RequestSession(partitionIndex, excluded);
        }
        partitionState.Pending.push_back(std::move(inFlight.Entry));
        if (partitionState.Session) {
            DrainPending(partitionIndex);
        }
    }

    void MaybeReleaseBackpressure()
    {
        if (BackpressurePromise_ && InFlightBytes_ <= InFlightBudget_) {
            YT_LOG_DEBUG(
                "Backpressure released (InFlightBytes: %v, InFlightBudget: %v)",
                InFlightBytes_,
                InFlightBudget_);
            auto promise = std::move(BackpressurePromise_);
            BackpressurePromise_.Reset();
            promise.Set();
        }
    }

    void MaybeFinishClose()
    {
        if (!Closing_ || ClosePromise_.IsSet() || OutstandingWork_ > 0) {
            return;
        }
        YT_LOG_INFO("Shuffle writer closed");
        ClosePromise_.Set();
    }

    void FailWriter(const TError& error)
    {
        if (!TerminalError_.IsOK()) {
            return;
        }
        YT_LOG_WARNING(error, "Shuffle writer failed");
        TerminalError_ = error;
        if (BackpressurePromise_) {
            auto promise = std::move(BackpressurePromise_);
            BackpressurePromise_.Reset();
            promise.Set(TerminalError_);
        }
        // ClosePromise_ is default-constructed (empty) until DoClose runs. If
        // FailWriter fires before Close, ClosePromise_ stays empty here and a
        // later Close picks up TerminalError_ via DoClose's early return.
        if (ClosePromise_ && !ClosePromise_.IsSet()) {
            ClosePromise_.Set(TerminalError_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IPushBasedShuffleWriterPtr CreatePushBasedShuffleWriter(
    TShuffleWriterConfigPtr config,
    IPartitionWriteSessionProviderPtr sessionProvider,
    IPartitionerPtr partitioner,
    NApi::NNative::IConnectionPtr connection,
    i32 mapperId,
    IInvokerPtr invoker,
    THashMap<int, TSessionDescriptor> seededSessions)
{
    auto writerConfig = config->WriterConfig;
    auto createWriter = BIND([connection, writerConfig] (TNodeDescriptor sequencerNode, TSessionId sessionId) {
        return CreateDistributedChunkWriter(
            sequencerNode,
            sessionId,
            connection,
            writerConfig);
    });

    return New<TPushBasedShuffleWriter>(
        std::move(config),
        std::move(sessionProvider),
        std::move(partitioner),
        std::move(createWriter),
        mapperId,
        std::move(invoker),
        std::move(seededSessions));
}

IPushBasedShuffleWriterPtr CreatePushBasedShuffleWriterForTesting(
    TShuffleWriterConfigPtr config,
    IPartitionWriteSessionProviderPtr sessionProvider,
    IPartitionerPtr partitioner,
    TCreateDistributedChunkWriterCallback createDistributedChunkWriter,
    i32 mapperId,
    IInvokerPtr invoker)
{
    return New<TPushBasedShuffleWriter>(
        std::move(config),
        std::move(sessionProvider),
        std::move(partitioner),
        std::move(createDistributedChunkWriter),
        mapperId,
        std::move(invoker),
        THashMap<int, TSessionDescriptor>{});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
