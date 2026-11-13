#include "writer_resource.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <deque>
#include <ranges>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto MaxWriterIdleTime = TDuration::Seconds(40);
constexpr auto EmptyPartitionReportDelay = TDuration::Seconds(30);

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStaticTableExportWriterResource::TWriter
    : public TRefCounted
{
public:
    TWriter(TStaticTableExportWriterParametersPtr parameters, const NLogging::TLogger& logger, const NProfiling::TProfiler& profiler)
        : Parameters_(std::move(parameters))
        , Logger(logger)
        , Profiler_(profiler)
        , ControlQueue_(New<TActionQueue>("StaticExport"))
        , IOQueue_(New<TActionQueue>("StaticExportIO"))
        , Invoker_(CreateSerializedInvoker(ControlQueue_->GetInvoker()))
    { }

    ~TWriter() override
    {
        TDelayedExecutor::CancelAndClear(WakeupCookie_);
    }

    IStaticTableExportParticipantPtr RegisterPartition(
        TStaticTableExportBinding binding,
        TStaticTableExportClaim claim,
        IInvokerPtr poolInvoker)
    {
        auto partition = New<TPartition>(claim, std::move(poolInvoker));
        Invoker_->Invoke(BIND(&TWriter::Register, MakeStrong(this), std::move(binding), partition));
        return New<TParticipant>(MakeStrong(this), std::move(partition));
    }

    void Schedule()
    {
        Invoker_->Invoke(BIND(&TWriter::Tick, MakeWeak(this)));
    }

private:
    struct TRequest
    {
        TStaticTableExportMessage Metadata;
        TOutputMessageConstPtr Message;
        TOnDistributedCallback Callback;
        TPromise<void> Accepted = NewPromise<void>();
    };

    using TRequestPtr = std::shared_ptr<TRequest>;

    struct TPartition
        : public TRefCounted
    {
        const TStaticTableExportClaim Claim;
        const IInvokerPtr PoolInvoker;
        //! Owned by the partition, so a parser bound to its job lives no longer than the job.
        TStaticTableExportRowParser ParseRows;
        std::atomic<bool> Canceled = false;

        // Snapshot for the job thread.
        YT_DECLARE_SPIN_LOCK(TSpinLock, Lock);
        TError Error;
        TStaticTableExportPartitionState State;

        // Writer invoker state.
        bool Claimed = false;
        //! The latest claim of the partition found no export state.
        bool Unbootstrapped = false;
        bool Draining = false;
        bool HandingOff = false;
        const TPromise<void> Handoff = NewPromise<void>();
        std::deque<TRequestPtr> Queue;
        //! Accepted requests form the prefix of #Queue.
        i64 AcceptedCount = 0;
        int StreamCount = 0;
        i64 AdmittedCount = 0;
        i64 DurableNextOffset = 0;
        std::optional<ui64> DurableHighestWindow;
        i64 NextOffset = 0;
        std::optional<ui64> HighestWindow;
        std::optional<TSystemTimestamp> FirstWriteTimestamp;
        //! Start of the latest period the job reads without draining.
        TInstant RegisteredAt = TInstant::Now();

        TPartition(TStaticTableExportClaim claim, IInvokerPtr poolInvoker)
            : Claim(claim)
            , PoolInvoker(std::move(poolInvoker))
        { }

        bool IsHealthy()
        {
            auto guard = Guard(Lock);
            return !Canceled.load() && Error.IsOK();
        }
    };

    using TPartitionPtr = TIntrusivePtr<TPartition>;

    struct TRecord
    {
        TPartitionPtr Partition;
        TRequestPtr Request;
    };

    struct TParsed
    {
        TSharedRange<TUnversionedRow> Rows;
        i64 MemoryUsage = 0;
        i64 RawSize = 0;
    };

    struct TStream
        : public TRefCounted
    {
        const ui64 Window;
        TStaticTableExportWindowStreamPtr Handle;
        bool Opened = false;
        bool Closed = false;
        bool Working = false;
        bool Canceled = false;
        //! The transaction is abandoned; the retained records are rewritten into a new one.
        bool Restarting = false;
        int Attempt = 0;
        //! Distinguishes parse results of the current transaction from abandoned ones.
        int Epoch = 0;
        TInstant RetryAt;
        std::optional<TInstant> ResolveDeadline;
        TInstant OpenedAt;
        TInstant LastInputAt;
        std::optional<std::string> CloseReason;
        std::vector<TRecord> Records;
        std::map<int, TStaticTableExportDelta> Deltas;
        std::deque<int> Unparsed;
        std::vector<TParsed> Parsed;
        i64 WrittenCount = 0;
        //! Raw size of the records not yet written in the current transaction.
        i64 UnwrittenBytes = 0;
        bool WriterOpen = false;
        TInstant LastWriteAt;

        explicit TStream(ui64 window)
            : Window(window)
        { }
    };

    using TStreamPtr = TIntrusivePtr<TStream>;

    class TParticipant
        : public IStaticTableExportParticipant
    {
    public:
        TParticipant(TIntrusivePtr<TWriter> writer, TPartitionPtr partition)
            : Writer_(std::move(writer))
            , Partition_(std::move(partition))
        { }

        ~TParticipant() override
        {
            Partition_->Canceled.store(true);
            Writer_->Schedule();
        }

        void Enqueue(TStaticTableExportMessage metadata, TOutputMessageConstPtr message, TOnDistributedCallback callback) override
        {
            auto request = std::make_shared<TRequest>(TRequest{
                .Metadata = metadata,
                .Message = std::move(message),
                .Callback = std::move(callback),
            });
            auto futureAccepted = request->Accepted.ToFuture();
            Writer_->Invoker_->Invoke(BIND(&TWriter::Enqueue, Writer_, Partition_, std::move(request)));
            WaitFor(futureAccepted)
                .ThrowOnError();
        }

        void SetDraining(bool draining) override
        {
            Writer_->Invoker_->Invoke(BIND([writer = Writer_, partition = Partition_, draining] {
                if (partition->Draining && !draining) {
                    partition->RegisteredAt = TInstant::Now();
                }
                partition->Draining = draining;
                writer->Schedule();
            }));
        }

        TFuture<void> PrepareForHandoff() override
        {
            Writer_->Invoker_->Invoke(BIND(&TWriter::PrepareForHandoff, Writer_, Partition_));
            return Partition_->Handoff.ToFuture();
        }

        void ThrowOnError() const override
        {
            auto guard = Guard(Partition_->Lock);
            Partition_->Error.ThrowOnError();
        }

        TStaticTableExportPartitionState GetState() const override
        {
            auto guard = Guard(Partition_->Lock);
            return Partition_->State;
        }

    private:
        const TIntrusivePtr<TWriter> Writer_;
        const TPartitionPtr Partition_;
    };

    const TStaticTableExportWriterParametersPtr Parameters_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;
    const NProfiling::TGauge RawBytes_ = Profiler_.Gauge("/raw_bytes");
    const NProfiling::TGauge OpenWindows_ = Profiler_.Gauge("/open_windows");
    const NProfiling::TCounter DurableBytes_ = Profiler_.Counter("/durable_bytes");
    const NProfiling::TCounter RejectedRecords_ = Profiler_.Counter("/rejected_records");
    const NProfiling::TCounter Resets_ = Profiler_.Counter("/resets");
    const NProfiling::TCounter StreamRetries_ = Profiler_.Counter("/stream_retries");
    const NProfiling::TCounter FencedPartitions_ = Profiler_.Counter("/fenced_partitions");
    const TActionQueuePtr ControlQueue_;
    const TActionQueuePtr IOQueue_;
    const IInvokerPtr Invoker_;

    std::optional<TStaticTableExportBinding> Binding_;
    TStaticTableExportWriterStoragePtr Storage_;
    TNameTablePtr NameTable_;
    std::vector<TPartitionPtr> Partitions_;
    std::map<ui64, TStreamPtr> Streams_;
    //! Refused opens of a window back off exponentially, like stream retries.
    std::map<ui64, std::pair<TInstant, int>> OpenRetryAfter_;
    std::optional<ui64> ClosedThrough_;
    i64 RawDataWeight_ = 0;
    i64 ParsedDataWeight_ = 0;
    bool Claiming_ = false;
    bool Refreshing_ = false;
    bool BandBlocked_ = false;
    TInstant RefreshAfter_;
    TInstant RetryAfter_;
    TInstant ClaimRetryAfter_;
    std::optional<TError> ResetError_;
    bool ResetReading_ = false;
    TDelayedExecutorCookie WakeupCookie_;

    void Register(TStaticTableExportBinding binding, TPartitionPtr partition)
    {
        partition->ParseRows = std::exchange(binding.ParseRows, {});
        try {
            if (!Binding_) {
                NameTable_ = TNameTable::FromSchema(*binding.Schema);
                Storage_ = New<TStaticTableExportWriterStorage>(
                    binding.Client,
                    binding.Parameters,
                    binding.Schema,
                    binding.Owner,
                    binding.SourceIdentity,
                    Parameters_->TableWriterConfig,
                    Parameters_->TableWriterCount,
                    Profiler_);
                Binding_ = std::move(binding);
                ScheduleWakeup();
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(Binding_->SourceIdentity == binding.SourceIdentity && AreNodesEqual(Binding_->Owner, binding.Owner),
                    "Static table export writer is shared by different exports");
            }
            Partitions_.push_back(partition);
        } catch (const std::exception& ex) {
            SetError(partition, TError(ex));
        }
        Tick();
    }

    void ScheduleWakeup()
    {
        WakeupCookie_ = TDelayedExecutor::Submit(
            BIND([weakThis = MakeWeak(this)] {
                if (auto this_ = weakThis.Lock()) {
                    this_->Tick();
                    this_->ScheduleWakeup();
                }
            }),
            TDuration::MilliSeconds(200),
            Invoker_);
    }

    void SetError(const TPartitionPtr& partition, TError error)
    {
        {
            auto guard = Guard(partition->Lock);
            if (!partition->Error.IsOK()) {
                return;
            }
            partition->Error = std::move(error)
                .With("partition", partition->Claim.Partition)
                .With("generation", partition->Claim.Generation);
        }

        YT_TLOG_WARNING("Static table export partition failed")
            .With(partition->Error);
        DropQueue(partition, partition->Error);
        partition->Handoff.TrySet(partition->Error);
    }

    void DropQueue(const TPartitionPtr& partition, const TError& error)
    {
        for (const auto& request : partition->Queue) {
            RawDataWeight_ -= request->Message->ByteSize;
            request->Accepted.TrySet(error);
        }
        partition->Queue.clear();
        partition->AcceptedCount = 0;
    }

    void Enqueue(const TPartitionPtr& partition, TRequestPtr request)
    {
        if (!partition->IsHealthy() || partition->HandingOff) {
            request->Accepted.TrySet(TError("Static table export participant is not accepting messages"));
            return;
        }
        RawDataWeight_ += request->Message->ByteSize;
        if (!partition->FirstWriteTimestamp) {
            partition->FirstWriteTimestamp = request->Metadata.WriteTimestamp;
        }
        partition->Queue.push_back(request);
        Tick();
    }

    void PrepareForHandoff(const TPartitionPtr& partition)
    {
        partition->HandingOff = true;
        // The successor replays every unadmitted message from the source.
        DropQueue(partition, TError("Static table export participant is handing off"));
        CloseStreamsOf(partition, "handoff");
        Tick();
    }

    void CloseStreamsOf(const TPartitionPtr& partition, const std::string& reason)
    {
        // An earlier stream sharing any partition must commit first, so it is closed as well.
        THashSet<int> partitions{partition->Claim.Partition};
        for (const auto& [_, stream] : Streams_ | std::views::reverse) {
            if (std::ranges::none_of(stream->Deltas, [&] (const auto& item) {
                    return partitions.contains(item.first);
                })) {
                continue;
            }
            if (!stream->CloseReason) {
                stream->CloseReason = reason;
            }
            for (const auto& [index, _] : stream->Deltas) {
                partitions.insert(index);
            }
        }
    }

    bool HasStreamsOfIndex(int index) const
    {
        return std::ranges::any_of(Partitions_, [&] (const auto& partition) {
            return partition->Claim.Partition == index && partition->StreamCount > 0;
        });
    }

    //! Runs blocking storage IO off the writer invoker and handles its outcome on it.
    template <class TAction, class THandler>
    void RunIO(TAction action, THandler handler)
    {
        using TResult = std::invoke_result_t<TAction>;
        BIND(std::move(action))
            .AsyncVia(IOQueue_->GetInvoker())
            .Run()
            .Subscribe(BIND([this, this_ = MakeStrong(this), handler = std::move(handler)] (const TErrorOr<TResult>& resultOrError) mutable {
                handler(resultOrError);
                Tick();
            }).Via(Invoker_));
    }

    //! A failed write or close rewrites the stream; a failed commit is resolved from the cursors.
    template <class TAction, class TOnSuccess>
    void Launch(const TStreamPtr& stream, TAction action, TOnSuccess onSuccess, std::function<void()> onDone = {})
    {
        using TResult = std::invoke_result_t<TAction>;
        YT_VERIFY(!stream->Working);
        stream->Working = true;
        RunIO(std::move(action), [this, stream, onSuccess = std::move(onSuccess), onDone = std::move(onDone)] (const TErrorOr<TResult>& resultOrError) mutable {
            stream->Working = false;
            if (onDone) {
                onDone();
            }
            if (ResetError_ || stream->Canceled) {
                return;
            }
            if constexpr (std::is_same_v<TResult, TStaticTableExportCursors>) {
                if (resultOrError.IsOK()) {
                    onSuccess(resultOrError.Value());
                } else {
                    ResolveCommit(stream, TError(resultOrError));
                }
            } else if (stream->Restarting) {
                // The stream lost a partition while the action ran; its transaction is rewritten anyway.
            } else if (resultOrError.IsOK()) {
                onSuccess();
            } else {
                RestartStream(stream, TError(resultOrError));
            }
        });
    }

    void RestartStream(const TStreamPtr& stream, const TError& error)
    {
        StreamRetries_.Increment();
        ++stream->Attempt;
        const auto backoff = std::min(Parameters_->RetryBackoff * (1 << std::min(stream->Attempt - 1, 10)), TDuration::Seconds(30));
        YT_TLOG_INFO("Retrying static table export stream")
            .With("Window", stream->Window)
            .With("Attempt", stream->Attempt)
            .With("Backoff", backoff)
            .With(error);
        MarkRestarting(stream);
        stream->RetryAt = TInstant::Now() + backoff;
    }

    //! Abandons the transaction and schedules every retained record to be parsed and written again.
    void MarkRestarting(const TStreamPtr& stream)
    {
        stream->Restarting = true;
        stream->Closed = false;
        stream->WriterOpen = false;
        stream->ResolveDeadline.reset();
        ++stream->Epoch;
        ReleaseParsed(stream);
        stream->WrittenCount = 0;
        stream->Unparsed.clear();
        stream->UnwrittenBytes = 0;
        for (int index = 0; index < std::ssize(stream->Records); ++index) {
            stream->Unparsed.push_back(index);
            stream->UnwrittenBytes += stream->Records[index].Request->Message->ByteSize;
        }
    }

    void ReleaseParsed(const TStreamPtr& stream)
    {
        for (const auto& item : stream->Parsed) {
            ParsedDataWeight_ -= item.MemoryUsage;
        }
        stream->Parsed.clear();
    }

    //! Stops the work of |stream|; its transaction is aborted once no action of it runs.
    void CancelStream(const TStreamPtr& stream)
    {
        stream->Canceled = true;
        ++stream->Epoch;
        stream->Unparsed.clear();
        ReleaseParsed(stream);
    }

    void Reopen(const TStreamPtr& stream)
    {
        if (stream->Handle) {
            Storage_->AbortStream(stream->Handle);
            stream->Handle.Reset();
        }
        stream->Working = true;
        const auto deadline = TInstant::Now() + Parameters_->MaxStreamAge + Parameters_->CommitTimeout;
        RunIO(
            [storage = Storage_, window = stream->Window, timeout = Parameters_->TransactionTimeout, deadline] {
                return storage->OpenStream(window, timeout, deadline);
            },
            [this, stream] (const TErrorOr<std::pair<TStaticTableExportWindowStreamPtr, TStaticTableExportProgressPtr>>& resultOrError) {
                stream->Working = false;
                if (ResetError_ || stream->Canceled) {
                    return;
                }
                if (!resultOrError.IsOK() || !resultOrError.Value().first) {
                    const auto& progress = resultOrError.IsOK() ? resultOrError.Value().second : nullptr;
                    if (progress && progress->ClosedThrough >= stream->Window) {
                        Reset(TError("Static table export window was published while its records were unacknowledged")
                                .With("window", stream->Window));
                    } else {
                        RestartStream(stream, TError("Failed to reopen static table export stream").With(TError(resultOrError)));
                    }
                    return;
                }
                stream->Handle = resultOrError.Value().first;
                stream->Restarting = false;
                stream->OpenedAt = TInstant::Now();
                YT_TLOG_DEBUG("Reopened static table export stream")
                    .With("Window", stream->Window)
                    .With("RecordCount", stream->Records.size());
            });
    }

    //! A commit reply may be lost after the commit happened, so its outcome is read from the locked cursors.
    void ResolveCommit(const TStreamPtr& stream, const TError& error)
    {
        if (!stream->ResolveDeadline) {
            stream->ResolveDeadline = TInstant::Now() + Parameters_->CommitTimeout;
            // Aborting releases the cursor key locks the failed commit took; it cannot undo a commit that happened.
            Storage_->AbortStream(stream->Handle);
        }
        std::vector<TStaticTableExportDelta> deltas;
        std::vector<int> partitions;
        for (const auto& [partition, delta] : stream->Deltas) {
            deltas.push_back(delta);
            partitions.push_back(partition);
        }
        stream->Working = true;
        RunIO(
            [storage = Storage_, partitions = std::move(partitions), timeout = Parameters_->TransactionTimeout] {
                return storage->ReadCursors(partitions, timeout);
            },
            [this, stream, error, deltas = std::move(deltas)] (const TErrorOr<TStaticTableExportCursors>& cursorsOrError) {
                stream->Working = false;
                // A stream that lost a partition meanwhile is rewritten, and its next commit resolves the outcome again.
                if (ResetError_ || stream->Canceled || stream->Restarting) {
                    return;
                }
                if (!cursorsOrError.IsOK()) {
                    // Durable progress stays unknown; only a writer-wide reset can rebuild it later.
                    if (TInstant::Now() > *stream->ResolveDeadline) {
                        Reset(TError("Failed to resolve a static table export commit")
                                .With("window", stream->Window)
                                .With(error)
                                .With(TError(cursorsOrError)));
                    } else {
                        // Stay busy so the stream is not committed again meanwhile.
                        stream->Working = true;
                        TDelayedExecutor::Submit(
                            BIND([this, this_ = MakeStrong(this), stream, error] {
                                stream->Working = false;
                                if (!ResetError_ && !stream->Canceled) {
                                    ResolveCommit(stream, error);
                                }
                            }),
                            Parameters_->RetryBackoff,
                            Invoker_);
                    }
                    return;
                }
                const auto& cursors = cursorsOrError.Value();
                std::vector<int> fenced;
                const auto outcome = ClassifyStaticTableExportCommit(stream->Window, deltas, cursors, &fenced);
                for (int index : fenced) {
                    for (const auto& record : stream->Records) {
                        if (record.Partition->Claim.Partition == index) {
                            DropPartition(record.Partition, TError("Static table export partition is owned by a newer job").With("owner_generation", GetOrCrash(cursors, index)->Generation));
                            break;
                        }
                    }
                }

                switch (outcome) {
                    case EStaticTableExportCommitOutcome::Committed:
                        Complete(stream, cursors);
                        break;

                    case EStaticTableExportCommitOutcome::NotCommitted:
                        if (!stream->Canceled) {
                            RestartStream(stream, error);
                        }
                        break;

                    case EStaticTableExportCommitOutcome::Unknown:
                        // Cursors match neither side of this commit, so the durable prefix is unknown.
                        Reset(TError("Static table export commit outcome is unknown")
                                .With("window", stream->Window)
                                .With(error));
                        break;
                }
            });
    }

    //! Fails only |partition|: its records leave every stream, which are then rewritten without them.
    //! Takes |partition| by value since callers may pass a reference into the records it erases.
    void DropPartition(TPartitionPtr partition, TError error)
    {
        FencedPartitions_.Increment();
        SetError(partition, std::move(error));
        for (const auto& [_, stream] : Streams_) {
            if (!stream->Deltas.erase(partition->Claim.Partition)) {
                continue;
            }
            std::erase_if(stream->Records, [&] (const auto& record) {
                if (record.Partition != partition) {
                    return false;
                }
                RawDataWeight_ -= record.Request->Message->ByteSize;
                return true;
            });
            if (stream->Records.empty()) {
                CancelStream(stream);
            } else {
                MarkRestarting(stream);
            }
        }
        partition->StreamCount = 0;
        partition->AdmittedCount = 0;
    }

    void Tick()
    {
        if (!Storage_) {
            return;
        }
        RemoveCanceled();
        AcceptWithinBudget();
        if (ResetError_) {
            TryFinishReset();
        } else if (TInstant::Now() >= RetryAfter_) {
            Claim();
            RefreshProgress();
            ScheduleCloses();
            Admit();
            SubmitParsing();
            DriveStreams();
        }
        AcceptWithinBudget();
        UpdateStates();
    }

    void RemoveCanceled()
    {
        for (auto it = Partitions_.begin(); it != Partitions_.end();) {
            const auto& partition = *it;
            if (!partition->Canceled.load()) {
                ++it;
                continue;
            }
            DropQueue(partition, TError("Static table export participant is canceled"));
            partition->Handoff.TrySet(TError("Static table export participant is canceled"));
            // Committing its admitted records early lets the successor claim without fencing them.
            CloseStreamsOf(partition, "canceled");
            if (partition->StreamCount > 0) {
                ++it;
            } else {
                it = Partitions_.erase(it);
            }
        }
    }

    void Claim()
    {
        if (Claiming_ || TInstant::Now() < ClaimRetryAfter_) {
            return;
        }
        std::vector<TPartitionPtr> partitions;
        std::vector<TStaticTableExportClaim> claims;
        for (const auto& partition : Partitions_) {
            // A previous job of the same partition must finish its streams before the successor claims.
            if (!partition->Claimed && partition->IsHealthy() && !HasStreamsOfIndex(partition->Claim.Partition)) {
                partitions.push_back(partition);
                claims.push_back(partition->Claim);
            }
        }
        if (claims.empty()) {
            return;
        }
        Claiming_ = true;
        RunIO(
            [storage = Storage_, claims = std::move(claims), timeout = Parameters_->TransactionTimeout] {
                return storage->Claim(claims, timeout);
            },
            [this, partitions = std::move(partitions)] (const TErrorOr<std::pair<TStaticTableExportProgressPtr, TStaticTableExportCursors>>& resultOrError) {
                Claiming_ = false;
                if (!resultOrError.IsOK()) {
                    YT_TLOG_DEBUG("Static table export claim failed; will retry")
                        .With(resultOrError);
                    ClaimRetryAfter_ = TInstant::Now() + Parameters_->RetryBackoff;
                    return;
                }
                const auto& [progress, cursors] = resultOrError.Value();
                for (const auto& partition : partitions) {
                    partition->Unbootstrapped = !progress;
                }
                if (!progress) {
                    ClaimRetryAfter_ = TInstant::Now() + Parameters_->RetryBackoff;
                    return;
                }
                UpdateClosedThrough(progress->ClosedThrough);
                for (const auto& partition : partitions) {
                    const auto& cursor = GetOrCrash(cursors, partition->Claim.Partition);
                    if (cursor->Generation != partition->Claim.Generation) {
                        SetError(partition, TError("Static table export partition is owned by a newer job").With("owner_generation", cursor->Generation));
                        continue;
                    }
                    partition->Claimed = true;
                    ApplyDurable(partition, cursor);
                }
            });
    }

    void ApplyDurable(const TPartitionPtr& partition, const TStaticTableExportCursorPtr& cursor)
    {
        partition->DurableNextOffset = cursor->NextOffset;
        partition->DurableHighestWindow = cursor->HighestWindow;
        if (partition->StreamCount == 0) {
            partition->NextOffset = cursor->NextOffset;
            partition->HighestWindow = cursor->HighestWindow;
        }
    }

    void UpdateClosedThrough(ui64 closedThrough)
    {
        if (ClosedThrough_ && closedThrough <= *ClosedThrough_) {
            return;
        }
        ClosedThrough_ = closedThrough;
        BandBlocked_ = false;
        std::erase_if(OpenRetryAfter_, [&] (const auto& item) {
            return item.first <= closedThrough;
        });
    }

    void RefreshProgress()
    {
        const bool drainingUnpublished = std::ranges::any_of(Partitions_, [&] (const auto& partition) {
            return partition->Draining && IsUnpublished(partition);
        });
        if (Refreshing_ || !(BandBlocked_ || drainingUnpublished) || TInstant::Now() < RefreshAfter_) {
            return;
        }
        Refreshing_ = true;
        RefreshAfter_ = TInstant::Now() + Parameters_->ProgressRefreshPeriod;
        RunIO(
            [storage = Storage_] {
                return storage->ReadProgress();
            },
            [this] (const TErrorOr<TStaticTableExportProgressPtr>& progressOrError) {
                Refreshing_ = false;
                if (progressOrError.IsOK() && progressOrError.Value()) {
                    UpdateClosedThrough(progressOrError.Value()->ClosedThrough);
                }
            });
    }

    bool IsUnpublished(const TPartitionPtr& partition) const
    {
        return partition->DurableHighestWindow && (!ClosedThrough_ || *partition->DurableHighestWindow > *ClosedThrough_);
    }

    void ScheduleCloses()
    {
        const auto now = TInstant::Now();
        const bool draining = std::ranges::any_of(Partitions_, [] (const auto& partition) {
            return partition->Draining;
        });
        const auto idlePeriod = draining ? std::min(Parameters_->IdleFlushPeriod, TDuration::Seconds(1)) : Parameters_->IdleFlushPeriod;
        // A stream gets no input while the writer holds admission back, which does not make it idle.
        const bool admissionBlocked = GetUnwrittenBytes() >= Parameters_->MaxUnwrittenDataWeight;
        bool closing = false;
        for (const auto& [_, stream] : Streams_) {
            if (stream->Opened && !stream->CloseReason) {
                if (now >= stream->OpenedAt + Parameters_->MaxStreamAge) {
                    stream->CloseReason = "age";
                } else if (!admissionBlocked && stream->WrittenCount == std::ssize(stream->Records) && now >= stream->LastInputAt + idlePeriod) {
                    stream->CloseReason = "idle";
                }
            }
            closing |= stream->CloseReason.has_value();
        }
        if (RawDataWeight_ > Parameters_->MaxRawDataWeight && !closing) {
            // The oldest stream with records is always committable, so closing it frees payload budget.
            for (const auto& [_, stream] : Streams_) {
                if (stream->Opened && !stream->Records.empty()) {
                    stream->CloseReason = "pressure";
                    break;
                }
            }
        }
    }

    i64 GetUnwrittenBytes() const
    {
        i64 unwrittenBytes = 0;
        for (const auto& [_, stream] : Streams_) {
            unwrittenBytes += stream->UnwrittenBytes;
        }
        return unwrittenBytes;
    }

    void Admit()
    {
        if (!ClosedThrough_) {
            return;
        }
        // A stream closes only after its records are written, so the unwritten backlog bounds the close time.
        auto unwrittenBytes = GetUnwrittenBytes();
        // Round robin keeps partitions of one writer at a similar pace under the budget.
        bool admitted = true;
        while (admitted && unwrittenBytes < Parameters_->MaxUnwrittenDataWeight) {
            admitted = false;
            for (const auto& partition : Partitions_) {
                if (!partition->Claimed || partition->HandingOff || !partition->IsHealthy() ||
                    partition->AcceptedCount == 0)
                {
                    continue;
                }
                // An acknowledged replay releases its payload, so the size is taken first.
                const auto size = partition->Queue.front()->Message->ByteSize;
                if (TryAdmit(partition, partition->Queue.front())) {
                    unwrittenBytes += size;
                    partition->Queue.pop_front();
                    --partition->AcceptedCount;
                    admitted = true;
                }
            }
        }
    }

    bool TryAdmit(const TPartitionPtr& partition, const TRequestPtr& request)
    {
        const auto offset = request->Metadata.Offset;
        if (offset < partition->DurableNextOffset) {
            // A replayed prefix is already durable; acknowledge it after the earlier admitted records.
            if (partition->StreamCount > 0) {
                return false;
            }
            Acknowledge({partition, request});
            return true;
        }
        if (offset < partition->NextOffset) {
            // An admitted offset delivered again would block the queue forever; replay from the cursor instead.
            SetError(partition, TError("Static table export received an offset that is already admitted").With("offset", offset).With("next_offset", partition->NextOffset));
            return false;
        }
        const auto window = AssignStaticTableExportWindow(
            request->Metadata.WriteTimestamp,
            partition->HighestWindow,
            *ClosedThrough_,
            Binding_->Parameters);
        if (!window) {
            BandBlocked_ = true;
            return false;
        }
        auto it = Streams_.find(*window);
        if (it == Streams_.end()) {
            OpenStream(*window);
            return false;
        }
        const auto& stream = it->second;
        if (!stream->Opened || stream->CloseReason || stream->Canceled) {
            return false;
        }
        auto [deltaIt, inserted] = stream->Deltas.try_emplace(partition->Claim.Partition);
        auto& delta = deltaIt->second;
        if (inserted) {
            delta.Partition = partition->Claim.Partition;
            delta.Generation = partition->Claim.Generation;
            // A partition that never committed starts at its first delivered offset, not at zero.
            delta.FirstOffset = partition->HighestWindow ? partition->NextOffset : offset;
            ++partition->StreamCount;
        }
        delta.NextOffset = offset + 1;
        partition->NextOffset = offset + 1;
        partition->HighestWindow = *window;
        ++partition->AdmittedCount;
        stream->Unparsed.push_back(stream->Records.size());
        stream->UnwrittenBytes += request->Message->ByteSize;
        stream->Records.push_back({partition, request});
        stream->LastInputAt = TInstant::Now();
        return true;
    }

    void OpenStream(ui64 window)
    {
        if (auto it = OpenRetryAfter_.find(window); it != OpenRetryAfter_.end() && TInstant::Now() < it->second.first) {
            return;
        }
        auto stream = New<TStream>(window);
        EmplaceOrCrash(Streams_, window, stream);
        OpenWindows_.Update(Streams_.size());
        const auto deadline = TInstant::Now() + Parameters_->MaxStreamAge + Parameters_->CommitTimeout;
        stream->Working = true;
        RunIO(
            [storage = Storage_, window, timeout = Parameters_->TransactionTimeout, deadline] {
                return storage->OpenStream(window, timeout, deadline);
            },
            [this, stream] (const TErrorOr<std::pair<TStaticTableExportWindowStreamPtr, TStaticTableExportProgressPtr>>& resultOrError) {
                stream->Working = false;
                if (stream->Canceled) {
                    return;
                }
                const auto [handle, progress] = resultOrError.IsOK()
                    ? resultOrError.Value()
                    : std::pair<TStaticTableExportWindowStreamPtr, TStaticTableExportProgressPtr>{};
                if (progress) {
                    UpdateClosedThrough(progress->ClosedThrough);
                }
                if (!handle) {
                    // Nothing was admitted yet: the gate is published, beyond the band, busy or unreachable.
                    YT_TLOG_DEBUG_UNLESS(resultOrError.IsOK(), "Failed to open static table export stream")
                        .With("Window", stream->Window)
                        .With(TError(resultOrError));
                    auto& [retryAt, attempt] = OpenRetryAfter_[stream->Window];
                    retryAt = TInstant::Now() + Parameters_->RetryBackoff * (1 << std::min(attempt, 3));
                    ++attempt;
                    EraseOrCrash(Streams_, stream->Window);
                    OpenWindows_.Update(Streams_.size());
                    return;
                }
                OpenRetryAfter_.erase(stream->Window);
                stream->Handle = handle;
                stream->Opened = true;
                stream->OpenedAt = stream->LastInputAt = TInstant::Now();
            });
    }

    void SubmitParsing()
    {
        for (const auto& [_, stream] : Streams_) {
            // Retained parsed rows are written without more parsing, and an empty budget still admits an oversized record.
            while (!stream->Canceled && !stream->Unparsed.empty() && ParsedDataWeight_ < Parameters_->MaxParsedDataWeight) {
                const auto& record = stream->Records[stream->Unparsed.front()];
                stream->Unparsed.pop_front();
                // Reserve the raw size until the actual row memory is known.
                const auto reserved = record.Request->Message->ByteSize;
                ParsedDataWeight_ += reserved;
                BIND(record.Partition->ParseRows, record.Request->Message, NameTable_, record.Partition->Claim.Partition)
                    .AsyncVia(record.Partition->PoolInvoker)
                    .Run()
                    .Subscribe(BIND(&TWriter::OnParsed, MakeStrong(this), stream, stream->Epoch, record, reserved)
                            .Via(Invoker_));
            }
        }
    }

    void OnParsed(const TStreamPtr& stream, int epoch, const TRecord& record, i64 reserved, const TErrorOr<TStaticTableExportParsedRows>& resultOrError)
    {
        ParsedDataWeight_ -= reserved;
        if (stream->Canceled || stream->Epoch != epoch || ResetError_) {
            Tick();
            return;
        }
        if (!resultOrError.IsOK()) {
            DropPartition(record.Partition, TError("Static table export failed to parse a source message").With("offset", record.Request->Metadata.Offset).With(TError(resultOrError)));
            Tick();
            return;
        }
        const auto& rows = resultOrError.Value();
        RejectedRecords_.Increment(rows.RejectedRecords);
        ParsedDataWeight_ += rows.MemoryUsage;
        stream->Parsed.push_back({
            .Rows = rows.Rows,
            .MemoryUsage = rows.MemoryUsage,
            .RawSize = record.Request->Message->ByteSize,
        });
        Tick();
    }

    bool IsCommitEligible(const TStreamPtr& stream) const
    {
        for (const auto& [window, other] : Streams_) {
            if (window >= stream->Window) {
                break;
            }
            for (const auto& [partition, _] : stream->Deltas) {
                if (other->Deltas.contains(partition)) {
                    return false;
                }
            }
        }
        return true;
    }

    void DriveStreams()
    {
        for (const auto& [_, stream] : Streams_) {
            if (!stream->Opened || stream->Working || stream->Canceled) {
                continue;
            }
            if (stream->Restarting) {
                if (TInstant::Now() >= stream->RetryAt) {
                    Reopen(stream);
                }
            } else if (!stream->Parsed.empty()) {
                auto parsed = std::exchange(stream->Parsed, {});
                std::vector<TSharedRange<TUnversionedRow>> rows;
                for (const auto& item : parsed) {
                    rows.push_back(item.Rows);
                }
                i64 memoryUsage = 0;
                i64 rawSize = 0;
                for (const auto& item : parsed) {
                    memoryUsage += item.MemoryUsage;
                    rawSize += item.RawSize;
                }
                Launch(
                    stream,
                    [storage = Storage_,
                        handle = stream->Handle,
                        nameTable = NameTable_,
                        rows = std::move(rows),
                        timeout = Parameters_->IOTimeout,
                        backlogged = GetUnwrittenBytes() >= Parameters_->MaxUnwrittenDataWeight] {
                        storage->WriteStream(handle, nameTable, rows, timeout, backlogged);
                    },
                    [stream, count = std::ssize(parsed), rawSize] {
                        stream->WrittenCount += count;
                        stream->UnwrittenBytes -= rawSize;
                        stream->WriterOpen = true;
                        stream->LastWriteAt = TInstant::Now();
                    },
                    [this, memoryUsage] {
                        ParsedDataWeight_ -= memoryUsage;
                    });
            } else if (stream->CloseReason && !stream->Closed && stream->WrittenCount == std::ssize(stream->Records)) {
                YT_TLOG_INFO("Closing static table export stream")
                    .With("Window", stream->Window)
                    .With("Reason", *stream->CloseReason)
                    .With("RecordCount", stream->Records.size())
                    .With("Age", TInstant::Now() - stream->OpenedAt);
                Launch(
                    stream,
                    [storage = Storage_, handle = stream->Handle, timeout = Parameters_->IOTimeout] {
                        storage->CloseStream(handle, timeout);
                    },
                    [stream] {
                        stream->Closed = true;
                        stream->WriterOpen = false;
                    });
            } else if (stream->WriterOpen && TInstant::Now() >= stream->LastWriteAt + MaxWriterIdleTime) {
                // The proxy fails a table write that receives no data for a minute, so a starved writer finishes its chunk.
                YT_TLOG_INFO("Closing idle static table export table writer")
                    .With("Window", stream->Window)
                    .With("RecordCount", stream->Records.size());
                Launch(
                    stream,
                    [storage = Storage_, handle = stream->Handle, timeout = Parameters_->IOTimeout] {
                        storage->CloseStream(handle, timeout);
                    },
                    [stream] {
                        stream->WriterOpen = false;
                    });
            } else if (stream->Closed && stream->Records.empty()) {
                CancelStream(stream);
            } else if (stream->Closed && IsCommitEligible(stream)) {
                std::vector<TStaticTableExportDelta> deltas;
                for (const auto& [_, delta] : stream->Deltas) {
                    deltas.push_back(delta);
                }
                Launch(
                    stream,
                    [storage = Storage_, handle = stream->Handle, deltas = std::move(deltas), closedThrough = ClosedThrough_.value_or(0), backoff = Parameters_->RetryBackoff] {
                        return storage->CommitStream(handle, deltas, closedThrough, backoff);
                    },
                    [this, stream] (const TStaticTableExportCursors& cursors) {
                        Complete(stream, cursors);
                    });
            }
        }
        std::erase_if(Streams_, [&] (const auto& item) {
            const auto& stream = item.second;
            if (!stream->Canceled || stream->Working) {
                return false;
            }
            if (stream->Handle) {
                Storage_->AbortStream(stream->Handle);
            }
            return true;
        });
        OpenWindows_.Update(Streams_.size());
        for (const auto& partition : Partitions_) {
            if (partition->HandingOff && partition->StreamCount == 0) {
                partition->Handoff.TrySet();
            }
        }
    }

    void Complete(const TStreamPtr& stream, const TStaticTableExportCursors& cursors)
    {
        // A partition dropped during the commit restarted the stream and may have resubmitted its parsing.
        CancelStream(stream);
        EraseOrCrash(Streams_, stream->Window);
        OpenWindows_.Update(Streams_.size());
        for (const auto& partition : GetStreamPartitions(stream)) {
            --partition->StreamCount;
            ApplyDurable(partition, GetOrCrash(cursors, partition->Claim.Partition));
        }
        // Records are in admission order, so acknowledgements keep the partition order.
        for (const auto& record : stream->Records) {
            --record.Partition->AdmittedCount;
            DurableBytes_.Increment(record.Request->Message->ByteSize);
            Acknowledge(record);
        }
        YT_TLOG_DEBUG("Committed static table export stream")
            .With("Window", stream->Window)
            .With("RecordCount", stream->Records.size());
    }

    void Acknowledge(const TRecord& record)
    {
        RawDataWeight_ -= record.Request->Message->ByteSize;
        record.Request->Message.Reset();
        if (record.Partition->IsHealthy()) {
            record.Request->Callback();
        }
    }

    void AcceptWithinBudget()
    {
        // Several job fibers may wait for acceptance at once, so the requests are accepted in queue order.
        // A partition without admitted records may exceed the budget by one message, so it is never starved.
        for (const auto& partition : Partitions_) {
            if (partition->AcceptedCount == std::ssize(partition->Queue)) {
                continue;
            }
            if (RawDataWeight_ <= Parameters_->MaxRawDataWeight ||
                (partition->AcceptedCount == 0 && partition->AdmittedCount == 0))
            {
                partition->Queue[partition->AcceptedCount]->Accepted.TrySet();
                ++partition->AcceptedCount;
            }
        }
        RawBytes_.Update(RawDataWeight_);
    }

    void UpdateStates()
    {
        for (const auto& partition : Partitions_) {
            auto guard = Guard(partition->Lock);
            partition->State = {
                .PendingMessageCount = std::ssize(partition->Queue) + partition->AdmittedCount,
                // Before bootstrap nothing is durable, so the state of the partition is known.
                .Claimed = partition->Claimed || partition->Unbootstrapped,
                .Bootstrapped = partition->Claimed,
                .Unpublished = IsUnpublished(partition),
                .FirstWriteTimestamp = GetFirstWriteTimestamp(partition),
                .SilentWhileDraining = !partition->FirstWriteTimestamp && partition->Draining,
            };
        }
    }

    //! The last resort for states whose durable progress cannot be determined:
    //! aborts every stream, rereads the cursors and requeues all retained records.
    void Reset(TError error)
    {
        if (ResetError_) {
            return;
        }
        Resets_.Increment();
        YT_TLOG_WARNING("Static table export writer is resetting its streams")
            .With(error);
        ResetError_ = std::move(error);
        for (const auto& [_, stream] : Streams_) {
            stream->Canceled = true;
        }
    }

    void TryFinishReset()
    {
        if (ResetReading_ || TInstant::Now() < RetryAfter_ ||
            std::ranges::any_of(Streams_, [] (const auto& item) {
                return item.second->Working;
            }))
        {
            return;
        }
        std::vector<int> indexes;
        for (const auto& [_, stream] : Streams_) {
            if (stream->Handle) {
                Storage_->AbortStream(stream->Handle);
            }
            for (const auto& [index, _] : stream->Deltas) {
                indexes.push_back(index);
            }
        }
        SortUnique(indexes);
        ResetReading_ = true;
        // An uncertain commit may have succeeded; locked cursors tell which prefixes are durable.
        RunIO(
            [storage = Storage_, indexes = std::move(indexes), timeout = Parameters_->TransactionTimeout] {
                return storage->ReadCursors(indexes, timeout);
            },
            [this] (const TErrorOr<TStaticTableExportCursors>& resultOrError) {
                ResetReading_ = false;
                RetryAfter_ = TInstant::Now() + Parameters_->RetryBackoff;
                if (!resultOrError.IsOK()) {
                    YT_TLOG_WARNING("Static table export writer failed to read cursors; will retry")
                        .With(resultOrError);
                    return;
                }
                FinishReset(resultOrError.Value());
            });
    }

    void FinishReset(const TStaticTableExportCursors& cursors)
    {
        // Requeueing in reverse keeps each partition's admission order ahead of its unadmitted suffix.
        for (const auto& [_, stream] : Streams_ | std::views::reverse) {
            ReleaseParsed(stream);
            for (const auto& record : stream->Records | std::views::reverse) {
                const auto& partition = record.Partition;
                partition->StreamCount = 0;
                partition->AdmittedCount = 0;
                if (partition->IsHealthy() && !partition->HandingOff) {
                    partition->Queue.push_front(record.Request);
                    ++partition->AcceptedCount;
                } else {
                    RawDataWeight_ -= record.Request->Message->ByteSize;
                }
            }
        }
        Streams_.clear();
        OpenWindows_.Update(0);
        for (const auto& partition : Partitions_) {
            if (auto it = cursors.find(partition->Claim.Partition); it != cursors.end()) {
                if (it->second->Generation != partition->Claim.Generation) {
                    SetError(partition, TError("Static table export partition is owned by a newer job").With("owner_generation", it->second->Generation).With(*ResetError_));
                    continue;
                }
                ApplyDurable(partition, it->second);
            }
            if (partition->HandingOff) {
                partition->Handoff.TrySet(TError("Static table export handoff was interrupted")
                        .With(*ResetError_));
            }
        }
        ResetError_.reset();
    }

    static THashSet<TPartitionPtr> GetStreamPartitions(const TStreamPtr& stream)
    {
        THashSet<TPartitionPtr> partitions;
        for (const auto& record : stream->Records) {
            partitions.insert(record.Partition);
        }
        return partitions;
    }

    static std::optional<TSystemTimestamp> GetFirstWriteTimestamp(const TPartitionPtr& partition)
    {
        if (partition->FirstWriteTimestamp) {
            return partition->FirstWriteTimestamp;
        }
        // A partition with a backlog delivers its first message much sooner, so a silent one starts from now.
        // A draining job reads nothing, so its silence says nothing about the backlog.
        if (!partition->Draining && TInstant::Now() >= partition->RegisteredAt + EmptyPartitionReportDelay) {
            return TSystemTimestamp(partition->RegisteredAt.Seconds());
        }
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

TStaticTableExportWriterResource::TStaticTableExportWriterResource(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(std::move(context), std::move(dynamicContext))
    , Writer_(New<TWriter>(GetParameters(), GetContext()->Logger, GetContext()->Profiler))
{ }

TStaticTableExportWriterResource::~TStaticTableExportWriterResource() = default;

IStaticTableExportParticipantPtr TStaticTableExportWriterResource::RegisterPartition(
    TStaticTableExportBinding binding,
    TStaticTableExportClaim claim,
    IInvokerPtr poolInvoker)
{
    return Writer_->RegisterPartition(std::move(binding), claim, std::move(poolInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
