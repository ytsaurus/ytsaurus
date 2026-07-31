#pragma once

#include "message_distributor.h"

#include "buffer_state_manager.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/common/worker/message_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timestamp_statistics.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/library/profiling/sensors_owner/sensors_owner.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <deque>
#include <list>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TTimestampStatisticsWithLock final
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    TTimestampStatistics Statistics;
};

using TTimestampStatisticsWithLockPtr = TIntrusivePtr<TTimestampStatisticsWithLock>;

class TTimestampStatisticsGuard
{
public:
    TTimestampStatisticsGuard() = default;
    TTimestampStatisticsGuard(TTimestampStatisticsWithLockPtr&& statistics, const TTimestampStatisticsRegistrationInfo& registrationInfo);
    ~TTimestampStatisticsGuard();

    TTimestampStatisticsGuard(TTimestampStatisticsGuard&&) noexcept = default;
    TTimestampStatisticsGuard& operator=(TTimestampStatisticsGuard&&) noexcept = default;

    TTimestampStatisticsGuard(const TTimestampStatisticsGuard&) = delete;
    TTimestampStatisticsGuard& operator=(const TTimestampStatisticsGuard&) = delete;

private:
    TTimestampStatisticsWithLockPtr Statistics_;
    TTimestampStatisticsRegistrationInfo Info_;
};

////////////////////////////////////////////////////////////////////////////////

using TTaskId = i64; // Number of task in distributor.

struct TTaskKey
{
    TSystemTimestamp AlignmentTimestamp = ZeroSystemTimestamp;
    TTaskId TaskId = 0;

    auto operator<=>(const TTaskKey&) const = default;
};

struct TTask
{
    TTaskId Id = 0;
    // Collocated with Id so std::set comparisons read both fields from one cache line
    // instead of chasing through Message->AlignmentTimestamp.
    TSystemTimestamp AlignmentTimestamp = ZeroSystemTimestamp;
    // Mutable: a duplicate from a dead source's successor overwrites SourceJobId and OnDistributed in
    // place with the live successor's. Neither is part of the task key, so mutating them through a set
    // iterator is safe.
    mutable TJobId SourceJobId;
    TComputationId ComputationId;
    TOutputMessageConstPtr Message;
    mutable TOnDistributedCallback OnDistributed;
    TTimestampStatisticsGuard TimestampStatisticsGuard;
    TInstant CreateTime;

    TTaskKey GetKey() const
    {
        return {.AlignmentTimestamp = AlignmentTimestamp, .TaskId = Id};
    }
};

struct TRoutedTask
{
    TJobId DestinationJobId;
    TTask Task;
};

// Comparator for std::set<TRoutedTask> that orders by TTaskKey extracted from the task.
// Using a set instead of map<TTaskKey, TRoutedTask> avoids storing the key twice.
struct TRoutedTaskKeyComparator
{
    using is_transparent = void;

    bool operator()(const TRoutedTask& lhs, const TRoutedTask& rhs) const
    {
        return lhs.Task.GetKey() < rhs.Task.GetKey();
    }

    bool operator()(const TRoutedTask& lhs, const TTaskKey& rhs) const
    {
        return lhs.Task.GetKey() < rhs;
    }

    bool operator()(const TTaskKey& lhs, const TRoutedTask& rhs) const
    {
        return lhs < rhs.Task.GetKey();
    }
};

using TRoutedTaskSet = std::set<TRoutedTask, TRoutedTaskKeyComparator>;

////////////////////////////////////////////////////////////////////////////////

struct TOrderingTimestampBiases final
{
    THashMap<std::pair<TStreamId, TComputationId>, double> Biases;
};

using TOrderingTimestampBiasesPtr = TIntrusivePtr<TOrderingTimestampBiases>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMessageDistributor)
DECLARE_REFCOUNTED_CLASS(TMessageDistributorState)
DECLARE_REFCOUNTED_CLASS(TWorkerConnection)
DECLARE_REFCOUNTED_CLASS(TExponentialThinner)

////////////////////////////////////////////////////////////////////////////////

class TMessageDistributorState
    : public TRefCounted
{
public:
    explicit TMessageDistributorState(IJobDirectoryPtr jobDirectory)
        : JobDirectory(std::move(jobDirectory))
        , DynamicSpec(New<TDynamicMessageDistributorSpec>())
        , PipelineSpec(New<TPipelineSpec>())
        , CachedBiases_(New<TOrderingTimestampBiases>())
    { }

    const IJobDirectoryPtr JobDirectory;
    TAtomicIntrusivePtr<TDynamicMessageDistributorSpec> DynamicSpec;
    TAtomicIntrusivePtr<TPipelineSpec> PipelineSpec;
    TAtomicIntrusivePtr<TMessageTransferingInfo> MessageTransferingInfo;

    //! Returns the per-stream statistics holder (creating it on first use) together with whether the
    //! stream was seen for the first time.
    std::pair<TTimestampStatisticsWithLockPtr, bool> GetOrCreateStreamStatistics(TStreamId streamId);

    TOrderingTimestampBiasesPtr GetOrderingTimestampBiases() const;

    void RecomputeOrderingTimestampBiases();

    TMessageDistributorStatusPtr GetStatus() const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StreamTimestampStatisticsLock_);
    THashMap<TStreamId, TTimestampStatisticsWithLockPtr> StreamTimestampStatistics_;

    TAtomicIntrusivePtr<TOrderingTimestampBiases> CachedBiases_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, BiasesLock_);
    // Last seen non-empty statistics per stream; restores a stream's bias when it goes
    // momentarily idle (MessageCount drops to 0) so the cache never serves a zero bias.
    THashMap<TStreamId, TTimestampStatistics> LastNonEmptyStreamStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TMessageDistributorState);

////////////////////////////////////////////////////////////////////////////////

class TWorkerConnection
    : public TRefCounted
{
public:
    class TBatchLimiter
        : public TMessageBatchLimiter
    {
    public:
        using TMessageBatchLimiter::TMessageBatchLimiter;

        void Add(const TRoutedTask& task)
        {
            TMessageBatchLimiter::Add(task.Task.Message->ByteSize);
        }
    };

private:
    struct TWorkerConnectionPerStreamSensors
    {
        NProfiling::TProfiler Profiler;
        TStreamId StreamId;

        NProfiling::TProfiler LocalProfiler = Profiler.WithTag("stream_id", StreamId.Underlying());
        NProfiling::TGauge InflightMessagesGauge = LocalProfiler.Gauge("/inflight_messages_count");
        NProfiling::TGauge QueuedMessagesGauge = LocalProfiler.Gauge("/queued_messages_count");
    };

public:
    TWorkerConnection(
        TMessageDistributorStatePtr state,
        NRpc::IChannelPtr channel,
        const std::string& workerAddress,
        TStreamSpecStoragePtr streamSpecStorage,
        IInvokerPtr serializedInvoker,
        IInvokerPtr poolInvoker,
        const NProfiling::TSensorsOwner& sensorsOwner);

    ~TWorkerConnection() override;

    void Stop(bool checkJobsStopped = true) noexcept;

    //! Drops the job and returns its still-queued tasks so the distributor can re-route them.
    TFuture<std::vector<TTask>> StopJob(const TJobId& jobId) noexcept;

    void Start() noexcept;

    void Send(TRoutedTask task) noexcept;

    void Reconfigure(const TDynamicMessageDistributorSpecPtr& dynamicSpec) noexcept;

protected:
    const NLogging::TLogger Logger;

private:
    struct TJobState
    {
        struct TStreamQueuedState
        {
            TRoutedTaskSet Tasks;
            TExponentialThinnerPtr Thinner;

            TStreamQueuedState();
        };

        THashMap<TStreamId, TStreamQueuedState> QueuedTasks;
        THashMap<TStreamId, TRoutedTaskSet> AcceptedTasks;
        absl::flat_hash_map<TMessageId, std::pair<TStreamId, TTaskKey>, ::THash<TMessageId>> MessageIdToTaskInfo; // For all tasks.

        void ResetAccepted() noexcept;
    };

private:
    const TMessageDistributorStatePtr State_;
    const NProfiling::TSensorsOwner DistributorSensorsOwner_;
    const TStreamSpecStoragePtr StreamSpecStorage_;
    const IInvokerPtr SerializedInvoker_;
    const IInvokerPtr PoolInvoker_;
    THashMap<TJobId, TJobState> JobStates_;
    THashMap<std::pair<TJobId, TStreamId>, i64> InflatedNextBatchByteLimit_;

    TMessageServiceProxy MessageServiceProxy_;
    const std::string WorkerAddress_;

    TDuration BatchDuration_;
    TBatchLimiter BatchLimiter_;

    i64 Offset_ = 0;

    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();

    std::optional<TGuid> ConnectionId_;

    NProfiling::TCounter PushMessagesTimeoutCounter_;
    THashMap<TStreamId, TWorkerConnectionPerStreamSensors> PerStreamSensors_;

    TInstant UpdateGaugesInstant_ = TInstant::Zero();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SentTasksLock_);
    std::vector<TRoutedTask> SentTasks_;

private:
    std::vector<TTask> DoStopJob(const TJobId& jobId) noexcept;

    void CheckJobsStopped() noexcept;

    void Reset() noexcept;

    void PushMessagesUntilCanceled();

    void DoSend() noexcept;

    void DoReconfigure(const TDynamicMessageDistributorSpecPtr& dynamicSpec) noexcept;

    // Return value: batch was full and successfully sent or not.
    bool DoRequest();
};

DEFINE_REFCOUNTED_TYPE(TWorkerConnection);

////////////////////////////////////////////////////////////////////////////////

class TMessageDistributor
    : public IMessageDistributor
{
public:
    TMessageDistributor(
        IJobDirectoryPtr jobDirectory,
        NRpc::IChannelFactoryPtr channelFactory,
        TStreamSpecStoragePtr streamSpecStorage);

    void Initialize();

    ~TMessageDistributor() override;

    void DistributeOutputMessages(
        TJobId fromJobId,
        std::deque<TDistributorOutputMessage>&& messages) override;

    void Reconfigure(TPipelineSpecPtr pipelineSpec, TDynamicMessageDistributorSpecPtr dynamicSpec) override;

    void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) override;

    TMessageDistributorStatusPtr GetStatus() const override;

protected:
    const NLogging::TLogger Logger;

private:
    struct TDestinationJobState
    {
        TFuture<void> CanRouteFuture;
        TWorkerConnectionPtr Connection;
    };

    const TMessageDistributorStatePtr State_;
    const IJobDirectoryPtr JobDirectory_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TStreamSpecStoragePtr StreamSpecStorage_;
    const NConcurrency::TActionQueuePtr UnknownTasksThread_;
    const NConcurrency::IThreadPoolPtr ThreadPool_;
    const IInvokerPtr SerializedInvoker_;
    const NProfiling::TProfiler Profiler_;
    const NProfiling::TSensorsOwner SensorsOwner_;
    NProfiling::TGauge UnknownTaskQueueSizeGauge_;
    NProfiling::TCounter AddedUnknownTasks_;

    std::atomic<TTaskId> TaskIdCounter_ = 0;
    // Chains of tasks awaiting routing: fresh chains go to the back, chains returned from stopped
    // connections (older tasks) to the front, rejected chains back to the front. Routing never
    // context-switches, so this front/back order is the whole ordering guarantee.
    std::list<std::vector<TTask>> UnknownTasks_;
    // Total number of tasks across all chains in UnknownTasks_.
    i64 UnknownTaskCount_ = 0;

    //! Snapshot used for destination route lookup, advanced after the gate is set. Serialized-thread
    //! only. Source liveness uses the latest snapshot from JobDirectory_, not this one.
    TJobDirectorySnapshotPtr RoutingSnapshot_;

    THashMap<std::string, TWorkerConnectionPtr> WorkerAddressToConnection_;
    THashMap<TJobId, TDestinationJobState> DestinationJobStates_;

    //! Gate: a newly added destination job becomes routable only after every job removed in the
    //! same reconfiguration has returned its tasks. Serialized-thread only.
    TFuture<void> CanRouteToNewJobsFuture_ = OKFuture;

private:
    static void ProcessUnknownTasks(TWeakPtr<TMessageDistributor> weakThis);

    //! Routes everything currently in UnknownTasks_; returns whether any task was routed.
    bool DoProcessUnknownTasks() noexcept;

    bool TryRouteTask(const TJobDirectorySnapshotPtr& routingSnapshot, TTask& task, const std::optional<TKey>& key) noexcept;

    void EnqueueProducedTasks(std::vector<TTask>&& tasks) noexcept;

    void ReturnTasksFromStoppedConnection(std::vector<TTask>&& tasks) noexcept;

    void DoOnWorkerRemoved(const std::string& workerAddress) noexcept;

    //! Stops removed jobs/workers, refreshes the routing gate and advances RoutingSnapshot_.
    void DoOnSnapshotPublished(const TJobDirectorySnapshotPtr& snapshot) noexcept;

    void DoReconfigure() noexcept;
};

DEFINE_REFCOUNTED_TYPE(TMessageDistributor);

////////////////////////////////////////////////////////////////////////////////

class TExponentialThinner
    : public TRefCounted
{
public:
    using TKey = TTaskKey;

public:
    TExponentialThinner() = default;
    ~TExponentialThinner() override;

    // O(log(N)).
    void Insert(TKey key, i64 size);

    // O(log(N)).
    void Erase(TKey key);

    // O(log(N) ^ 2).
    std::vector<std::pair<TSystemTimestamp, i64>> GetThinnedBuckets();

private:
    // Use treap data structure.

    struct TNode
    {
        TKey Key;
        ui32 Priority{};
        ui32 Size{};
        i64 SubtreeSize{};

        TNode* Left = nullptr;
        TNode* Right = nullptr;
    };

    using TNodePtr = TNode*;

private:
    void FixSubtree(TNodePtr root);
    void Split(TNodePtr root, TKey key, TNodePtr& left, TNodePtr& right);
    void Insert(TNodePtr& root, TNodePtr node);
    void Merge(TNodePtr& root, TNodePtr left, TNodePtr right);
    void Erase(TNodePtr& root, TKey key);
    void EraseTree(TNodePtr& root);

    // Search largest prefix with limitations.
    // |keyLimit| is strict, |sizeLimit| can be exceeded by one node.
    // Return value: (taken size, next node). Next node is null if limits are not reached.
    std::pair<i64, TNodePtr> FindLargestPrefix(TNodePtr root, TKey keyLimit, i64 sizeLimit);

private:
    TNodePtr Root_ = nullptr;
};

DEFINE_REFCOUNTED_TYPE(TExponentialThinner);

////////////////////////////////////////////////////////////////////////////////

template <typename TTaskIterateFunction, typename TSelectedCallback>
bool SelectTasksToSend(
    TTaskIterateFunction queuedTaskIterateFunction,
    const THashMap<std::pair<TStreamId, TComputationId>, double>& orderingTimestampBiases,
    THashMap<std::pair<TJobId, TStreamId>, i64> inflatedNextBatchByteLimit,
    TWorkerConnection::TBatchLimiter& batchTotalLimiter,
    TSelectedCallback selectedCallback)
{
    using TSet = TRoutedTaskSet;
    using TSetIterator = TSet::iterator;

    struct TState
    {
        TJobId JobId;
        TSetIterator It;
        TSet* Set = nullptr;
        double Bias = 0;
        i64* InflatedLimit = nullptr;
    };

    struct THeapElement
    {
        TTaskKey TaskKey;
        TState* State = nullptr;

        bool operator<(const THeapElement& other) const
        {
            return TaskKey < other.TaskKey;
        }
    };

#ifndef NDEBUG
    const auto& Logger = WorkerLogger();
#endif

    auto getTaskPriority = [] (const TRoutedTask& routedTask, double bias) {
        return TTaskKey(TSystemTimestamp(routedTask.Task.Message->AlignmentTimestamp.Underlying() + bias), routedTask.Task.Id);
    };

    std::deque<TState> states;
    std::vector<THeapElement> heap;
    queuedTaskIterateFunction([&] (const auto& jobId, const auto& streamId, auto& tasks) {
        if (!tasks.empty()) {
            const auto it = tasks.begin();
            const double bias = GetOrDefault(orderingTimestampBiases, std::pair<TStreamId, TComputationId>(it->Task.Message->StreamId, it->Task.ComputationId));
            // THashMap is node-based, so this slot reference stays valid as more queues insert below.
            auto& inflatedLimit = inflatedNextBatchByteLimit[std::pair{jobId, streamId}];
            auto& state = states.emplace_back(TState{.JobId = jobId, .It = it, .Set = &tasks, .Bias = bias, .InflatedLimit = &inflatedLimit});
            heap.push_back(THeapElement{.TaskKey = getTaskPriority(*it, bias), .State = &state});
        }
    });

    MakeHeap(heap.begin(), heap.end());
    auto dropFront = [&] {
        ExtractHeap(heap.begin(), heap.end());
        heap.pop_back();
    };
    auto advanceFront = [&] {
        auto* state = heap.front().State;
        if (++state->It != state->Set->end()) {
            heap.front().TaskKey = getTaskPriority(*state->It, state->Bias);
            AdjustHeapFront(heap.begin(), heap.end());
        } else {
            dropFront();
        }
    };

#ifndef NDEBUG
    absl::flat_hash_set<std::pair<TMessageId, TJobId>, ::THash<std::pair<TMessageId, TJobId>>> messageIds;
#endif

    while (!heap.empty()) {
        auto* state = heap.front().State;
        const auto it = state->It;

        // Charge the destination buffer's per-message inflation against the limit:
        // the input buffer accounts every accepted message as #InflatedByteSize() inflated bytes,
        // and |inflatedNextBatchByteLimit| is computed from that same inflated-byte budget
        // (TInputBuffer::RecalculateStreamLimits). Mirroring the inflation here keeps the source
        // and destination accounting in the same units.
        const i64 inflatedChargedSize = InflatedByteSize(it->Task.Message->ByteSize);
        auto& inflatedCurrentLimit = *state->InflatedLimit;
        if (inflatedCurrentLimit < inflatedChargedSize) {
            dropFront();
            continue;
        }
        inflatedCurrentLimit -= inflatedChargedSize;

#ifndef NDEBUG
        const auto messageKey = std::pair(it->Task.Message->MessageId, state->JobId);
        auto [messageIdsIt, ok] = messageIds.emplace(messageKey);
        YT_TLOG_FATAL_UNLESS(ok, "There is duplicate of message in batch")
            .With("MessageId", it->Task.Message->MessageId)
            .With("JobId", state->JobId);
#endif

        selectedCallback(state->JobId, *it);

        batchTotalLimiter.Add(*it);
        if (batchTotalLimiter.IsFull()) {
            return true;
        }

        advanceFront();
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
