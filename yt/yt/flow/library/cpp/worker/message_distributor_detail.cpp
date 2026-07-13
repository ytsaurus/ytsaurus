#include "message_distributor_detail.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message_batch.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/common/worker/message_id_batch.h>

#include <yt/yt/flow/library/cpp/misc/prefetch.h>

#include <library/cpp/iterator/zip.h>

#include <util/digest/multi.h>

namespace NYT::NFlow::NWorker {

using namespace NThreading;
using namespace NConcurrency;
using namespace NRpc;
using namespace NProfiling;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

//! (sourceJobId, destinationComputationId, destinationKey) — the granularity of head-of-line blocking.
using TOrderingKey = std::tuple<TJobId, TComputationId, std::optional<TKey>>;

//! Transparent so the set can be probed with a std::tie tuple of references (no key copy on lookup);
//! pairs with std::equal_to<> for the equality. There is no ready THash for tuple/optional.
struct TOrderingKeyHash
{
    using is_transparent = void;

    template <class T>
    size_t operator()(const T& key) const
    {
        const auto& [sourceJobId, computationId, destinationKey] = key;
        return MultiHash(sourceJobId, computationId, destinationKey ? THash<TKey>()(*destinationKey) : size_t(0));
    }
};

////////////////////////////////////////////////////////////////////////////////

TTimestampStatisticsGuard::TTimestampStatisticsGuard(
    TTimestampStatisticsWithLockPtr&& statistics,
    const TTimestampStatisticsRegistrationInfo& registrationInfo)
    : Statistics_(std::move(statistics))
    , Info_(registrationInfo)
{
    auto guard = Guard(Statistics_->Lock);
    Statistics_->Statistics.RegisterMessage(Info_);
}

TTimestampStatisticsGuard::~TTimestampStatisticsGuard()
{
    if (Statistics_) {
        auto guard = Guard(Statistics_->Lock);
        Statistics_->Statistics.UnregisterMessage(Info_);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDistributorPerStreamSensors
{
    using TKey = TStreamId;

    TProfiler Profiler;
    TStreamId StreamId;

    TProfiler LocalProfiler = Profiler.WithTag("stream_id", StreamId.Underlying());
    TCounter RetransmitCounter = LocalProfiler.Counter("/retransmit_count");
    TEventTimer AcceptedTimer = LocalProfiler.Timer("/message_accepted_time");
    TEventTimer ProcessingFinishedTimer = LocalProfiler.Timer("/message_processing_finished_time");
};

////////////////////////////////////////////////////////////////////////////////


TWorkerConnection::TJobState::TStreamQueuedState::TStreamQueuedState()
    : Thinner(New<TExponentialThinner>())
{ }

void TWorkerConnection::TJobState::ResetAccepted() noexcept
{
    for (auto& [streamId, tasks] : AcceptedTasks) {
        auto& state = QueuedTasks[streamId];
        while (!tasks.empty()) {
            auto node = tasks.extract(tasks.begin());
            // Thin in inflated bytes so the offered per-stream limit matches the input buffer accounting.
            state.Thinner->Insert(node.value().Task.GetKey(), InflatedByteSize(node.value().Task.Message->ByteSize));
            state.Tasks.insert(std::move(node));
        }
    }
    AcceptedTasks.clear();
}

////////////////////////////////////////////////////////////////////////////////

TWorkerConnection::TWorkerConnection(
    TMessageDistributorStatePtr state,
    IChannelPtr channel,
    const std::string& workerAddress,
    TStreamSpecStoragePtr streamSpecStorage,
    IInvokerPtr serializedInvoker,
    IInvokerPtr poolInvoker,
    const TSensorsOwner& sensorsOwner)
    : Logger(WorkerLogger().WithTag("Component: DistributorWorkerConnection, DestinationWorker: %v", workerAddress))
    , State_(std::move(state))
    , DistributorSensorsOwner_(sensorsOwner)
    , StreamSpecStorage_(std::move(streamSpecStorage))
    , SerializedInvoker_(std::move(serializedInvoker))
    , PoolInvoker_(std::move(poolInvoker))
    , MessageServiceProxy_(std::move(channel))
    , WorkerAddress_(workerAddress)
    , BatchDuration_(TDuration::Seconds(1))
    , BatchLimiter_(0, 0)
    , PushMessagesTimeoutCounter_(DistributorSensorsOwner_.GetCounter("/push_messages_timeout_count"))
{ }

TWorkerConnection::~TWorkerConnection()
{
    CancelableContext_->Cancel(TError(NYT::EErrorCode::Canceled, "WorkerConnection is destroying"));
}

void TWorkerConnection::Stop(bool checkJobsStopped) noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    CancelableContext_->Cancel(TError(NYT::EErrorCode::Canceled, "WorkerConnection is stopped"));
    if (checkJobsStopped) {
        SerializedInvoker_->Invoke(BIND(&TWorkerConnection::CheckJobsStopped, MakeStrong(this)));
    }
}

TFuture<std::vector<TTask>> TWorkerConnection::StopJob(const TJobId& jobId) noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    return BIND(&TWorkerConnection::DoStopJob, MakeStrong(this), jobId).AsyncVia(SerializedInvoker_).Run();
}

void TWorkerConnection::Start() noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    SerializedInvoker_->Invoke(BIND(&TWorkerConnection::PushMessagesUntilCanceled, MakeStrong(this)));
}

void TWorkerConnection::Send(TRoutedTask task) noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    bool sentTasksWasEmpty = false;

    {
        auto guard = Guard(SentTasksLock_);
        sentTasksWasEmpty = SentTasks_.empty();
        SentTasks_.emplace_back(std::move(task));
    }

    // SerializedInvoker adds new scheduled task to underlying thread pool only when previous task is completed.
    // That's why, if the underlying thread pool is overloaded, it works extremely poorly when one serialized invoker
    // contains a lot of light DoSend tasks and other serialized invokers contain heavy DoRequest tasks.
    if (sentTasksWasEmpty) {
        // This simple task will be finished before any task that is scheduled later.
        SerializedInvoker_->Invoke(BIND(&TWorkerConnection::DoSend, MakeStrong(this)));
    }
}

void TWorkerConnection::Reconfigure(const TDynamicMessageDistributorSpecPtr& dynamicSpec) noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    SerializedInvoker_->Invoke(BIND(&TWorkerConnection::DoReconfigure, MakeWeak(this), dynamicSpec));
}

std::vector<TTask> TWorkerConnection::DoStopJob(const TJobId& jobId) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    TForbidContextSwitchGuard contextSwitchGuard;

    YT_LOG_DEBUG("Stopping job in worker connection (JobId: %v)", jobId);

    std::vector<TTask> tasks;

    const auto it = JobStates_.find(jobId);
    if (it == JobStates_.end()) {
        return tasks;
    }
    it->second.ResetAccepted();
    for (auto& [streamId, streamTasks] : it->second.QueuedTasks) {
        while (!streamTasks.Tasks.empty()) {
            auto node = streamTasks.Tasks.extract(streamTasks.Tasks.begin());
            tasks.emplace_back(std::move(node.value().Task));
        }
    }
    // Thinner is destroyed here with all elements.
    JobStates_.erase(it);

    return tasks;
}

void TWorkerConnection::CheckJobsStopped() noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    if (!JobStates_.empty()) {
        YT_LOG_FATAL("Some job states of connection are not stopped before stopping connection (ExampleJobId: %v)", JobStates_.begin()->first);
    }
}

void TWorkerConnection::Reset() noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    TForbidContextSwitchGuard contextSwitchGuard;

    YT_VERIFY(ConnectionId_.has_value());

    YT_LOG_DEBUG("Resetting connection (ConnectionId: %v)",
        ConnectionId_);

    for (auto& [jobId, jobState] : JobStates_) {
        jobState.ResetAccepted();
    }

    ConnectionId_.reset();
    Offset_ = 0;
}

void TWorkerConnection::PushMessagesUntilCanceled()
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

    while (!CancelableContext_->IsCanceled()) {
        try {
            const auto startTime = TInstant::Now();
            const bool fullAndSuccessful = DoRequest();
            if (!fullAndSuccessful) {
                auto delayedFuture = TDelayedExecutor::MakeDelayed(startTime + BatchDuration_ - TInstant::Now());
                CancelableContext_->PropagateTo(delayedFuture);
                WaitUntilSet(delayedFuture);
            }
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Unexpected exception in worker connection");
        }
    }
}

void TWorkerConnection::DoSend() noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    TForbidContextSwitchGuard contextSwitchGuard;

    std::vector<TRoutedTask> tasks;

    {
        auto guard = Guard(SentTasksLock_);
        std::swap(tasks, SentTasks_);
    }

    for (auto& task : tasks) {
        const auto taskKey = task.Task.GetKey();
        auto& jobState = JobStates_[task.DestinationJobId];
        auto [it, wasEmplaced] = jobState.MessageIdToTaskInfo.emplace(task.Task.Message->MessageId, std::pair{task.Task.Message->StreamId, taskKey});
        if (wasEmplaced) {
            auto& state = jobState.QueuedTasks[task.Task.Message->StreamId];
            // Thin in inflated bytes so the offered per-stream limit matches the input buffer accounting.
            state.Thinner->Insert(taskKey, InflatedByteSize(task.Task.Message->ByteSize));
            InsertOrCrash(state.Tasks, std::move(task));
        } else {
            const auto& [duplicateStreamId, duplicateTaskKey] = it->second;
            const TRoutedTask* survivingTask = [&] () -> const TRoutedTask* {
                if (auto streamIt = jobState.QueuedTasks.find(task.Task.Message->StreamId); streamIt != jobState.QueuedTasks.end()) {
                    if (auto setIt = streamIt->second.Tasks.find(duplicateTaskKey); setIt != streamIt->second.Tasks.end()) {
                        return &*setIt;
                    }
                }
                auto& acceptedSet = GetOrCrash(jobState.AcceptedTasks, task.Task.Message->StreamId);
                auto acceptedSetIt = acceptedSet.find(duplicateTaskKey);
                YT_VERIFY(acceptedSetIt != acceptedSet.end());
                return &*acceptedSetIt;
            }();
            // A duplicate can only come from a dead source job's successor replaying its output (from
            // the OutputStore, or recomputed for swift computations): the surviving (earlier) copy is
            // from the now-dead predecessor, while the arriving copy is from the live successor and must
            // fire. Adopt the successor's SourceJobId as well as its OnDistributed: otherwise the task
            // keeps the dead predecessor's SourceJobId and would be dropped on re-route (see
            // DoProcessUnknownTasks' IsJobAlive check), losing the successor's callback.
            YT_VERIFY(!State_->JobDirectory->GetSnapshot()->IsJobAlive(survivingTask->Task.SourceJobId));
            survivingTask->Task.SourceJobId = task.Task.SourceJobId;
            survivingTask->Task.OnDistributed = std::move(task.Task.OnDistributed);
        }
    }
}

void TWorkerConnection::DoReconfigure(const TDynamicMessageDistributorSpecPtr& dynamicSpec) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    BatchLimiter_ = TBatchLimiter(dynamicSpec->SendQueueMaxRowsPerBatch, dynamicSpec->SendQueueMaxBytesPerBatch);
    BatchDuration_ = dynamicSpec->SendQueueBatchDuration;
}

bool TWorkerConnection::DoRequest()
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

    auto dynamicSpec = State_->DynamicSpec.Acquire();
    auto orderingTimestampBiases = State_->GetOrderingTimestampBiases();

    auto req = MessageServiceProxy_.PushMessages();
    req->SetRequestHeavy(true);
    req->SetRequestCodec(dynamicSpec->CompressionCodec);
    req->SetTimeout(dynamicSpec->PushMessagesTimeout);
    if (ConnectionId_) {
        ToProto(req->mutable_connection_id(), ConnectionId_.value());
    }
    req->set_committed_offset(Offset_);
    req->set_max_processed_batch_size(dynamicSpec->MaxProcessedBatchSize);

    bool batchIsFull = false;
    std::deque<std::tuple<TJobId, TStreamId, TTaskKey>> sentTaskInfos;
    {
        TBatchLimiter batchTotalLimiter = BatchLimiter_;

        auto inflatedNextBatchByteLimit = std::move(InflatedNextBatchByteLimit_);
        InflatedNextBatchByteLimit_.clear();

        const auto streamSpecs = StreamSpecStorage_->GetStreamSpecs();

        absl::flat_hash_map<TJobId, NProto::TReqPushMessages::TJobData*, ::THash<TJobId>> jobIdToData;
        auto getOrAddJobData = [&] (const TJobId& jobId) {
            auto& jobDataPtr = jobIdToData[jobId];
            if (!jobDataPtr) {
                jobDataPtr = req->add_jobs();
                ToProto(jobDataPtr->mutable_job_id(), jobId);
            }
            return jobDataPtr;
        };

        for (const auto& [jobId, jobState] : JobStates_) {
            for (const auto& [streamId, state] : jobState.QueuedTasks) {
                if (state.Tasks.empty()) {
                    continue; // Do not send empty offers.
                }
                auto* offer = getOrAddJobData(jobId)->add_offers();
                offer->set_stream_spec_id(ToProto(streamSpecs->GetLastSpecId(streamId)));
                for (const auto& [minOrderTimestamp, inflatedByteSize] : state.Thinner->GetThinnedBuckets()) {
                    offer->add_min_order_timestamps(ToProto(minOrderTimestamp));
                    offer->add_inflated_byte_sizes(inflatedByteSize);
                }
            }
        }

        auto queuedTaskIterateFunction = [&] (auto&& callback) {
            for (auto& [jobId, jobState] : JobStates_) {
                for (auto& [streamId, state] : jobState.QueuedTasks) {
                    callback(jobId, state.Tasks);
                }
            }
        };

        // The tasks live in QueuedTasks and are not mutated until the response is processed,
        // so the const TRoutedTask* pointers stay valid here.
        absl::flat_hash_map<TJobId, std::vector<const TRoutedTask*>, ::THash<TJobId>> jobIdToTasks;
        auto selectedCallback = [&] (const TJobId& jobId, const TRoutedTask& task) {
            getOrAddJobData(jobId);
            jobIdToTasks[jobId].push_back(&task);
        };

        batchIsFull = SelectTasksToSend(queuedTaskIterateFunction, orderingTimestampBiases->Biases, std::move(inflatedNextBatchByteLimit), batchTotalLimiter, selectedCallback);

        // Emit messages in the request's job order; the serializer owns the wire layout.
        TMessageBatchSerializer batchSerializer(streamSpecs);
        for (auto& jobData : *req->mutable_jobs()) {
            auto jobId = FromProto<TJobId>(jobData.job_id());
            auto tasksIt = jobIdToTasks.find(jobId);
            if (tasksIt == jobIdToTasks.end()) {
                continue;
            }
            jobData.set_message_count(tasksIt->second.size());
            MakePrefetcher()
                .Add([] (const auto* task) {
                    Y_PREFETCH_READ(task, 3);
                })
                .Add([] (const auto* task) {
                    Y_PREFETCH_READ(task->Task.Message.Get(), 3);
                })
                .ForEach(tasksIt->second, [&] (const auto* task) {
                    batchSerializer.AddMessage(task->Task.Message);
                    sentTaskInfos.emplace_back(jobId, task->Task.Message->StreamId, task->Task.GetKey());
                });
        }
        req->Attachments().push_back(batchSerializer.Finish());
    }

    auto reqFuture = req->Invoke();
    CancelableContext_->PropagateTo(reqFuture);
    auto rspOrError = WaitFor(reqFuture); // Some jobs may be stopped during this yield.

    if (!rspOrError.IsOK()) {
        YT_LOG_ERROR(rspOrError, "PushMessages request failed");
        PushMessagesTimeoutCounter_.Increment();
        return false;
    }

    const auto requestFinishedInstant = TInstant::Now();

    const auto& rsp = rspOrError.Value();
    bool connectionReset = rsp->reset();
    auto responseConnectionId = FromProto<TGuid>(rsp->connection_id());

    if (connectionReset) {
        YT_VERIFY(rsp->processed_jobs().empty());
        Reset();
        ConnectionId_ = responseConnectionId;
    }

    YT_VERIFY(Offset_ <= rsp->offset());
    Offset_ = rsp->offset();

    if (ConnectionId_) {
        YT_VERIFY(responseConnectionId == ConnectionId_);
    } else {
        ConnectionId_ = responseConnectionId;

        YT_LOG_DEBUG("Initialized connection (ConnectionId: %v)",
            ConnectionId_);
    }

    for (const auto& [protoState, sentTaskInfo] : Zip(rsp->message_states(), sentTaskInfos)) {
        const auto& [jobId, streamId, taskKey] = sentTaskInfo;

        const auto jobStateIt = JobStates_.find(jobId);
        if (jobStateIt == JobStates_.end()) {
            // Job probably was stopped.
            continue;
        }
        auto& jobState = jobStateIt->second;

        auto deliveryState = FromProto<EMessageDeliveryState>(protoState);

        auto streamQueuedTasksIt = GetIteratorOrCrash(jobState.QueuedTasks, streamId);
        auto taskIt = GetIteratorOrCrash(streamQueuedTasksIt->second.Tasks, taskKey);

        YT_LOG_DEBUG("MessageLifeCycle.Distributor: message was pushed to destination worker "
            "(MessageId: %v, DestinationJobId: %v, DeliveryState: %v, ConnectionId: %v)",
            taskIt->Task.Message->MessageId,
            jobId,
            deliveryState,
            ConnectionId_);

        switch (deliveryState) {
            case EMessageDeliveryState::Accepted: {
                DistributorSensorsOwner_.Get<TDistributorPerStreamSensors>(streamId).AcceptedTimer.Record(requestFinishedInstant - taskIt->Task.CreateTime);
                // Move task from QueuedTasks to AcceptedTasks without reallocation.
                auto node = streamQueuedTasksIt->second.Tasks.extract(taskIt);
                streamQueuedTasksIt->second.Thinner->Erase(taskKey);
                auto [insertedIt, inserted, _] = jobState.AcceptedTasks[streamId].insert(std::move(node));
                YT_VERIFY(inserted);
                if (streamQueuedTasksIt->second.Tasks.empty()) {
                    jobState.QueuedTasks.erase(streamQueuedTasksIt);
                }
                break;
            }
            case EMessageDeliveryState::Declined:
                [[fallthrough]]; // Rely on StopJob call.
            case EMessageDeliveryState::CongestionDeclined: {
                DistributorSensorsOwner_.Get<TDistributorPerStreamSensors>(streamId).RetransmitCounter.Increment();
                break;
            }
            default:
                YT_ABORT();
        }
    }

    // Collect callbacks to fire after releasing the serialized invoker context.
    // Processed messages are grouped by job; all entries are implicitly ProcessingFinished.
    std::vector<TOnDistributedCallback> processedCallbacks;
    const auto& attachments = rsp->Attachments();
    auto processedMessageIds = ParseMessageIdBatch(attachments.empty() ? TRef() : TRef(attachments[0]));

    for (const auto& jobProcessed : rsp->processed_jobs()) {
        const auto jobId = FromProto<TJobId>(jobProcessed.job_id());
        auto jobStateIt = JobStates_.find(jobId);

        for (int index = 0; index < jobProcessed.message_count(); ++index) {
            YT_VERIFY(!processedMessageIds.empty());
            const auto messageId = processedMessageIds.front();
            processedMessageIds.pop_front();

            if (jobStateIt == JobStates_.end()) {
                // Job probably was stopped.
                continue;
            }
            auto& jobState = jobStateIt->second;

            YT_LOG_DEBUG("MessageLifeCycle.Distributor: message processing finished "
                "(MessageId: %v, DestinationJobId: %v, ConnectionId: %v)",
                messageId,
                jobId,
                ConnectionId_);

            const auto taskIdIt = jobState.MessageIdToTaskInfo.find(messageId);
            if (taskIdIt == jobState.MessageIdToTaskInfo.end()) {
                // It is possible for message to be processed and to be absent from MessageIdToInFlightTasks map,
                // if PushMessages request timed-out and we rescheduled the message, but it was actually received
                // (for example during SIGSTOP in unstable test).
                // Message will be sent one more time, so just do nothing here.
                continue;
            }
            const auto& [streamId, taskId] = taskIdIt->second;

            const auto acceptedTasksIt = jobState.AcceptedTasks.find(streamId);
            if (acceptedTasksIt == jobState.AcceptedTasks.end()) {
                // Timeout can occur in getting answer from destination worker,
                // so "ProcessingFinished" can be received before "Accepted".
                // No special logic here, just ignore this notification and
                // send message one more time.
                continue;
            }
            const auto taskIt = acceptedTasksIt->second.find(taskId);
            if (taskIt == acceptedTasksIt->second.end()) {
                continue;
            }

            auto node = acceptedTasksIt->second.extract(taskIt);
            auto& task = node.value();

            DistributorSensorsOwner_.Get<TDistributorPerStreamSensors>(streamId).ProcessingFinishedTimer.Record(requestFinishedInstant - task.Task.CreateTime);

            processedCallbacks.emplace_back(std::move(task.Task.OnDistributed));
            jobState.MessageIdToTaskInfo.erase(taskIdIt);

            if (acceptedTasksIt->second.empty()) {
                jobState.AcceptedTasks.erase(acceptedTasksIt);
            }

            if (jobState.MessageIdToTaskInfo.empty()) {
                YT_VERIFY(jobState.QueuedTasks.empty());
                YT_VERIFY(jobState.AcceptedTasks.empty());
                JobStates_.erase(jobStateIt);
                jobStateIt = JobStates_.end();
            }
        }
    }

    // The processed-message-id records must match the per-job counts exactly.
    YT_VERIFY(processedMessageIds.empty());

    for (const auto& streamLimit : rsp->stream_limits()) {
        if (streamLimit.job_ids_size() != streamLimit.inflated_next_batch_byte_limits_size()) {
            YT_LOG_ERROR("Got invalid limits (JobIdSize: %v, InflatedNextBatchByteLimit_Size: %v)",
                streamLimit.job_ids_size(),
                streamLimit.inflated_next_batch_byte_limits_size());
            continue;
        }
        for (const auto& [jobId, inflatedNextBatchByteLimit] : Zip(streamLimit.job_ids(), streamLimit.inflated_next_batch_byte_limits())) {
            InflatedNextBatchByteLimit_[std::pair{FromProto<TJobId>(jobId), FromProto<TStreamId>(streamLimit.stream_id())}] = inflatedNextBatchByteLimit;
        }
    }

    if (requestFinishedInstant > UpdateGaugesInstant_ + TDuration::Seconds(5)) {
        UpdateGaugesInstant_ = requestFinishedInstant;
        THashMap<TStreamId, std::pair<i64, i64>> queuedAndInflightMessages;
        for (const auto& [jobId, jobState] : JobStates_) {
            for (const auto& [streamId, tasks] : jobState.AcceptedTasks) {
                queuedAndInflightMessages[streamId].second += tasks.size();
            }
            for (const auto& [streamId, state] : jobState.QueuedTasks) {
                queuedAndInflightMessages[streamId].first += state.Tasks.size();
            }
        }
        for (const auto& [streamId, sensors] : PerStreamSensors_) {
            queuedAndInflightMessages[streamId]; // Just touch to provide zeros.
        }

        for (const auto& [streamId, queuedAndInflightCount] : queuedAndInflightMessages) {
            auto& sensors = PerStreamSensors_.try_emplace(streamId, DistributorSensorsOwner_.GetProfiler(), streamId).first->second;
            sensors.QueuedMessagesGauge.Update(queuedAndInflightCount.first);
            sensors.InflightMessagesGauge.Update(queuedAndInflightCount.second);
        }

        YT_LOG_DEBUG("Worker connection gauges were updated (QueuedAndInflightMessagesCount: %v)",
            NYson::ConvertToYsonString(queuedAndInflightMessages, NYson::EYsonFormat::Text));

        // Debug code.
        const TInstant threshold = TInstant::Now() - dynamicSpec->HungTaskThreshold;
        for (const auto& [jobId, jobState] : JobStates_) {
            auto process = [&] (auto& streamsTasks, const std::string& type) {
                for (const auto& [streamId, state] : streamsTasks) {
                    const auto& tasks = [&] () -> const TRoutedTaskSet& {
                        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(state)>, TJobState::TStreamQueuedState>) {
                            return state.Tasks;
                        } else {
                            return state;
                        }
                    }();
                    if (tasks.empty()) {
                        continue;
                    }
                    const auto& task = *tasks.begin();
                    if (task.Task.CreateTime > threshold) {
                        continue;
                    }
                    YT_LOG_DEBUG("Hung task (Type: %v, SrcJobId: %v, DstJobId: %v, DstComputationId: %v, "
                        "StreamId: %v, JobStreamNextBatchByteLimit: %v, ByteSize: %v, "
                        "TaskId: %v, MessageId: %v, AlignmentInstant: %v, CreateInstant: %v)",
                        type,
                        task.Task.SourceJobId,
                        jobId,
                        task.Task.ComputationId,
                        streamId,
                        InflatedNextBatchByteLimit_[std::pair{jobId, streamId}],
                        task.Task.Message->ByteSize,
                        task.Task.Id,
                        task.Task.Message->MessageId,
                        TInstant::Seconds(task.Task.Message->AlignmentTimestamp.Underlying()),
                        task.Task.CreateTime);
                }
            };
            process(jobState.AcceptedTasks, "AcceptedTasks");
            process(jobState.QueuedTasks, "QueuedTasks");
        }
    }

    PoolInvoker_->Invoke(BIND([processedCallbacks = std::move(processedCallbacks), req = std::move(req), reqFuture = std::move(reqFuture)] () mutable {
        for (auto& callback : processedCallbacks) {
            callback();
        }
        processedCallbacks.clear();
        req = {};
        reqFuture = {};
    }));

    YT_LOG_DEBUG("PushMessages iteration is complete (SentMessagesCount: %v)", sentTaskInfos.size());
    return batchIsFull;
}

////////////////////////////////////////////////////////////////////////////////

std::pair<TTimestampStatisticsWithLockPtr, bool> TMessageDistributorState::GetOrCreateStreamStatistics(TStreamId streamId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    auto guard = Guard(StreamTimestampStatisticsLock_);
    auto& statistics = StreamTimestampStatistics_[streamId];
    bool created = false;
    if (!statistics) {
        statistics = New<TTimestampStatisticsWithLock>();
        created = true;
    }
    return {statistics, created};
}

TOrderingTimestampBiasesPtr TMessageDistributorState::GetOrderingTimestampBiases() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return CachedBiases_.Acquire();
}

void TMessageDistributorState::RecomputeOrderingTimestampBiases()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = Guard(BiasesLock_);

    THashMap<TStreamId, TTimestampStatistics> statistics;
    // Fill as global statistics.
    if (auto messageTransferingInfo = MessageTransferingInfo.Acquire()) {
        statistics = messageTransferingInfo->StreamTimestampStatistics;
    }

    THashMap<TStreamId, TTimestampStatisticsWithLockPtr> localStatistics;
    {
        auto guard = Guard(StreamTimestampStatisticsLock_);
        localStatistics = StreamTimestampStatistics_;
    }

    // If local statistics is richer than global (global may not be computed and distributed yet), use local statistics.
    for (const auto& [streamId, localTimestampStatistics] : localStatistics) {
        auto& streamStatistics = statistics[streamId];
        auto guard = Guard(localTimestampStatistics->Lock);
        if (localTimestampStatistics->Statistics.MessageCount > streamStatistics.MessageCount) {
            streamStatistics = localTimestampStatistics->Statistics;
        }
    }

    // Remember the last non-empty statistics per stream and restore it for an idle stream, so a
    // stream that went empty (and may reactivate before the next recompute) never yields a zero bias.
    for (auto& [streamId, streamStatistics] : statistics) {
        if (streamStatistics.MessageCount > 0) {
            LastNonEmptyStreamStatistics_[streamId] = streamStatistics;
        } else if (auto it = LastNonEmptyStreamStatistics_.find(streamId); it != LastNonEmptyStreamStatistics_.end()) {
            streamStatistics = it->second;
        }
    }

    auto biases = New<TOrderingTimestampBiases>();
    biases->Biases = ComputeOrderingTimestampBiases(statistics, PipelineSpec.Acquire());
    CachedBiases_.Store(std::move(biases));
}

TMessageDistributorStatusPtr TMessageDistributorState::GetStatus() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    THashMap<TStreamId, TTimestampStatisticsWithLockPtr> localStatistics;
    {
        auto guard = Guard(StreamTimestampStatisticsLock_);
        localStatistics = StreamTimestampStatistics_;
    }

    auto status = New<TMessageDistributorStatus>();
    for (const auto& [streamId, localTimestampStatistics] : localStatistics) {
        auto& streamStatistics = status->StreamTimestampStatistics[streamId];
        auto guard = Guard(localTimestampStatistics->Lock);
        streamStatistics = localTimestampStatistics->Statistics;
    }

    return status;
}

////////////////////////////////////////////////////////////////////////////////

TMessageDistributor::TMessageDistributor(
    IJobDirectoryPtr jobDirectory,
    IChannelFactoryPtr channelFactory,
    TStreamSpecStoragePtr streamSpecStorage)
    : Logger(WorkerLogger().WithTag("Component: Distributor"))
    , State_(New<TMessageDistributorState>(jobDirectory))
    , JobDirectory_(std::move(jobDirectory))
    , ChannelFactory_(std::move(channelFactory))
    , StreamSpecStorage_(std::move(streamSpecStorage))
    , UnknownTasksThread_(New<TActionQueue>("MsgDistUnknown"))
    , ThreadPool_(NConcurrency::CreateThreadPool(State_->DynamicSpec.Acquire()->ThreadCount, "MsgDist"))
    , SerializedInvoker_(UnknownTasksThread_->GetInvoker())
    , Profiler_(WorkerProfiler().WithPrefix("/message_distributor"))
    , SensorsOwner_(Profiler_)
    , UnknownTaskQueueSizeGauge_(Profiler_.Gauge("/unknown_task_queue_size"))
    , AddedUnknownTasks_(Profiler_.Counter("/added_unknown_tasks"))
    , RoutingSnapshot_(JobDirectory_->GetSnapshot())
{ }

void TMessageDistributor::Initialize()
{
    // Topology changes arrive through the snapshot signal.
    JobDirectory_->SubscribeSnapshotPublished(
        BIND(&TMessageDistributor::DoOnSnapshotPublished, MakeWeak(this)).Via(SerializedInvoker_));

    SerializedInvoker_->Invoke(BIND(&TMessageDistributor::ProcessUnknownTasks, MakeWeak(this)));
}

TMessageDistributor::~TMessageDistributor()
{
    for (const auto& [_, connection] : WorkerAddressToConnection_) {
        connection->Stop(false);
    }
    WorkerAddressToConnection_.clear();
}

void TMessageDistributor::DistributeOutputMessages(
    TJobId fromJobId,
    std::deque<TDistributorOutputMessage>&& messages)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (messages.empty()) {
        return;
    }

    if (!State_->JobDirectory->GetSnapshot()->IsJobAlive(fromJobId)) {
        // Absence means the source job is dead/being removed (the snapshot is published before a job
        // starts, so it can never mean "not yet created"). Drop the callbacks without calling them.
        return;
    }

    const auto now = TInstant::Now();

    std::vector<TTask> tasks;
    tasks.reserve(messages.size());
    bool newStreamAppeared = false;
    for (auto& item : messages) {
        auto registrationInfo = TTimestampStatistics::ComputeRegistrationInfo(*item.Message);
        auto [streamStatistics, created] = State_->GetOrCreateStreamStatistics(item.Message->StreamId);
        newStreamAppeared |= created;
        const auto alignmentTimestamp = item.Message->AlignmentTimestamp;
        tasks.push_back(TTask{
            .Id = TaskIdCounter_.fetch_add(1),
            .AlignmentTimestamp = alignmentTimestamp,
            .SourceJobId = fromJobId,
            .ComputationId = item.ComputationId,
            .Message = std::move(item.Message),
            .OnDistributed = std::move(item.OnDistributed),
            .TimestampStatisticsGuard = TTimestampStatisticsGuard(std::move(streamStatistics), registrationInfo),
            .CreateTime = now,
        });
    }

    // A first-seen stream has no cached bias yet; recompute before the tasks become queued so they
    // are never ordered with a zero bias. New streams appear rarely, so the synchronous cost is fine.
    if (newStreamAppeared) {
        State_->RecomputeOrderingTimestampBiases();
    }

    SerializedInvoker_->Invoke(BIND(&TMessageDistributor::EnqueueProducedTasks, MakeWeak(this), Passed(std::move(tasks))));
}

void TMessageDistributor::Reconfigure(TPipelineSpecPtr pipelineSpec, TDynamicMessageDistributorSpecPtr dynamicSpec)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    State_->DynamicSpec.Store(std::move(dynamicSpec));
    State_->PipelineSpec.Store(std::move(pipelineSpec));

    SerializedInvoker_->Invoke(BIND(&TMessageDistributor::DoReconfigure, MakeWeak(this)));
}

TMessageDistributorStatusPtr TMessageDistributor::GetStatus() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    return State_->GetStatus();
}

void TMessageDistributor::UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    State_->MessageTransferingInfo.Store(std::move(messageTransferingInfo));
    State_->RecomputeOrderingTimestampBiases();
}

void TMessageDistributor::ProcessUnknownTasks(TWeakPtr<TMessageDistributor> weakThis)
{
    while (auto this_ = weakThis.Lock()) {
        if (!this_->DoProcessUnknownTasks()) {
            this_.Reset();
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
        }
    }
}

bool TMessageDistributor::DoProcessUnknownTasks() noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    // There is no fiber yield below, so UnknownTasks_ is not touched mid-routing and its front-to-back
    // order is the only thing that preserves per-(sourceJob, computation, key) ordering.
    TForbidContextSwitchGuard contextSwitchGuard;

    UnknownTaskQueueSizeGauge_.Update(UnknownTaskCount_);

    if (UnknownTasks_.empty()) {
        return false;
    }

    auto latestSnapshot = JobDirectory_->GetSnapshot();
    auto routingSnapshot = RoutingSnapshot_;

    // Usually empty (everything routes); the !empty() short-circuit keeps the common path free of a
    // lookup, and the transparent hash/eq let the rare lookup use std::tie without copying the key.
    absl::flat_hash_set<TOrderingKey, TOrderingKeyHash, std::equal_to<>> blockedOrderingKeys;
    std::vector<TTask> rejected;
    bool routedAny = false;

    while (!UnknownTasks_.empty()) {
        auto chain = std::move(UnknownTasks_.front());
        UnknownTasks_.pop_front();

        for (auto& task : chain) {
            if (!latestSnapshot->IsJobAlive(task.SourceJobId)) {
                YT_LOG_DEBUG("MessageLifeCycle.Distributor: message was dropped because source job has been removed (MessageId: %v, JobId: %v)",
                    task.Message->MessageId,
                    task.SourceJobId);
                --UnknownTaskCount_; // Callback dropped without calling.
                continue;
            }

            auto key = routingSnapshot->ComputeMessageKey(task.ComputationId, *task.Message);
            if (!blockedOrderingKeys.empty() &&
                blockedOrderingKeys.contains(std::tie(task.SourceJobId, task.ComputationId, key)))
            {
                rejected.push_back(std::move(task));
                continue;
            }

            if (TryRouteTask(routingSnapshot, task, key)) {
                routedAny = true;
                --UnknownTaskCount_;
            } else {
                blockedOrderingKeys.emplace(task.SourceJobId, task.ComputationId, key);
                rejected.push_back(std::move(task));
            }
        }
    }

    // Retry rejected tasks first next time, but behind anything returned from a connection meanwhile.
    if (!rejected.empty()) {
        UnknownTasks_.push_front(std::move(rejected));
    }

    return routedAny;
}

bool TMessageDistributor::TryRouteTask(
    const TJobDirectorySnapshotPtr& routingSnapshot,
    TTask& task,
    const std::optional<TKey>& key) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

    if (!key) {
        // Unknown computation; retry later.
        return false;
    }

    const auto* messageRoute = routingSnapshot->FindRouteByKey(task.ComputationId, *key);
    if (!messageRoute) {
        YT_LOG_DEBUG("MessageLifeCycle.Distributor: could not find route for message. Will try later (MessageId: %v, SourceJobId: %v)",
            task.Message->MessageId,
            task.SourceJobId);
        return false;
    }

    auto& destinationJobState = DestinationJobStates_[messageRoute->JobId];
    if (!destinationJobState.CanRouteFuture) {
        destinationJobState.CanRouteFuture = CanRouteToNewJobsFuture_;
    }
    if (!destinationJobState.CanRouteFuture.IsSet()) {
        YT_LOG_DEBUG("MessageLifeCycle.Distributor: could not route message before revision of messages in old connections is completed "
            "(MessageId: %v, SourceJobId: %v, DestinationJobId: %v, DestinationAddress: %v)",
            task.Message->MessageId,
            task.SourceJobId,
            messageRoute->JobId,
            messageRoute->WorkerAddress);
        return false;
    }

    YT_LOG_DEBUG("MessageLifeCycle.Distributor: found message route for message "
        "(MessageId: %v, SourceJobId: %v, DestinationJobId: %v, DestinationAddress: %v)",
        task.Message->MessageId,
        task.SourceJobId,
        messageRoute->JobId,
        messageRoute->WorkerAddress);

    if (!destinationJobState.Connection) {
        auto connectionIt = WorkerAddressToConnection_.find(messageRoute->WorkerAddress);
        if (connectionIt == WorkerAddressToConnection_.end()) {
            const auto currentSpec = State_->DynamicSpec.Acquire();
            TWorkerConnectionPtr connection = New<TWorkerConnection>(
                State_,
                ChannelFactory_->CreateChannel(messageRoute->WorkerAddress),
                messageRoute->WorkerAddress,
                StreamSpecStorage_,
                CreateSerializedInvoker(ThreadPool_->GetInvoker()),
                ThreadPool_->GetInvoker(),
                SensorsOwner_);
            connection->Reconfigure(currentSpec);
            connection->Start();
            connectionIt = EmplaceOrCrash(
                WorkerAddressToConnection_,
                messageRoute->WorkerAddress,
                connection);

            YT_LOG_DEBUG("Created new worker connection (Address: %v)",
                messageRoute->WorkerAddress);
        }
        destinationJobState.Connection = connectionIt->second;
    }

    destinationJobState.Connection->Send(
        TRoutedTask{.DestinationJobId = messageRoute->JobId, .Task = std::move(task)});
    return true;
}

void TMessageDistributor::EnqueueProducedTasks(std::vector<TTask>&& tasks) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    if (!tasks.empty()) {
        AddedUnknownTasks_.Increment(std::ssize(tasks));
        UnknownTaskCount_ += std::ssize(tasks);
        UnknownTasks_.push_back(std::move(tasks));
    }
}

void TMessageDistributor::ReturnTasksFromStoppedConnection(std::vector<TTask>&& tasks) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    if (!tasks.empty()) {
        UnknownTaskCount_ += std::ssize(tasks);
        UnknownTasks_.push_front(std::move(tasks));
    }
}

void TMessageDistributor::DoOnWorkerRemoved(const std::string& workerAddress) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    TForbidContextSwitchGuard contextSwitchGuard;

    if (const auto it = WorkerAddressToConnection_.find(workerAddress); it != WorkerAddressToConnection_.end()) {
        YT_LOG_DEBUG("Remove destination worker from Distributor (Address: %v)", workerAddress);
        it->second->Stop();
        WorkerAddressToConnection_.erase(it);
    }
}

void TMessageDistributor::DoOnSnapshotPublished(const TJobDirectorySnapshotPtr& newSnapshot) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
    // Establishing the gate and publishing the snapshot must be atomic w.r.t. routing.
    TForbidContextSwitchGuard contextSwitchGuard;

    auto diff = ComputeJobDirectoryDiff(RoutingSnapshot_, newSnapshot);

    // Stop removed jobs and re-enqueue their tasks. The gate future resolves only once that is done,
    // so added jobs are not routed to until the returned tasks are back ahead of any later message.
    std::vector<TFuture<void>> gateFutures;
    gateFutures.push_back(CanRouteToNewJobsFuture_);
    for (auto jobId : diff.RemovedJobs) {
        auto it = DestinationJobStates_.find(jobId);
        if (it == DestinationJobStates_.end()) {
            continue;
        }
        YT_LOG_DEBUG("Remove destination job from Distributor (JobId: %v)", jobId);
        // Connection is null if no messages were routed to this job yet.
        if (it->second.Connection) {
            auto gateFuture = it->second.Connection->StopJob(jobId)
                .AsUnique()
                .Apply(BIND([weakThis = MakeWeak(this)] (std::vector<TTask>&& tasks) {
                    if (auto this_ = weakThis.Lock()) {
                        this_->ReturnTasksFromStoppedConnection(std::move(tasks));
                    }
                }).AsyncVia(SerializedInvoker_));
            gateFutures.push_back(gateFuture);
        }
        DestinationJobStates_.erase(it);
    }

    if (gateFutures.size() > 1) {
        CanRouteToNewJobsFuture_ = AllSet<void>(
            std::move(gateFutures),
            TFutureCombinerOptions{
                .PropagateCancelationToInput = false,
                .CancelInputOnShortcut = false})
            .AsVoid();
    }

    // Removed workers: tear down their (now job-less) connections.
    for (const auto& workerAddress : diff.RemovedWorkers) {
        DoOnWorkerRemoved(workerAddress);
    }

    // Publish only after the gate is established, so new jobs are never routed to before it.
    RoutingSnapshot_ = newSnapshot;
}

void TMessageDistributor::DoReconfigure() noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

    const auto currentSpec = State_->DynamicSpec.Acquire();

    ThreadPool_->SetThreadCount(currentSpec->ThreadCount);

    // Pipeline spec (computations/streams) may have changed; refresh the cached biases.
    State_->RecomputeOrderingTimestampBiases();

    for (const auto& [_, connection] : WorkerAddressToConnection_) {
        connection->Reconfigure(currentSpec);
    }
}

////////////////////////////////////////////////////////////////////////////////

TExponentialThinner::~TExponentialThinner()
{
    EraseTree(Root_);
}

void TExponentialThinner::Insert(TKey key, i64 size)
{
    YT_VERIFY(size >= 0 && size <= std::numeric_limits<ui32>::max());
    auto* node = new TNode{
        .Key = key,
        .Priority = RandomNumber<ui32>(),
        .Size = static_cast<ui32>(size),
    };
    node->SubtreeSize = node->Size;
    Insert(Root_, node);
}

void TExponentialThinner::Erase(TKey key)
{
    Erase(Root_, key);
}

std::vector<std::pair<TSystemTimestamp, i64>> TExponentialThinner::GetThinnedBuckets()
{
    std::vector<std::pair<TSystemTimestamp, i64>> result;

    if (!Root_) {
        return result;
    }

    auto nextNode = Root_;
    while (nextNode->Left) {
        nextNode = nextNode->Left;
    }

    // nextNode is the minimum node now.

    auto doubleSafe = [] (i64 value) {
        return std::min(value, std::numeric_limits<i64>::max() / 2) * 2;
    };

    i64 prefixSize = 0;
    i64 maxBucketSize = 1;
    i64 maxBucketTimestampDelta = 0;
    while (true) {
        auto currentTimestamp = nextNode->Key.AlignmentTimestamp;
        TKey keyLimit = {
            .AlignmentTimestamp = TSystemTimestamp(currentTimestamp.Underlying() + maxBucketTimestampDelta),
            .TaskId = std::numeric_limits<TTaskId>::max(),
        };
        auto [newPrefixSize, newNextNode] = FindLargestPrefix(Root_, keyLimit, prefixSize + maxBucketSize);
        i64 takenInBucket = newPrefixSize - prefixSize;

        result.emplace_back(currentTimestamp, takenInBucket);

        if (!newNextNode) {
            break;
        }

        maxBucketSize = doubleSafe(std::max(maxBucketSize, takenInBucket));
        maxBucketTimestampDelta = doubleSafe(std::max<i64>(maxBucketTimestampDelta,
            newNextNode->Key.AlignmentTimestamp.Underlying() - currentTimestamp.Underlying()));

        nextNode = newNextNode;
        prefixSize = newPrefixSize;
    }

    return result;
}

void TExponentialThinner::FixSubtree(TNodePtr root)
{
    root->SubtreeSize = root->Size;
    if (root->Left) {
        root->SubtreeSize += root->Left->SubtreeSize;
    }
    if (root->Right) {
        root->SubtreeSize += root->Right->SubtreeSize;
    }
}

void TExponentialThinner::Split(TNodePtr root, TKey key, TNodePtr& left, TNodePtr& right)
{
    if (!root) {
        left = nullptr;
        right = nullptr;
    } else if (key <= root->Key) {
        Split(root->Left, key, left, root->Left);
        right = root;
        FixSubtree(right);
    } else {
        Split(root->Right, key, root->Right, right);
        left = root;
        FixSubtree(left);
    }
}

void TExponentialThinner::Insert(TNodePtr& root, TNodePtr node)
{
    if (!root) {
        root = node;
    } else if (node->Priority > root->Priority) {
        Split(root, node->Key, node->Left, node->Right);
        root = node;
        FixSubtree(root);
    } else {
        // Check does not guarantee that two nodes with the same key can not be inserted.
        YT_ASSERT(node->Key != root->Key);
        Insert(node->Key < root->Key ? root->Left : root->Right, node);
        FixSubtree(root);
    }
}

void TExponentialThinner::Merge(TNodePtr& root, TNodePtr left, TNodePtr right)
{
    if (!left) {
        root = right;
    } else if (!right) {
        root = left;
    } else if (left->Priority > right->Priority) {
        Merge(left->Right, left->Right, right);
        root = left;
        FixSubtree(root);
    } else {
        Merge(right->Left, left, right->Left);
        root = right;
        FixSubtree(root);
    }
}

void TExponentialThinner::Erase(TNodePtr& root, TKey key)
{
    YT_VERIFY(root);

    if (root->Key == key) {
        auto oldRoot = root;
        Merge(root, oldRoot->Left, oldRoot->Right);
        delete oldRoot;
    } else {
        Erase(key < root->Key ? root->Left : root->Right, key);
        FixSubtree(root);
    }
}

void TExponentialThinner::EraseTree(TNodePtr& root)
{
    if (root) {
        EraseTree(root->Left);
        EraseTree(root->Right);
        delete root;
        root = nullptr;
    }
}

std::pair<i64, TExponentialThinner::TNodePtr> TExponentialThinner::FindLargestPrefix(TNodePtr root, TKey keyLimit, i64 sizeLimit)
{
    if (!root) {
        return {0, nullptr};
    }

    i64 takenSize = 0;

    // Left subtree.
    if (root->Key <= keyLimit && root->Left && root->Left->SubtreeSize <= sizeLimit) {
        takenSize += root->Left->SubtreeSize;
        sizeLimit -= root->Left->SubtreeSize;
    } else {
        auto [leftTakenSize, leftNextNode] = FindLargestPrefix(root->Left, keyLimit, sizeLimit);
        if (leftNextNode) {
            return {leftTakenSize, leftNextNode};
        }
        takenSize += leftTakenSize;
        sizeLimit -= leftTakenSize;
    }

    // Root.
    if (root->Key <= keyLimit && sizeLimit > 0) {
        takenSize += root->Size;
        sizeLimit -= root->Size;
    } else {
        return {takenSize, root};
    }

    // Right subtree.
    auto [rightTakenSize, rightNextNode] = FindLargestPrefix(root->Right, keyLimit, sizeLimit);
    return {takenSize + rightTakenSize, rightNextNode};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
