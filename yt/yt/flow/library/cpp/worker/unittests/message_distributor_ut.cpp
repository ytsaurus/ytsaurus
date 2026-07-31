#include <yt/yt/flow/library/cpp/worker/message_distributor_detail.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <util/string/join.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/null_channel.h>

#include <yt/yt/core/test_framework/framework.h>

#include <atomic>

namespace NYT::NFlow::NWorker {
namespace {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using TSourceJobTaskQueues = std::vector<std::pair<TJobId, std::vector<std::pair<TStreamId, TRoutedTaskSet*>>>>;
using TSelectedBatch = THashMap<TJobId, THashMap<TStreamId, std::vector<TTaskKey>>>;

////////////////////////////////////////////////////////////////////////////////

const TWorkerConnection::TBatchLimiter UnlimitedBatcher(1e9, 1e9);

const TStreamId DefaultStreamId = "stream";
const TSystemTimestamp DefaultSystemTimestamp{1000500};

TComputationStreamSpecStoragePtr MakeStreamSpecStorage(const TStreamId& streamId, const NTableClient::TTableSchemaPtr& schema)
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[streamId][TStreamSpecId(1)] = streamSpec;
    return New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        /*groupBySchema*/ New<NTableClient::TTableSchema>(),
        /*evaluatorCache*/ nullptr);
}

struct TTaskParameters
{
    TStreamId Stream = DefaultStreamId;
    TMessageId MessageId = TMessageId("<id>");
    TSystemTimestamp SystemTimestamp = DefaultSystemTimestamp;
    i64 ColumnSize = 1;
};

TRoutedTask MakeRoutedTask(TTaskParameters parameters, TTaskKey key = {})
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""([{name="data"; type="string";};])""")));
    TMessageBuilder builder(parameters.Stream, schema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue(std::string(parameters.ColumnSize, 'X'), 0));
    builder.SetMessageId(parameters.MessageId);
    builder.SetSystemTimestamp(parameters.SystemTimestamp);
    builder.SetAlignmentTimestamp(key.AlignmentTimestamp);
    builder.SetEventTimestamp(parameters.SystemTimestamp);
    auto message = builder.Finish();
    return TRoutedTask{.Task = {.Id = key.TaskId, .AlignmentTimestamp = key.AlignmentTimestamp, .Message = New<TOutputMessage>(std::move(message), MakeStreamSpecStorage(parameters.Stream, schema))}};
}

auto MakeQueuedTaskIterateFunction(const TSourceJobTaskQueues& queues)
{
    return [&] (auto&& callback) {
        for (auto& [jobId, jobQueues] : queues) {
            for (auto& [streamId, tasks] : jobQueues) {
                callback(jobId, streamId, *tasks);
            }
        }
    };
}

auto MakeSelectedCallback(TSelectedBatch& selectedBatch)
{
    return [&] (const TJobId& jobId, const TRoutedTask& task) {
        selectedBatch[jobId][task.Task.Message->StreamId].push_back(task.Task.GetKey());
    };
}

// Fix StreamId in all tasks to match the stream they are placed in.
TSourceJobTaskQueues FixStream(TSourceJobTaskQueues&& queues)
{
    for (auto& [jobId, jobQueues] : queues) {
        for (auto& [streamId, tasks] : jobQueues) {
            TRoutedTaskSet fixed;
            while (!tasks->empty()) {
                auto node = tasks->extract(tasks->begin());
                TMessage modifiedMessage = *node.value().Task.Message;
                modifiedMessage.StreamId = streamId;
                auto streamSpecStorage = MakeStreamSpecStorage(streamId, modifiedMessage.PayloadSchema);
                node.value().Task.Message = New<TOutputMessage>(std::move(modifiedMessage), std::move(streamSpecStorage));
                fixed.insert(std::move(node));
            }
            *tasks = std::move(fixed);
        }
    }
    return queues;
}

////////////////////////////////////////////////////////////////////////////////

//! Minimal IJobDirectory whose only meaningful behavior is a fixed set of alive jobs.
//! Enough for TWorkerConnection, which only consults GetSnapshot()->IsJobAlive() while deduping.
class TFixedJobDirectory
    : public IJobDirectory
{
public:
    explicit TFixedJobDirectory(THashSet<TJobId> aliveJobs)
        : Snapshot_(New<TJobDirectorySnapshot>(
            /*groupBySchemas*/ THashMap<TComputationId, NTableClient::TTableSchemaPtr>{},
            /*converterCache*/ nullptr,
            /*computations*/ THashMap<TComputationId, TJobDirectorySnapshot::TComputationRouting>{},
            /*routableJobs*/ aliveJobs,
            /*aliveJobs*/ aliveJobs,
            /*workers*/ THashSet<std::string>{},
            NLogging::TLogger()))
    { }

    void Reconfigure(const TFlowLayoutPtr& /*flowLayout*/, const TPipelineSpecPtr& /*pipelineSpec*/) override
    { }

    TJobDirectorySnapshotPtr GetSnapshot() const override
    {
        return Snapshot_;
    }

    i64 GetPartitionCount(const TComputationId& /*computationId*/) const override
    {
        return 0;
    }

    std::optional<TMessageRoute> FindRouteByKey(const TComputationId& /*computationId*/, const TKey& /*key*/) const override
    {
        return std::nullopt;
    }

    DEFINE_SIGNAL_OVERRIDE(TSnapshotPublishedSignature, SnapshotPublished);

private:
    const TJobDirectorySnapshotPtr Snapshot_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TMessageDistributorTest, ExponentiallyThinnerSimple)
{
    auto thinner = New<TExponentialThinner>();

    auto key = TTaskKey{DefaultSystemTimestamp, 0};
    thinner->Insert(key, 100);
    auto buckets = thinner->GetThinnedBuckets();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_EQ(buckets[0].first, DefaultSystemTimestamp);
    EXPECT_EQ(buckets[0].second, 100);
    thinner->Erase(key);

    ASSERT_DEATH(thinner->Erase(key), "YT_VERIFY");
}

TEST(TMessageDistributorTest, ExponentiallyThinnerBig)
{
    constexpr i64 elementsCount = 100'000;
    constexpr i64 getBucketsOperations = 100'000;
    constexpr i64 elementSize = 100;

    auto thinner = New<TExponentialThinner>();

    std::vector<TTaskKey> keys;
    for (i64 i = 0; i < elementsCount; ++i) {
        keys.push_back(TTaskKey{TSystemTimestamp(i), i});
    }

    for (auto key : keys) {
        thinner->Insert(key, elementSize);
    }

    for (i64 i = 0; i < getBucketsOperations; ++i) {
        auto buckets = thinner->GetThinnedBuckets();

        ASSERT_EQ(buckets.size(), 17u);

        auto totalSize = 0;
        for (const auto& [systemTimestamp, size] : buckets) {
            totalSize += size;
        }
        ASSERT_EQ(totalSize, elementsCount * elementSize) << ConvertToYsonString(buckets, NYson::EYsonFormat::Text).ToString();

        auto key = keys[RandomNumber<ui64>() % keys.size()];
        thinner->Erase(key);
        thinner->Insert(key, elementSize);
    }

    for (auto key : keys) {
        thinner->Erase(key);
    }
}

////////////////////////////////////////////////////////////////////////////////

THashMap<std::pair<TJobId, TStreamId>, i64> MakeUnlimitedNextBatchByteLimit(const TSourceJobTaskQueues& queues)
{
    THashMap<std::pair<TJobId, TStreamId>, i64> inflatedNextBatchByteLimit;
    for (const auto& [jobId, jobQueues] : queues) {
        for (const auto& [streamId, tasks] : jobQueues) {
            inflatedNextBatchByteLimit[std::pair{jobId, streamId}] = 1e9;
        }
    }
    return inflatedNextBatchByteLimit;
}

TEST(TMessageDistributorTest, SelectTasksToSendTrivialCases)
{
    {
        auto batcher = UnlimitedBatcher;

        TSelectedBatch result;
        auto batchIsFull = SelectTasksToSend(MakeQueuedTaskIterateFunction({}), {}, {}, batcher, MakeSelectedCallback(result));
        ASSERT_TRUE(result.empty());
        ASSERT_FALSE(batchIsFull);
    }
    {
        TRoutedTaskSet tasks;
        tasks.insert(MakeRoutedTask({}, {DefaultSystemTimestamp, 0}));
        const TSourceJobTaskQueues queues = FixStream({
            {TJobId(), {{DefaultStreamId, &tasks}}},
        });
        auto batcher = UnlimitedBatcher;
        TSelectedBatch result;
        auto batchIsFull = SelectTasksToSend(
            MakeQueuedTaskIterateFunction(queues),
            {},
            MakeUnlimitedNextBatchByteLimit(queues),
            batcher,
            MakeSelectedCallback(result));
        ASSERT_EQ(result.size(), 1u);
        ASSERT_EQ(tasks.size(), 1u); // Not modified.
        ASSERT_FALSE(batchIsFull);
        const auto& selectedTasks = result.at(TJobId{}).at(DefaultStreamId);
        ASSERT_EQ(selectedTasks.size(), 1u);
        ASSERT_EQ(selectedTasks[0].AlignmentTimestamp, DefaultSystemTimestamp);
    }
}

TEST(TMessageDistributorTest, SelectTasksToSendDuplicate)
{
#ifdef NDEBUG
    GTEST_SKIP() << "Duplicate detection is compiled in only under NDEBUG-less builds.";
#else
    // Tasks with the same MessageId but different keys — duplicate detection is by MessageId.
    TRoutedTaskSet tasks;
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("1")}, {DefaultSystemTimestamp, 0}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("2")}, {DefaultSystemTimestamp, 1}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("1")}, {DefaultSystemTimestamp, 2}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("3")}, {DefaultSystemTimestamp, 3}));
    const TSourceJobTaskQueues queues = FixStream({
        {TJobId(), {{DefaultStreamId, &tasks}}},
    });
    auto batcher = UnlimitedBatcher;
    TSelectedBatch result;
    ASSERT_DEATH(
        SelectTasksToSend(MakeQueuedTaskIterateFunction(queues), {}, MakeUnlimitedNextBatchByteLimit(queues), batcher, MakeSelectedCallback(result)),
        "duplicate");
#endif
}

TEST(TMessageDistributorTest, SelectTasksToSendNextBatchLimit)
{
    TRoutedTaskSet tasks;
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("0"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 0}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("1"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 1}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("2"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 2}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("3"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 3}));
    const TSourceJobTaskQueues queues = FixStream({
        {TJobId(), {{DefaultStreamId, &tasks}}},
    });
    const THashMap<std::pair<TJobId, TStreamId>, i64> inflatedNextBatchByteLimit = {
        {std::pair{TJobId(), DefaultStreamId}, 37000},
    };
    auto batcher = UnlimitedBatcher;
    TSelectedBatch result;
    SelectTasksToSend(
        MakeQueuedTaskIterateFunction(queues),
        {},
        inflatedNextBatchByteLimit,
        batcher,
        MakeSelectedCallback(result));
    ASSERT_EQ(tasks.size(), 4u); // Not modified.
    const auto& selectedTasks = result.at(TJobId{}).at(DefaultStreamId);
    ASSERT_EQ(selectedTasks.size(), 3u); // Limited.
}

TEST(TMessageDistributorTest, SelectTasksToSendTotalLimit)
{
    TRoutedTaskSet tasks;
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("0"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 0}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("1"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 1}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("2"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 2}));
    tasks.insert(MakeRoutedTask({.MessageId = TMessageId("3"), .ColumnSize = 10000}, {DefaultSystemTimestamp, 3}));
    const TSourceJobTaskQueues& queues = FixStream({
        {TJobId(), {{DefaultStreamId, &tasks}}},
    });

    TWorkerConnection::TBatchLimiter batcher(3, 35000);
    TSelectedBatch result;
    auto batchIsFull = SelectTasksToSend(
        MakeQueuedTaskIterateFunction(queues),
        {},
        MakeUnlimitedNextBatchByteLimit(queues),
        batcher,
        MakeSelectedCallback(result));
    ASSERT_TRUE(batchIsFull);

    ASSERT_EQ(tasks.size(), 4u); // Not modified.
    const auto& selectedTasks = result.at(TJobId{}).at(DefaultStreamId);
    ASSERT_EQ(selectedTasks.size(), 3u); // Limited.
}

TEST(TMessageDistributorTest, SelectTasksToSendPriority)
{
    // Keys encode ordering: AlignmentTimestamp is the primary sort key, TaskId is secondary.
    // B0 has a smaller AlignmentTimestamp so it comes first.
    // Within the same AlignmentTimestamp, tasks are ordered by TaskId: A0(0), A1(1), C2(2), A3(3), C4(4), A5(5).
    TRoutedTaskSet tasksA;
    tasksA.insert(MakeRoutedTask({.MessageId = TMessageId("A0")}, {DefaultSystemTimestamp, 0}));
    tasksA.insert(MakeRoutedTask({.MessageId = TMessageId("A1")}, {DefaultSystemTimestamp, 1}));
    tasksA.insert(MakeRoutedTask({.MessageId = TMessageId("A3")}, {DefaultSystemTimestamp, 3}));
    tasksA.insert(MakeRoutedTask({.MessageId = TMessageId("A5")}, {DefaultSystemTimestamp, 5}));
    TRoutedTaskSet tasksB;
    tasksB.insert(MakeRoutedTask({.MessageId = TMessageId("B0")}, {TSystemTimestamp{DefaultSystemTimestamp.Underlying() - 1}, 0}));
    TRoutedTaskSet tasksC;
    tasksC.insert(MakeRoutedTask({.MessageId = TMessageId("C2")}, {DefaultSystemTimestamp, 2}));
    tasksC.insert(MakeRoutedTask({.MessageId = TMessageId("C4")}, {DefaultSystemTimestamp, 4}));
    const TSourceJobTaskQueues& queues = FixStream({
        {TJobId(TGuid(0, 1)), {{DefaultStreamId, &tasksA}, {TStreamId(Concat<std::string>(DefaultStreamId.Underlying(), "-C")), &tasksC}}},
        {TJobId(TGuid(0, 2)), {{DefaultStreamId, &tasksB}}},
    });

    std::vector<std::string> messageIds;
    while (true) {
        TWorkerConnection::TBatchLimiter batcher(1, 35000);

        TSelectedBatch result;
        SelectTasksToSend(
            MakeQueuedTaskIterateFunction(queues),
            {},
            MakeUnlimitedNextBatchByteLimit(queues),
            batcher,
            MakeSelectedCallback(result));
        if (result.empty()) {
            break;
        }
        ASSERT_EQ(result.size(), 1u);
        const auto jobId = result.begin()->first;
        ASSERT_EQ(result.begin()->second.size(), 1u);
        const auto streamId = result.begin()->second.begin()->first;
        ASSERT_EQ(result[jobId][streamId].size(), 1u);
        const auto taskKey = result[jobId][streamId].front();
        auto& tasks = *FindIf(
            FindIf(
                queues,
                [&] (const auto& x) {
                    return x.first == jobId;
                })
                ->second,
            [&] (const auto& x) {
                return x.first == streamId;
            })
            ->second;
        const auto taskIt = tasks.find(taskKey);
        ASSERT_NE(taskIt, tasks.end());
        messageIds.emplace_back(taskIt->Task.Message->MessageId.Underlying());
        tasks.erase(taskIt);
    }

    ASSERT_THAT(messageIds, ::testing::ElementsAre("B0", "A0", "A1", "C2", "A3", "C4", "A5"));
}

////////////////////////////////////////////////////////////////////////////////

// Regression for the distributor dedup losing a live successor's callback.
//
// When a source job dies and its successor replays the same output, the duplicate reaches the
// destination connection while the earlier (predecessor) copy is still queued/accepted. The dedup must
// adopt the live successor's SourceJobId (and OnDistributed) onto the surviving task. Otherwise the
// surviving task keeps the dead predecessor's SourceJobId, and once a destination StopJob re-routes it,
// the distributor drops it (DoProcessUnknownTasks' IsJobAlive check), losing the successor's callback —
// the destination never reports processing-finished and the source's output stays stuck.
TEST(TMessageDistributorTest, WorkerConnectionDedupAdoptsLiveSuccessor)
{
    const auto deadSourceJob = TJobId(TGuid(0, 1));
    const auto liveSourceJob = TJobId(TGuid(0, 2));
    const auto destinationJob = TJobId(TGuid(0, 3));

    // The predecessor is gone from the layout; only its successor (and the destination) is alive.
    auto jobDirectory = New<TFixedJobDirectory>(THashSet<TJobId>{liveSourceJob, destinationJob});
    auto state = New<TMessageDistributorState>(jobDirectory);

    auto actionQueue = New<TActionQueue>("DistTest");
    auto serializedInvoker = CreateSerializedInvoker(actionQueue->GetInvoker());

    auto connection = New<TWorkerConnection>(
        state,
        // Never invoked: we drive DoSend/DoStopJob directly, without Start().
        NRpc::CreateNullChannel("destination-worker"),
        /*workerAddress*/ "destination-worker",
        New<TStreamSpecStorage>(/*converterCache*/ nullptr),
        serializedInvoker,
        /*poolInvoker*/ actionQueue->GetInvoker(),
        NProfiling::TSensorsOwner());

    // Owned by the callbacks (captured by value), so a flag outlives the test stack no matter when or
    // on which thread the callback fires or is dropped — no dangling reference.
    auto predecessorCallbackFired = std::make_shared<std::atomic<bool>>(false);
    auto successorCallbackFired = std::make_shared<std::atomic<bool>>(false);

    // Earlier copy: produced by the now-dead predecessor; its callback is moot.
    auto predecessorTask = MakeRoutedTask({.MessageId = TMessageId("dup")}, {DefaultSystemTimestamp, 0});
    predecessorTask.DestinationJobId = destinationJob;
    predecessorTask.Task.SourceJobId = deadSourceJob;
    predecessorTask.Task.OnDistributed = TOnDistributedCallback::FromCallback([flag = predecessorCallbackFired] {
        flag->store(true);
    });

    // Duplicate: the live successor replaying the same message; its callback must survive.
    auto successorTask = MakeRoutedTask({.MessageId = TMessageId("dup")}, {DefaultSystemTimestamp, 1});
    successorTask.DestinationJobId = destinationJob;
    successorTask.Task.SourceJobId = liveSourceJob;
    successorTask.Task.OnDistributed = TOnDistributedCallback::FromCallback([flag = successorCallbackFired] {
        flag->store(true);
    });

    connection->Send(std::move(predecessorTask));
    connection->Send(std::move(successorTask));

    // StopJob runs on the serialized invoker after both DoSend calls and returns the surviving task.
    // AsVoid()/WaitFor blocks for it without copying the move-only result; GetOrCrash then moves it out.
    auto stopJobFuture = connection->StopJob(destinationJob);
    WaitFor(stopJobFuture.AsVoid()).ThrowOnError();
    auto reroutedTasks = stopJobFuture.AsUnique().GetOrCrash().ValueOrThrow();

    ASSERT_EQ(reroutedTasks.size(), 1u);
    auto& reroutedTask = reroutedTasks[0];

    // The dedup adopted the live successor onto the surviving task: both its SourceJobId and its
    // OnDistributed callback, not the dead predecessor's.
    EXPECT_EQ(reroutedTask.SourceJobId, liveSourceJob);
    ASSERT_TRUE(static_cast<bool>(reroutedTask.OnDistributed));
    reroutedTask.OnDistributed();
    EXPECT_TRUE(successorCallbackFired->load());
    EXPECT_FALSE(predecessorCallbackFired->load());

    connection->Stop(/*checkJobsStopped*/ false);
}

////////////////////////////////////////////////////////////////////////////////

// A stream that goes idle (its statistics drop to empty) must keep its previously computed bias:
// it may reactivate before the next recompute, and ordering must never fall back to a zero bias.
TEST(TMessageDistributorTest, OrderingTimestampBiasesKeepLastNonEmpty)
{
    auto state = New<TMessageDistributorState>(New<TFixedJobDirectory>(THashSet<TJobId>{}));

    const TComputationId computationId("comp");
    const TStreamId streamId("s");

    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds = {streamId};
    computationSpec->InputOrdering = New<TInputOrderingSpec>();
    auto pipelineSpec = New<TPipelineSpec>();
    pipelineSpec->Computations[computationId] = computationSpec;
    state->PipelineSpec.Store(pipelineSpec);

    const std::pair key(streamId, computationId);

    // No statistics yet: the stream has no bias.
    state->RecomputeOrderingTimestampBiases();
    EXPECT_FALSE(state->GetOrderingTimestampBiases()->Biases.contains(key));

    TMessageMeta meta;
    meta.EventTimestamp = TSystemTimestamp(110);
    meta.AlignmentTimestamp = TSystemTimestamp(100);
    const auto registrationInfo = TTimestampStatistics::ComputeRegistrationInfo(meta);

    auto [statistics, created] = state->GetOrCreateStreamStatistics(streamId);
    EXPECT_TRUE(created);

    {
        // A live task keeps the stream's statistics non-empty (alignment->event bias of 10).
        TTimestampStatisticsGuard guard(std::move(statistics), registrationInfo);

        state->RecomputeOrderingTimestampBiases();
        auto biases = state->GetOrderingTimestampBiases();
        ASSERT_TRUE(biases->Biases.contains(key));
        EXPECT_DOUBLE_EQ(biases->Biases.at(key), 10.0);
    }

    // The task is gone, the stream's statistics are empty, but the bias must be preserved.
    state->RecomputeOrderingTimestampBiases();
    auto biases = state->GetOrderingTimestampBiases();
    ASSERT_TRUE(biases->Biases.contains(key));
    EXPECT_DOUBLE_EQ(biases->Biases.at(key), 10.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
