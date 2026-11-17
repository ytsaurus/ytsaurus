#include "computation_test_harness.h"

#include <yt/yt/flow/library/cpp/computation/async_processor_manager.h>

#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>

#include <yt/yt/flow/library/cpp/common/async_processor.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <type_traits>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

const TStreamId RootStreamId("fetch_timer");
const TStreamId OtherRootStreamId("other_timer");

struct TTestTask
    : public TYsonMessage
{
    i64 Id = 0;

    REGISTER_YSON_STRUCT(TTestTask);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("id", &TThis::Id)
            .Default();
    }
};

using TTestTaskPtr = TIntrusivePtr<TTestTask>;

struct TTestResult
    : public TYsonMessage
{
    i64 Value = 0;

    REGISTER_YSON_STRUCT(TTestResult);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

using TTestResultPtr = TIntrusivePtr<TTestResult>;

using TTestHandle = TAsyncProcessorHandle<TTestTask, TTestResult>;

// The task type of #IOutputCollector::Add() comes from the handle only, so a raw task pointer converts to it.
static_assert(requires(IOutputCollector& output, const TTestHandle& handle, TTestTask* task) {
    output.Add(handle, task);
});

//! A result that the manager fails to measure: saving it throws.
struct TUnmeasurableResult
    : public TYsonMessage
{
    NComputationTesting::TUnserializable Value;

    REGISTER_YSON_STRUCT(TUnmeasurableResult);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

using TUnmeasurableResultPtr = TIntrusivePtr<TUnmeasurableResult>;

//! Returns the swift timer #index of |streamId| with distinct timestamps.
TInputTimerConstPtr MakeRootTimer(int index, const TStreamId& streamId = RootStreamId)
{
    TTimer timer;
    timer.MessageId = TMessageId(Format("root-%v", index));
    timer.StreamId = streamId;
    timer.SystemTimestamp = TSystemTimestamp(100 + index);
    timer.AlignmentTimestamp = TSystemTimestamp(200 + index);
    timer.EventTimestamp = TSystemTimestamp(300 + index);
    timer.TriggerTimestamp = TSystemTimestamp(1);
    timer.KeySchema = NComputationTesting::GetTestKeySchema();
    timer.Key = MakeKey(static_cast<ui64>(index));
    return New<TInputTimer>(std::move(timer), NComputationTesting::GetTestKeySchema());
}

TTestTaskPtr MakeTask(i64 id)
{
    auto task = New<TTestTask>();
    task->Id = id;
    return task;
}

TTestResultPtr MakeResult(i64 value)
{
    auto result = New<TTestResult>();
    result->Value = value;
    return result;
}

//! Returns the id of the task of |task| and the value of its result, which must be successful.
std::pair<i64, i64> GetTaskIdAndValue(const TUntypedCompletedAsyncTask& task)
{
    return {
        StaticPointerCast<TTestTask>(task.Task)->Id,
        StaticPointerCast<TTestResult>(task.Result.ValueOrThrow())->Value,
    };
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncProcessorManagerTest
    : public ::testing::Test
{
protected:
    //! A task started by the start callback of #InitProcessor().
    struct TTaskControl
    {
        TPromise<TTestResultPtr> Result = NewPromise<TTestResultPtr>();
        //! Set once the manager cancels the task.
        TPromise<void> Canceled = NewPromise<void>();
    };

    //! The limits of a processor; an unset limit keeps its default.
    struct TLimits
    {
        std::optional<i64> MaxRunningTasks;
        std::optional<i64> MaxReadyByteSize;
        std::optional<TDuration> Timeout;
    };

    const TActionQueuePtr Queue_ = New<TActionQueue>("AsyncProcessorManagerTest");
    const TAsyncProcessorManagerPtr Manager_ = New<TAsyncProcessorManager>(
        Queue_->GetInvoker(),
        NProfiling::TProfiler(),
        NLogging::TLogger("AsyncProcessorManagerTest"));

    //! The limits of the registered processors by their names, as the dynamic spec has them.
    THashMap<std::string, TDynamicAsyncProcessorSpecPtr> Specs_;

    //! The started tasks by task ids; filled on the queue by the start callback of #InitProcessor().
    THashMap<i64, TTaskControl> Tasks_;
    //! The ids of the started tasks, in start order.
    std::vector<i64> StartedTaskIds_;

    void TearDown() override
    {
        Stop();
        Queue_->Shutdown();
    }

    //! Runs |functor| on the queue of the manager and returns its result.
    template <class TFunctor>
    auto RunOnQueue(TFunctor functor)
    {
        auto future = BIND(std::move(functor))
            .AsyncVia(Queue_->GetInvoker())
            .Run();
        if constexpr (std::is_void_v<std::invoke_result_t<TFunctor>>) {
            WaitFor(future).ThrowOnError();
        } else {
            return WaitFor(future).ValueOrThrow();
        }
    }

    //! Returns the registered processor |name| with |limits| whose tasks complete when the test sets their promises,
    //! see #Complete(). The result handler does nothing unless |resultHandler| is set.
    TTestHandle InitProcessor(TStringBuf name = "fetcher", const TLimits& limits = {}, TTestHandle::TResultHandler resultHandler = {})
    {
        TTestHandle handle;
        InitProcessor(
            handle,
            name,
            BIND([this] (TIntrusivePtr<TTestTask> task) {
                StartedTaskIds_.push_back(task->Id);
                TTaskControl control;
                control.Result.OnCanceled(BIND([canceled = control.Canceled] (const TError& /*error*/) {
                    canceled.TrySet();
                }));
                EmplaceOrCrash(Tasks_, task->Id, control);
                return control.Result.ToFuture();
            }),
            limits,
            std::move(resultHandler));
        return handle;
    }

    //! Initializes |handle| as the registered processor |name| with |limits|, as a computation does in #DoInit().
    //! The result handler does nothing unless |resultHandler| is set.
    template <class TTask, class TResult>
    void InitProcessor(
        TAsyncProcessorHandle<TTask, TResult>& handle,
        TStringBuf name,
        typename TAsyncProcessorHandle<TTask, TResult>::TStart start,
        const TLimits& limits = {},
        typename TAsyncProcessorHandle<TTask, TResult>::TResultHandler resultHandler = {})
    {
        if (!resultHandler) {
            resultHandler = BIND([] (const TCompletedAsyncTask<TTask, TResult>& /*completedTask*/, IOutputCollectorPtr /*output*/) {
            });
        }
        RunOnQueue([&] {
            New<TJobInitContext>(/*stateManager*/ nullptr, /*prefix*/ "", Manager_)
                ->InitAsyncProcessor(handle, name, std::move(start), std::move(resultHandler));
        });
        Specs_[std::string(name)] = MakeSpec(limits);
        Reconfigure(Specs_);
    }

    void Reconfigure(const THashMap<std::string, TDynamicAsyncProcessorSpecPtr>& specs)
    {
        RunOnQueue([&] {
            Manager_->Reconfigure(specs);
        });
    }

    void Start(const TTestHandle& handle, const std::vector<std::pair<i64, int>>& tasks)
    {
        Start(handle.GetDescriptor(), tasks);
    }

    //! Starts the task #taskId for each pair of |tasks| with the root timer #rootTimerIndex.
    void Start(const TAsyncProcessorDescriptorPtr& descriptor, const std::vector<std::pair<i64, int>>& tasks)
    {
        std::vector<TAsyncTaskRequest> requests;
        for (auto [taskId, rootTimerIndex] : tasks) {
            requests.push_back(MakeRequest(descriptor, MakeTask(taskId), MakeRootTimer(rootTimerIndex)));
        }
        StartRequests(std::move(requests));
    }

    void StartRequests(std::vector<TAsyncTaskRequest> requests)
    {
        RunOnQueue([&] {
            Manager_->Start(std::move(requests));
        });
    }

    //! Completes the task #taskId with the result |value| and waits until the manager takes the completion.
    void Complete(i64 taskId, i64 value)
    {
        GetOrCrash(Tasks_, taskId).Result.Set(MakeResult(value));
        Sync();
    }

    //! Waits until the manager runs the actions queued so far, e.g. its completion handlers.
    void Sync()
    {
        RunOnQueue([] {
        });
    }

    std::vector<TUntypedCompletedAsyncTaskPtr> TakeReady(
        i64 maxRows = 1000,
        i64 maxByteSize = 1_GB,
        THashSet<TStreamId> allowedRootStreams = {RootStreamId, OtherRootStreamId})
    {
        return RunOnQueue([=, this] {
            return Manager_->TakeReady(allowedRootStreams, maxRows, maxByteSize);
        });
    }

    void Stop()
    {
        RunOnQueue([this] {
            Manager_->Stop();
        });
    }

    THashMap<TStreamId, TBatchStatistics> GetReadyByRootStream()
    {
        return RunOnQueue([this] {
            return Manager_->GetReadyByRootStream();
        });
    }

    std::vector<std::string> GetSaturatedProcessors()
    {
        return RunOnQueue([this] {
            return Manager_->GetSaturatedProcessors();
        });
    }

    bool IsSaturated()
    {
        return !GetSaturatedProcessors().empty();
    }

    static TAsyncTaskRequest MakeRequest(
        const TAsyncProcessorDescriptorPtr& descriptor,
        TYsonMessagePtr task,
        TInputTimerConstPtr rootTimer)
    {
        return {
            .Processor = descriptor,
            .Task = std::move(task),
            .RootTimer = std::move(rootTimer),
        };
    }

    static TDynamicAsyncProcessorSpecPtr MakeSpec(const TLimits& limits)
    {
        auto spec = New<TDynamicAsyncProcessorSpec>();
        if (limits.MaxRunningTasks) {
            spec->MaxRunningTasks = NYTree::TSize(*limits.MaxRunningTasks);
        }
        if (limits.MaxReadyByteSize) {
            spec->MaxReadyByteSize = NYTree::TSize(*limits.MaxReadyByteSize);
        }
        spec->Timeout = limits.Timeout;
        return spec;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAsyncProcessorManagerTest, ResultsComeBackInCompletionOrderWithTheirTasksAndRootTimers)
{
    auto handle = InitProcessor();
    Start(handle, {{1, 1}, {2, 2}, {3, 3}});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2, 3));
    EXPECT_TRUE(TakeReady().empty());

    Complete(2, 20);
    Complete(3, 30);
    Complete(1, 10);

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 3);
    const std::vector<std::pair<i64, i64>> expected = {{2, 20}, {3, 30}, {1, 10}};
    for (int index = 0; index < std::ssize(ready); ++index) {
        const auto& task = *ready[index];
        EXPECT_EQ(task.Processor.Get(), handle.GetDescriptor().Get());
        // The task #i has the root timer #i.
        EXPECT_EQ(task.RootTimer->MessageId, MakeRootTimer(expected[index].first)->MessageId);
        EXPECT_EQ(GetTaskIdAndValue(task), expected[index]);
    }
    EXPECT_TRUE(TakeReady().empty());
}

TEST_F(TAsyncProcessorManagerTest, ResultHandlerGetsTaskWithResult)
{
    struct THandled
    {
        i64 TaskId;
        i64 Value;
        TMessageId RootTimerId;
    };

    std::vector<THandled> handled;
    auto handle = InitProcessor(
        "fetcher",
        /*limits*/ {},
        BIND([&] (const TCompletedAsyncTask<TTestTask, TTestResult>& completedTask, IOutputCollectorPtr /*output*/) {
            handled.push_back({
                .TaskId = completedTask.GetTask().Id,
                .Value = completedTask.GetResult().ValueOrThrow()->Value,
                .RootTimerId = completedTask.GetRootTimer()->MessageId,
            });
        }));
    Start(handle, {{1, 1}});
    Complete(1, 10);

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 1);
    ready[0]->Processor->ResultHandler(ready[0], /*output*/ nullptr);
    ASSERT_EQ(std::ssize(handled), 1);
    EXPECT_EQ(handled[0].TaskId, 1);
    EXPECT_EQ(handled[0].Value, 10);
    EXPECT_EQ(handled[0].RootTimerId, MakeRootTimer(1)->MessageId);
}

TEST_F(TAsyncProcessorManagerTest, StopDropsStartAndResultHandler)
{
    auto handle = InitProcessor();
    ASSERT_TRUE(handle.GetDescriptor()->Start);
    ASSERT_TRUE(handle.GetDescriptor()->ResultHandler);

    Stop();
    EXPECT_FALSE(handle.GetDescriptor()->Start);
    EXPECT_FALSE(handle.GetDescriptor()->ResultHandler);
}

TEST_F(TAsyncProcessorManagerTest, TakeReadyRespectsMaxRows)
{
    auto handle = InitProcessor();
    Start(handle, {{1, 1}, {2, 2}, {3, 3}});
    Complete(1, 10);
    Complete(2, 20);
    Complete(3, 30);

    auto first = TakeReady(/*maxRows*/ 2);
    ASSERT_EQ(std::ssize(first), 2);
    EXPECT_EQ(first[0]->RootTimer->MessageId, MakeRootTimer(1)->MessageId);
    EXPECT_EQ(first[1]->RootTimer->MessageId, MakeRootTimer(2)->MessageId);
    auto second = TakeReady(/*maxRows*/ 2);
    ASSERT_EQ(std::ssize(second), 1);
    EXPECT_EQ(second[0]->RootTimer->MessageId, MakeRootTimer(3)->MessageId);
}

TEST_F(TAsyncProcessorManagerTest, TakeReadyStopsBeforeExceedingMaxByteSize)
{
    auto handle = InitProcessor();
    // The results have equal byte sizes.
    Start(handle, {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}});
    for (int taskId = 1; taskId <= 5; ++taskId) {
        Complete(taskId, 10 * taskId);
    }

    // The first result is taken even if it alone exceeds the limit.
    auto first = TakeReady(/*maxRows*/ 10, /*maxByteSize*/ 1);
    ASSERT_EQ(std::ssize(first), 1);
    EXPECT_EQ(first[0]->RootTimer->MessageId, MakeRootTimer(1)->MessageId);
    const auto byteSize = first[0]->ByteSize;
    EXPECT_GT(byteSize, 1);

    auto underLimit = TakeReady(/*maxRows*/ 10, /*maxByteSize*/ 2 * byteSize - 1);
    ASSERT_EQ(std::ssize(underLimit), 1);
    EXPECT_EQ(underLimit[0]->ByteSize, byteSize);

    auto atLimit = TakeReady(/*maxRows*/ 10, /*maxByteSize*/ 2 * byteSize);
    ASSERT_EQ(std::ssize(atLimit), 2);
    EXPECT_EQ(atLimit[0]->ByteSize, byteSize);
    EXPECT_EQ(atLimit[1]->ByteSize, byteSize);

    EXPECT_EQ(std::ssize(TakeReady()), 1);
}

TEST_F(TAsyncProcessorManagerTest, SaturatedProcessorsAreNamedInOrder)
{
    auto parser = InitProcessor("parser", {.MaxRunningTasks = 1});
    auto fetcher = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    auto idle = InitProcessor("idle", {.MaxRunningTasks = 1});
    EXPECT_THAT(GetSaturatedProcessors(), ::testing::IsEmpty());

    Start(parser, {{1, 1}});
    EXPECT_THAT(GetSaturatedProcessors(), ElementsAre("parser"));

    Start(fetcher, {{2, 2}});
    EXPECT_THAT(GetSaturatedProcessors(), ElementsAre("fetcher", "parser"));

    Complete(1, 10);
    EXPECT_THAT(GetSaturatedProcessors(), ElementsAre("fetcher"));
}

TEST_F(TAsyncProcessorManagerTest, SaturatedWhileReadyByteSizeReachesMaxReadyByteSize)
{
    auto handle = InitProcessor("fetcher", {.MaxReadyByteSize = 1});
    Start(handle, {{1, 1}});
    EXPECT_FALSE(IsSaturated());

    Complete(1, 10);
    EXPECT_TRUE(IsSaturated());

    EXPECT_EQ(std::ssize(TakeReady()), 1);
    EXPECT_FALSE(IsSaturated());
}

TEST_F(TAsyncProcessorManagerTest, TasksBeyondMaxRunningTasksWaitAndStartInOrderAsSlotsFree)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    Start(handle, {{1, 1}, {2, 2}, {3, 3}});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1));
    EXPECT_TRUE(IsSaturated());

    Complete(1, 10);
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));
    EXPECT_TRUE(IsSaturated());

    Complete(2, 20);
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2, 3));
    EXPECT_TRUE(IsSaturated());

    Complete(3, 30);
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2, 3));
    EXPECT_FALSE(IsSaturated());

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 3);
    for (int index = 0; index < std::ssize(ready); ++index) {
        const int taskId = index + 1;
        EXPECT_EQ(ready[index]->RootTimer->MessageId, MakeRootTimer(taskId)->MessageId);
        EXPECT_EQ(GetTaskIdAndValue(*ready[index]), (std::pair<i64, i64>(taskId, 10 * taskId)));
    }
}

TEST_F(TAsyncProcessorManagerTest, StopReleasesTaskAndRootTimerOfRunningTask)
{
    // The task ignores the cancelation, so it is still running after the stop.
    auto result = NewPromise<TTestResultPtr>();
    TTestHandle handle;
    InitProcessor(
        handle,
        "uncancelable",
        BIND([result] (TIntrusivePtr<TTestTask> /*task*/) {
            return result.ToFuture().ToUncancelable();
        }));
    TWeakPtr<TTestTask> weakTask;
    TWeakPtr<const TInputTimer> weakRootTimer;
    {
        auto task = MakeTask(1);
        auto rootTimer = MakeRootTimer(1);
        weakTask = task;
        weakRootTimer = rootTimer;
        StartRequests({MakeRequest(handle.GetDescriptor(), std::move(task), std::move(rootTimer))});
    }
    // Only the manager holds the task and its root timer.
    EXPECT_FALSE(weakTask.IsExpired());
    EXPECT_FALSE(weakRootTimer.IsExpired());

    Stop();
    EXPECT_FALSE(result.IsSet());
    EXPECT_TRUE(weakTask.IsExpired());
    EXPECT_TRUE(weakRootTimer.IsExpired());

    result.Set(MakeResult(10));
    Sync();
    EXPECT_TRUE(TakeReady().empty());
}

TEST_F(TAsyncProcessorManagerTest, PendingTaskIsNotStartedWhenRunningTaskCompletesAfterStop)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    Start(handle, {{1, 1}, {2, 2}});
    ASSERT_THAT(StartedTaskIds_, ElementsAre(1));

    Stop();
    EXPECT_TRUE(GetOrCrash(Tasks_, 1).Canceled.IsSet());
    // The task completes despite the cancelation.
    GetOrCrash(Tasks_, 1).Result.TrySet(MakeResult(10));
    Sync();

    EXPECT_THAT(StartedTaskIds_, ElementsAre(1));
    EXPECT_TRUE(TakeReady().empty());
}

TEST_F(TAsyncProcessorManagerTest, PendingTaskIsNotStartedByCompletionQueuedBeforeStop)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    Start(handle, {{1, 1}, {2, 2}});
    ASSERT_THAT(StartedTaskIds_, ElementsAre(1));

    // The completion of the task waits in the queue of the manager until the stop, as it does behind an epoch.
    RunOnQueue([this] {
        GetOrCrash(Tasks_, 1).Result.Set(MakeResult(10));
        Manager_->Stop();
    });
    Sync();

    EXPECT_THAT(StartedTaskIds_, ElementsAre(1));
    EXPECT_TRUE(TakeReady().empty());
}

TEST_F(TAsyncProcessorManagerTest, ThrowingStartYieldsErrorResult)
{
    TTestHandle handle;
    InitProcessor(
        handle,
        "throwing",
        BIND([] (TIntrusivePtr<TTestTask> /*task*/) -> TFuture<TTestResultPtr> {
            THROW_ERROR_EXCEPTION("Start failed");
        }));
    Start(handle, {{1, 1}});
    Sync();

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 1);
    EXPECT_EQ(ready[0]->RootTimer->MessageId, MakeRootTimer(1)->MessageId);
    ASSERT_FALSE(ready[0]->Result.IsOK());
    EXPECT_THAT(ToString(TError(ready[0]->Result)), HasSubstr("Start failed"));
    EXPECT_FALSE(IsSaturated());
}

TEST_F(TAsyncProcessorManagerTest, NullFutureOfHandleStartYieldsErrorResult)
{
    TTestHandle handle;
    InitProcessor(
        handle,
        "null",
        BIND([] (TIntrusivePtr<TTestTask> /*task*/) {
            return TFuture<TTestResultPtr>();
        }));
    Start(handle, {{1, 1}});
    Sync();

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 1);
    ASSERT_FALSE(ready[0]->Result.IsOK());
    EXPECT_THAT(ToString(TError(ready[0]->Result)), HasSubstr("null future"));
}

TEST_F(TAsyncProcessorManagerTest, UnmeasurableResultYieldsErrorResultAndReleasesSlot)
{
    auto result = NewPromise<TUnmeasurableResultPtr>();
    TAsyncProcessorHandle<TTestTask, TUnmeasurableResult> handle;
    InitProcessor(
        handle,
        "unmeasurable",
        BIND([result] (TIntrusivePtr<TTestTask> /*task*/) {
            return result.ToFuture();
        }),
        {.MaxRunningTasks = 1});
    Start(handle.GetDescriptor(), {{1, 1}});
    EXPECT_TRUE(IsSaturated());

    result.Set(New<TUnmeasurableResult>());
    Sync();
    EXPECT_FALSE(IsSaturated());

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 1);
    EXPECT_EQ(ready[0]->RootTimer->MessageId, MakeRootTimer(1)->MessageId);
    ASSERT_FALSE(ready[0]->Result.IsOK());
    const auto error = ToString(TError(ready[0]->Result));
    EXPECT_THAT(error, HasSubstr("Failed to measure"));
    EXPECT_THAT(error, HasSubstr("Value is not serializable"));
    // The error takes the place of the result, but the completed task keeps the task.
    EXPECT_EQ(StaticPointerCast<TTestTask>(ready[0]->Task)->Id, 1);
    EXPECT_GT(ready[0]->ByteSize, 0);
}

TEST_F(TAsyncProcessorManagerTest, PendingTaskWaitsWhileReadyByteSizeReachesMaxReadyByteSize)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1, .MaxReadyByteSize = 1});
    Start(handle, {{1, 1}, {2, 2}});
    ASSERT_THAT(StartedTaskIds_, ElementsAre(1));

    // The slot is free, but the result fills the ready bytes.
    Complete(1, 10);
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1));
    EXPECT_TRUE(IsSaturated());

    // The commit of the epoch that takes the result starts the waiting task.
    EXPECT_EQ(std::ssize(TakeReady()), 1);
    Start(handle, {});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));
    Start(handle, {});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));
}

TEST_F(TAsyncProcessorManagerTest, StopDropsPendingTasksAndReadyResults)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    TWeakPtr<TTestTask> weakReadyTask;
    TWeakPtr<TTestTask> weakPendingTask;
    {
        auto readyTask = MakeTask(1);
        auto pendingTask = MakeTask(3);
        weakReadyTask = readyTask;
        weakPendingTask = pendingTask;
        std::vector<TAsyncTaskRequest> requests;
        requests.push_back(MakeRequest(handle.GetDescriptor(), std::move(readyTask), MakeRootTimer(1)));
        requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(2), MakeRootTimer(2)));
        requests.push_back(MakeRequest(handle.GetDescriptor(), std::move(pendingTask), MakeRootTimer(3)));
        StartRequests(std::move(requests));
    }
    // The first task is done, the second one runs and the third one waits.
    Complete(1, 10);
    ASSERT_THAT(StartedTaskIds_, ElementsAre(1, 2));

    Stop();
    EXPECT_TRUE(GetOrCrash(Tasks_, 2).Canceled.IsSet());
    // The manager is alive but keeps neither the result nor the waiting task.
    EXPECT_TRUE(weakReadyTask.IsExpired());
    EXPECT_TRUE(weakPendingTask.IsExpired());
    EXPECT_TRUE(TakeReady().empty());
    EXPECT_TRUE(GetReadyByRootStream().empty());
    EXPECT_FALSE(IsSaturated());
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));
}

TEST_F(TAsyncProcessorManagerTest, TakeReadyLeavesResultsOfDisallowedRootStreamsInOrder)
{
    auto handle = InitProcessor();
    std::vector<TAsyncTaskRequest> requests;
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(1), MakeRootTimer(1, OtherRootStreamId)));
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(2), MakeRootTimer(2)));
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(3), MakeRootTimer(3, OtherRootStreamId)));
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(4), MakeRootTimer(4)));
    StartRequests(std::move(requests));
    for (int taskId = 1; taskId <= 4; ++taskId) {
        Complete(taskId, 10 * taskId);
    }

    EXPECT_TRUE(TakeReady(/*maxRows*/ 10, /*maxByteSize*/ 1_GB, /*allowedRootStreams*/ {}).empty());

    auto allowed = TakeReady(/*maxRows*/ 10, /*maxByteSize*/ 1_GB, /*allowedRootStreams*/ {RootStreamId});
    ASSERT_EQ(std::ssize(allowed), 2);
    EXPECT_EQ(allowed[0]->RootTimer->MessageId, MakeRootTimer(2)->MessageId);
    EXPECT_EQ(allowed[1]->RootTimer->MessageId, MakeRootTimer(4)->MessageId);
    EXPECT_EQ(GetOrCrash(GetReadyByRootStream(), OtherRootStreamId).Count, 2);

    auto rest = TakeReady();
    ASSERT_EQ(std::ssize(rest), 2);
    EXPECT_EQ(rest[0]->RootTimer->MessageId, MakeRootTimer(1, OtherRootStreamId)->MessageId);
    EXPECT_EQ(rest[1]->RootTimer->MessageId, MakeRootTimer(3, OtherRootStreamId)->MessageId);
}

TEST_F(TAsyncProcessorManagerTest, ReadyByRootStreamCountsResultsUntilTaken)
{
    auto handle = InitProcessor();
    std::vector<TAsyncTaskRequest> requests;
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(1), MakeRootTimer(1)));
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(2), MakeRootTimer(2, OtherRootStreamId)));
    requests.push_back(MakeRequest(handle.GetDescriptor(), MakeTask(3), MakeRootTimer(3)));
    StartRequests(std::move(requests));
    EXPECT_TRUE(GetReadyByRootStream().empty());
    for (int taskId = 1; taskId <= 3; ++taskId) {
        Complete(taskId, 10 * taskId);
    }

    const auto before = GetReadyByRootStream();
    ASSERT_EQ(std::ssize(before), 2);
    EXPECT_EQ(GetOrCrash(before, RootStreamId).Count, 2);
    EXPECT_EQ(GetOrCrash(before, OtherRootStreamId).Count, 1);

    // Taking a result changes only the entry of the stream of its root timer.
    auto first = TakeReady(/*maxRows*/ 1);
    ASSERT_EQ(std::ssize(first), 1);
    auto after = GetReadyByRootStream();
    EXPECT_EQ(GetOrCrash(after, RootStreamId).Count, 1);
    EXPECT_EQ(GetOrCrash(after, RootStreamId).ByteSize, GetOrCrash(before, RootStreamId).ByteSize - first[0]->ByteSize);
    EXPECT_EQ(GetOrCrash(after, OtherRootStreamId).Count, 1);
    EXPECT_EQ(GetOrCrash(after, OtherRootStreamId).ByteSize, GetOrCrash(before, OtherRootStreamId).ByteSize);

    // The entry goes away with its last result.
    auto second = TakeReady(/*maxRows*/ 1);
    ASSERT_EQ(std::ssize(second), 1);
    EXPECT_EQ(second[0]->RootTimer->StreamId, OtherRootStreamId);
    EXPECT_EQ(second[0]->ByteSize, GetOrCrash(before, OtherRootStreamId).ByteSize);
    after = GetReadyByRootStream();
    EXPECT_FALSE(after.contains(OtherRootStreamId));
    EXPECT_EQ(GetOrCrash(after, RootStreamId).Count, 1);

    EXPECT_EQ(std::ssize(TakeReady()), 1);
    EXPECT_TRUE(GetReadyByRootStream().empty());
}

TEST_F(TAsyncProcessorManagerTest, DuplicateProcessorNameThrowsOnRegister)
{
    InitProcessor("fetcher");
    EXPECT_THROW_WITH_SUBSTRING(
        InitProcessor("fetcher"),
        "registered twice");
}

TEST_F(TAsyncProcessorManagerTest, ReconfigureThrowsOnUnregisteredProcessor)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    EXPECT_THROW_WITH_SUBSTRING(
        Reconfigure({{"fetcher", MakeSpec({.MaxRunningTasks = 2})}, {"other", MakeSpec({})}}),
        "does not register");

    // The rejected specs are not applied.
    Start(handle, {{1, 1}, {2, 2}});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1));
}

TEST_F(TAsyncProcessorManagerTest, ReconfigureStartsWaitingTasksThatFitRaisedLimit)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 1});
    Start(handle, {{1, 1}, {2, 2}, {3, 3}});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1));

    Reconfigure({{"fetcher", MakeSpec({.MaxRunningTasks = 2})}});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));
    EXPECT_TRUE(IsSaturated());

    // A processor without a spec gets the default limits.
    Reconfigure({});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2, 3));
    EXPECT_FALSE(IsSaturated());
}

TEST_F(TAsyncProcessorManagerTest, ReconfigureLowersLimitWithoutCancelingRunningTasks)
{
    auto handle = InitProcessor("fetcher", {.MaxRunningTasks = 2});
    Start(handle, {{1, 1}, {2, 2}, {3, 3}});
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));

    Reconfigure({{"fetcher", MakeSpec({.MaxRunningTasks = 1})}});
    Complete(1, 10);
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2));
    Complete(2, 20);
    EXPECT_THAT(StartedTaskIds_, ElementsAre(1, 2, 3));
    EXPECT_EQ(std::ssize(TakeReady()), 2);
}

TEST_F(TAsyncProcessorManagerTest, ReconfiguredTimeoutAppliesToTasksStartedLater)
{
    auto handle = InitProcessor();
    Start(handle, {{1, 1}});

    Reconfigure({{"fetcher", MakeSpec({.MaxRunningTasks = 2, .Timeout = TDuration::MilliSeconds(50)})}});
    Start(handle, {{2, 2}});
    EXPECT_TRUE(IsSaturated());
    // On the timeout, the manager queues the error result before the task is canceled.
    WaitFor(GetOrCrash(Tasks_, 2).Canceled.ToFuture()).ThrowOnError();
    Sync();

    auto ready = TakeReady();
    ASSERT_EQ(std::ssize(ready), 1);
    EXPECT_EQ(ready[0]->RootTimer->MessageId, MakeRootTimer(2)->MessageId);
    EXPECT_TRUE(ready[0]->Result.FindMatching(NYT::EErrorCode::Timeout)) << ToString(TError(ready[0]->Result));
    EXPECT_FALSE(GetOrCrash(Tasks_, 1).Canceled.IsSet());
    // The timed out task frees its slot.
    EXPECT_FALSE(IsSaturated());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDynamicAsyncProcessorSpecTest, DefaultsAndNonPositiveLimits)
{
    auto parse = [] (TStringBuf yson) {
        return ConvertTo<TDynamicAsyncProcessorSpecPtr>(NYson::TYsonStringBuf(yson));
    };
    auto spec = parse("{}");
    EXPECT_EQ(spec->MaxRunningTasks, 100);
    EXPECT_EQ(spec->MaxReadyByteSize, static_cast<i64>(64_MB));
    EXPECT_FALSE(spec->Timeout);

    EXPECT_EQ(parse("{timeout=1500}")->Timeout, TDuration::MilliSeconds(1500));
    EXPECT_ANY_THROW(parse("{max_running_tasks=0}"));
    EXPECT_ANY_THROW(parse("{max_ready_byte_size=0}"));
    EXPECT_ANY_THROW(parse("{timeout=0}"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
