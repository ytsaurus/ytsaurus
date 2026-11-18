#include "adapter_test_context.h"

#include <yt/yt/flow/library/cpp/process_function/host/computation.h>
#include <yt/yt/flow/library/cpp/process_function/host/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/computation/async_processor_manager.h>

#include <yt/yt/flow/library/cpp/common/async_processor.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NTesting {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TProbeTask
    : public TYsonMessage
{
    REGISTER_YSON_STRUCT(TProbeTask);

    static void Register(TRegistrar /*registrar*/)
    { }
};

struct TProbeResult
    : public TYsonMessage
{
    i64 Value = 0;

    REGISTER_YSON_STRUCT(TProbeResult);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

//! Registers an async processor whose result handler records what it gets.
class TAsyncProbeFunction
    : public IProcessFunction
{
public:
    TAsyncProcessorDescriptorPtr Descriptor;
    std::vector<i64> Values;
    std::vector<TWatermarkStatePtr> WatermarkStates;
    std::vector<IOutputCollectorPtr> Outputs;

    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitAsyncProcessor(
            Processor_,
            "probe",
            BIND([] (TIntrusivePtr<TProbeTask> /*task*/) {
                return MakeFuture(New<TProbeResult>());
            }),
            BIND(&TAsyncProbeFunction::OnResult, Unretained(this)));
        Descriptor = Processor_.GetDescriptor();
    }

private:
    TAsyncProcessorHandle<TProbeTask, TProbeResult> Processor_;

    void OnResult(
        const TCompletedAsyncTask<TProbeTask, TProbeResult>& completedTask,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context)
    {
        auto result = completedTask.GetResult()
            .ValueOrThrow();
        Values.push_back(result->Value);
        WatermarkStates.push_back(context->GetEpochWatermarkState());
        Outputs.push_back(output);
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TAsyncProbeFunction);

//! Sends a task from every timer and counts the tasks of the key in its state. The result handler sends
//! one more task after a failed one; otherwise it adds a timer whose trigger is that count.
//! The failed tasks alternate between a null future and an exception.
class TAsyncRetryFunction
    : public IProcessFunction
{
public:
    //! The number of tasks that fail before one succeeds.
    int FailuresLeft = 0;
    std::vector<IRuntimeContextPtr> ResultContexts;

    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient<i64>(Attempts_, "attempts");
        initContext->InitAsyncProcessor(
            Processor_,
            "probe",
            BIND(&TAsyncRetryFunction::Start, Unretained(this)),
            BIND(&TAsyncRetryFunction::OnResult, Unretained(this)));
    }

    // A message cannot be the root of a task.
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& /*context*/) override
    {
        output->Add(Processor_, New<TProbeTask>());
    }

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& /*context*/) override
    {
        AddTask(timer, output);
    }

private:
    TMutableStateKeyClient<i64> Attempts_;
    TAsyncProcessorHandle<TProbeTask, TProbeResult> Processor_;

    void AddTask(const TInputTimerConstPtr& timer, const IOutputCollectorPtr& output)
    {
        auto attempts = Attempts_.GetState(timer->Key);
        *attempts = *attempts + 1;
        output->Add(Processor_, New<TProbeTask>());
    }

    TFuture<TIntrusivePtr<TProbeResult>> Start(TIntrusivePtr<TProbeTask> /*task*/)
    {
        if (FailuresLeft > 0) {
            if (--FailuresLeft % 2 == 0) {
                THROW_ERROR_EXCEPTION("Probe failed");
            }
            return {};
        }
        return MakeFuture(New<TProbeResult>());
    }

    void OnResult(
        const TCompletedAsyncTask<TProbeTask, TProbeResult>& completedTask,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context)
    {
        ResultContexts.push_back(context);
        const auto& timer = completedTask.GetRootTimer();
        if (!completedTask.GetResult().IsOK()) {
            AddTask(timer, output);
            return;
        }
        output->AddTimer(TSystemTimestamp(*Attempts_.GetState(timer->Key)));
    }
};

//! Adds |TasksPerEpoch| tasks and |TimersPerEpoch| timers right in the batch, so they descend from all
//! the inputs of the epoch.
class TAsyncBatchFunction
    : public IBatchProcessFunction
{
public:
    int TasksPerEpoch = 1;
    int TimersPerEpoch = 0;

    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitAsyncProcessor(
            Processor_,
            "probe",
            BIND([] (TIntrusivePtr<TProbeTask> /*task*/) {
                return MakeFuture(New<TProbeResult>());
            }),
            BIND([] (const TCompletedAsyncTask<TProbeTask, TProbeResult>& /*completedTask*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/) {
            }));
    }

    void Process(
        const IInputContextPtr& /*input*/,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& /*context*/) override
    {
        for (int index = 0; index < TasksPerEpoch; ++index) {
            output->Add(Processor_, New<TProbeTask>());
        }
        for (int index = 0; index < TimersPerEpoch; ++index) {
            output->AddTimer(TSystemTimestamp(index + 1));
        }
    }

private:
    TAsyncProcessorHandle<TProbeTask, TProbeResult> Processor_;
};

//! Registers two async processors under the same name.
class TDuplicateAsyncProcessorFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        for (auto* processor : {&First_, &Second_}) {
            initContext->InitAsyncProcessor(
                *processor,
                "probe",
                BIND([] (TIntrusivePtr<TProbeTask> /*task*/) {
                    return MakeFuture(New<TProbeResult>());
                }),
                BIND([] (const TCompletedAsyncTask<TProbeTask, TProbeResult>& /*completedTask*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/) {
                }));
        }
    }

private:
    TAsyncProcessorHandle<TProbeTask, TProbeResult> First_;
    TAsyncProcessorHandle<TProbeTask, TProbeResult> Second_;
};

// ApplyPendingStates and Function_ are protected, hence this exposing subclass of the registered adapter.
class TSeededAdapterComputation
    : public TProcessFunctionComputation
{
public:
    using TProcessFunctionComputation::ApplyPendingStates;
    using TProcessFunctionComputation::Function_;
    using TProcessFunctionComputation::TProcessFunctionComputation;
};

TComputationSpecPtr MakeSpec(TStringBuf computationClassName)
{
    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = computationClassName;
    spec->ProcessingFunction = std::string(TypeName<TAsyncProbeFunction>());
    return spec;
}

TUntypedCompletedAsyncTaskPtr MakeCompletedTask(TAsyncProcessorDescriptorPtr processor, i64 value)
{
    auto result = New<TProbeResult>();
    result->Value = value;
    auto task = New<TUntypedCompletedAsyncTask>();
    task->Processor = std::move(processor);
    task->Task = New<TProbeTask>();
    task->Result = TYsonMessagePtr(std::move(result));
    return task;
}

////////////////////////////////////////////////////////////////////////////////

// The results are processed before DoProcess(), which is where the adapter refreshes the runtime
// context for the other hooks, so every result handler must see the state of its own epoch.
TEST(TProcessFunctionAsyncProcessorTest, ResultHandlerGetsContextOfCurrentEpoch)
{
    auto queue = New<TActionQueue>("AsyncProcessorTest");
    auto invoker = queue->GetInvoker();
    auto context = MakeAdapterTestComputationContext(invoker, MakeSpec("NYT::NFlow::TProcessFunctionComputation"));
    auto dynamicContext = MakeAdapterTestDynamicComputationContext();
    TTestStateEnvironment environment;
    auto manager = New<TAsyncProcessorManager>(invoker, NProfiling::TProfiler(), NLogging::TLogger("Test"));

    TIntrusivePtr<TAsyncProbeFunction> function;
    std::vector<TWatermarkStatePtr> watermarkStates;
    std::vector<IOutputCollectorPtr> outputs;
    WaitFor(
        BIND([&] {
            auto computation = New<TSeededAdapterComputation>(context, dynamicContext);
            computation->DoInit(environment.GetStateManager()->CreateContext(/*prefix*/ {}, manager));
            function = DynamicPointerCast<TAsyncProbeFunction>(computation->Function_);
            ASSERT_TRUE(function);
            for (i64 value : {1, 2}) {
                watermarkStates.push_back(New<TWatermarkState>());
                computation->UpdateWatermarkState(watermarkStates.back());
                computation->ApplyPendingStates();
                outputs.push_back(New<TRecordingOutputCollector>());
                function->Descriptor->ResultHandler(MakeCompletedTask(function->Descriptor, value), outputs.back());
            }
            manager->Stop();
        }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();

    ASSERT_TRUE(function);
    EXPECT_EQ(function->Values, std::vector<i64>({1, 2}));
    EXPECT_EQ(function->WatermarkStates, watermarkStates);
    EXPECT_EQ(function->Outputs, outputs);
}

// The job context of a computation without async processors has no manager.
TEST(TProcessFunctionAsyncProcessorTest, HostRejectsAsyncProcessorsWithoutManager)
{
    auto queue = New<TActionQueue>("AsyncProcessorTest");
    auto invoker = queue->GetInvoker();
    auto context = MakeAdapterTestComputationContext(
        invoker,
        MakeSpec("NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation"));
    auto dynamicContext = MakeAdapterTestDynamicComputationContext();
    TTestStateEnvironment environment;

    auto result = WaitFor(
        BIND([&] {
            auto computation = New<TProcessFunctionTransformOrderedSourceComputation>(context, dynamicContext);
            computation->DoInit(environment.GetStateManager()->CreateContext());
        }).AsyncVia(invoker)
            .Run());
    EXPECT_THROW_WITH_SUBSTRING(result.ThrowOnError(), "Async processors are not supported by this computation");
}

// Without the provider the results would have no context, so the processor is rejected at Init.
TEST(TProcessFunctionAsyncProcessorTest, HostRejectsAsyncProcessorsWithoutResultHandlerContext)
{
    auto queue = New<TActionQueue>("AsyncProcessorTest");
    auto invoker = queue->GetInvoker();
    TTestStateEnvironment environment;
    auto manager = New<TAsyncProcessorManager>(invoker, NProfiling::TProfiler(), NLogging::TLogger("Test"));
    auto initContext = New<TRuntimeInitContext>(
        environment.GetStateManager()->CreateContext(/*prefix*/ {}, manager),
        environment.GetStateManager(),
        environment.GetComputationId(),
        environment.GetPartitionId());

    EXPECT_THROW_WITH_SUBSTRING(
        New<TAsyncProbeFunction>()->Init(initContext),
        "Async processors are not supported by this host of the process function");
}

// The harness runs the tasks of an epoch in the next one, as the worker does after a commit.
TEST(TProcessFunctionAsyncProcessorTest, HarnessRunsTasksAndPassesResultsToHandlers)
{
    TTestStateEnvironment environment;
    auto function = New<TAsyncRetryFunction>();
    function->FailuresLeft = 2;
    TProcessFunctionTestHarness harness(environment, function);
    auto timer = MakeTestTimer(MakeKey<ui64>(1), TSystemTimestamp(1));

    harness.RunEpoch({}, {timer});
    ASSERT_EQ(std::ssize(harness.GetTasks()), 1);
    EXPECT_EQ(harness.GetTasks()[0].RootTimer, timer);
    EXPECT_TRUE(function->ResultContexts.empty());

    // The task fails, so its result handler sends one more task of the same timer.
    for (int failure = 0; failure < 2; ++failure) {
        harness.RunEpoch({}, {}, {});
        ASSERT_EQ(std::ssize(harness.GetTasks()), 1);
        EXPECT_EQ(harness.GetTasks()[0].RootTimer, timer);
        EXPECT_TRUE(harness.GetTimers().empty());
    }

    // The handler reads the state of the key of the root timer, which is not an input of the epoch.
    harness.RunEpoch({}, {}, {});
    EXPECT_TRUE(harness.GetTasks().empty());
    ASSERT_EQ(std::ssize(harness.GetTimers()), 1);
    EXPECT_EQ(harness.GetTimers()[0].TriggerTimestamp, TSystemTimestamp(3));
    EXPECT_EQ(harness.GetTimers()[0].ParentIds, std::vector{timer->MessageId});
    EXPECT_EQ(function->ResultContexts, std::vector<IRuntimeContextPtr>(3, harness.GetContext()));

    harness.RunEpoch({}, {}, {});
    EXPECT_TRUE(harness.GetTimers().empty());
    EXPECT_EQ(std::ssize(function->ResultContexts), 3);
}

// The timer adds its task before the message fails the epoch; an epoch that did not commit starts no task.
TEST(TProcessFunctionAsyncProcessorTest, HarnessRejectsTaskWithoutRootTimer)
{
    TTestStateEnvironment environment;
    auto function = New<TAsyncRetryFunction>();
    TProcessFunctionTestHarness harness(environment, function);
    auto message = MakeTestMessage("input", MakeKey<ui64>(1), New<NTableClient::TTableSchema>());
    auto timer = MakeTestTimer(MakeKey<ui64>(1), TSystemTimestamp(1));

    EXPECT_THROW_WITH_SUBSTRING(
        harness.RunEpoch({message}, {timer}),
        "must have a single parent, a swift timer");
    EXPECT_EQ(std::ssize(harness.GetTasks()), 1);

    harness.RunEpoch({}, {}, {});
    EXPECT_TRUE(function->ResultContexts.empty());
}

// As in the worker, the batch gets a collector whose parents are the inputs of the epoch.
TEST(TProcessFunctionAsyncProcessorTest, HarnessPassesInputsAsParentsOfBatch)
{
    TTestStateEnvironment environment;
    TProcessFunctionTestHarness harness(environment, New<TAsyncBatchFunction>());
    auto timer = MakeTestTimer(MakeKey<ui64>(1), TSystemTimestamp(1));

    harness.RunEpoch({}, {timer});
    ASSERT_EQ(std::ssize(harness.GetTasks()), 1);
    EXPECT_EQ(harness.GetTasks()[0].RootTimer, timer);
}

TEST(TProcessFunctionAsyncProcessorTest, HarnessRejectsTwoTasksOfTimerInEpoch)
{
    TTestStateEnvironment environment;
    auto function = New<TAsyncBatchFunction>();
    function->TasksPerEpoch = 2;
    TProcessFunctionTestHarness harness(environment, function);

    EXPECT_THROW_WITH_SUBSTRING(
        harness.RunEpoch({}, {MakeTestTimer(MakeKey<ui64>(1), TSystemTimestamp(1))}),
        "gets two tasks in one epoch");
}

// As in the worker, a timer that gets a task may not emit outputs: a restart would replay them.
TEST(TProcessFunctionAsyncProcessorTest, HarnessRejectsOutputsOfTimerWithTask)
{
    TTestStateEnvironment environment;
    auto function = New<TAsyncBatchFunction>();
    function->TimersPerEpoch = 1;
    TProcessFunctionTestHarness harness(environment, function);

    EXPECT_THROW_WITH_SUBSTRING(
        harness.RunEpoch({}, {MakeTestTimer(MakeKey<ui64>(1), TSystemTimestamp(1))}),
        "adds a task and a timer in one epoch");
}

TEST(TProcessFunctionAsyncProcessorTest, HarnessChecksAsyncProcessorNamesOfEachInit)
{
    TTestStateEnvironment environment;
    TProcessFunctionTestHarness duplicate(environment, New<TDuplicateAsyncProcessorFunction>());
    EXPECT_THROW_WITH_SUBSTRING(duplicate.RunEpoch({}, {}, {}), "Async processor \"probe\" is registered twice");

    // The names are checked per Init, so several functions of the environment may use the same name.
    for (int job = 0; job < 2; ++job) {
        TProcessFunctionTestHarness harness(environment, New<TAsyncBatchFunction>());
        harness.RunEpoch({}, {MakeTestTimer(MakeKey<ui64>(1), TSystemTimestamp(1))});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NTesting
