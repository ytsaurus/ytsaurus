#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/async_processor.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/system/file.h>

namespace NYT::NFlow::NAsyncProcessorTest {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TStreamId FetchTimerStreamId("fetch_timer");

//! The first attempt of every event whose id is a multiple of this fails.
constexpr i64 FailingEventPeriod = 4;
//! Long enough for the task to span several epochs.
constexpr auto TaskDuration = TDuration::Seconds(2);
constexpr auto TaskBarrierCheckPeriod = TDuration::MilliSeconds(100);
constexpr i64 ResultFactor = 10;

////////////////////////////////////////////////////////////////////////////////

struct TFetchTask
    : public TYsonMessage
{
    i64 EventId = 0;
    i64 Attempt = 0;

    REGISTER_YSON_STRUCT(TFetchTask);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("event_id", &TThis::EventId)
            .Default();
        registrar.Parameter("attempt", &TThis::Attempt)
            .Default();
    }
};

using TFetchTaskPtr = TIntrusivePtr<TFetchTask>;

struct TFetchResult
    : public TYsonMessage
{
    i64 Value = 0;

    REGISTER_YSON_STRUCT(TFetchResult);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

using TFetchResultPtr = TIntrusivePtr<TFetchResult>;

////////////////////////////////////////////////////////////////////////////////

struct TTransformAwaiterParameters
    : public TTransformComputation::TParameters
{
    //! Where the starts of the tasks are recorded.
    std::string TaskStartsPath;
    //! The file whose creation lets the tasks proceed, if set.
    std::string TaskBarrierPath;
    //! The first attempt that waits for the barrier; the earlier ones run at once.
    i64 TaskBarrierFromAttempt = 0;

    REGISTER_YSON_STRUCT(TTransformAwaiterParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("task_starts_path", &TThis::TaskStartsPath)
            .Default();
        registrar.Parameter("task_barrier_path", &TThis::TaskBarrierPath)
            .Default();
        registrar.Parameter("task_barrier_from_attempt", &TThis::TaskBarrierFromAttempt)
            .Default(0);
    }
};

using TTransformAwaiterParametersPtr = TIntrusivePtr<TTransformAwaiterParameters>;

////////////////////////////////////////////////////////////////////////////////

//! Appends a line "<event id> <attempt>" to |path|. Unlike the worker log, a single
//! unbuffered append survives a hard kill of the worker and does not interleave with the other worker.
void RecordTaskStart(const std::string& path, i64 eventId, i64 attempt)
{
    auto line = Format("%v %v\n", eventId, attempt);
    TFile::ForAppend(TString(path)).Write(line.data(), line.size());
}

//! Runs |task| in a fiber of |invoker|: it records its start and waits for the barrier file, if set.
//! The first attempt of every #FailingEventPeriod-th event fails.
TFuture<TFetchResultPtr> RunFetch(
    const IInvokerPtr& invoker,
    const TTransformAwaiterParametersPtr& parameters,
    TIntrusivePtr<TFetchTask> task)
{
    return BIND([=] {
        RecordTaskStart(parameters->TaskStartsPath, task->EventId, task->Attempt);
        const auto& barrierPath = parameters->TaskBarrierPath;
        while (!barrierPath.empty() && task->Attempt >= parameters->TaskBarrierFromAttempt && !NFS::Exists(barrierPath)) {
            TDelayedExecutor::WaitForDuration(TaskBarrierCheckPeriod);
        }
        TDelayedExecutor::WaitForDuration(TaskDuration);
        if (task->EventId % FailingEventPeriod == 0 && task->Attempt == 0) {
            THROW_ERROR_EXCEPTION("Fetch of event %v failed", task->EventId);
        }
        auto result = New<TFetchResult>();
        result->Value = task->EventId * ResultFactor;
        return result;
    })
        .AsyncVia(invoker)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

//! The fetch of an event, kept from its input message until its result is emitted.
struct TFetchState
    : public TYsonStruct
{
    i64 EventId = 0;
    i64 Attempt = 0;

    REGISTER_YSON_STRUCT(TFetchState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("event_id", &TThis::EventId)
            .Default();
        registrar.Parameter("attempt", &TThis::Attempt)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Keeps the fetch of every event in the state and sets a swift timer that runs it as a task. A failed fetch
//! is retried from a new timer; a successful one emits the result and clears the state.
class TTransformAwaiter
    : public TTransformComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TTransformAwaiterParameters);

    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(StateClient_, "fetches");
        initContext->InitAsyncProcessor(
            Fetcher_,
            "fetcher",
            BIND(&RunFetch, GetContext()->PoolInvoker, GetParameters()),
            BIND(&TTransformAwaiter::OnFetched, Unretained(this)));
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        StateClient_.GetState(message)->EventId = GetColumnValue<i64>(message, "event_id");
        output->AddTimer(FetchTimerStreamId, GetCurrentTimestamp());
    }

    void DoProcessTimer(const TInputTimerConstPtr& timer, IOutputCollectorPtr output) override
    {
        output->Add(Fetcher_, MakeTask(*StateClient_.GetState(timer)));
    }

    void OnFetched(const TCompletedAsyncTask<TFetchTask, TFetchResult>& completedTask, IOutputCollectorPtr output)
    {
        const auto& task = completedTask.GetTask();
        const auto& result = completedTask.GetResult();
        auto state = StateClient_.GetState(completedTask.GetRootTimer());
        if (!result.IsOK()) {
            // The retry starts from a new timer, so that the chain of the root timer does not grow with the attempts.
            state->Attempt = task.Attempt + 1;
            output->AddTimer(FetchTimerStreamId, GetCurrentTimestamp());
            return;
        }
        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(MakeUnversionedInt64Value(task.EventId), "event_id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(result.Value()->Value), "result");
        builder.Payload().SetValue(MakeUnversionedInt64Value(task.Attempt), "attempt");
        output->AddMessage(builder.Finish());
        state.Clear();
    }

private:
    TMutableStateKeyClient<TFetchState> StateClient_;
    TAsyncProcessorHandle<TFetchTask, TFetchResult> Fetcher_;

    static TFetchTaskPtr MakeTask(const TFetchState& state)
    {
        auto task = New<TFetchTask>();
        task->EventId = state.EventId;
        task->Attempt = state.Attempt;
        return task;
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTransformAwaiter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NAsyncProcessorTest

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
