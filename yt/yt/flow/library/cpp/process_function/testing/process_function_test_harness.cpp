#include "process_function_test_harness.h"

#include <util/generic/hash_set.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! As in the worker: a timer that gets a task stays held, so it may not emit outputs in the same epoch.
void ValidateHeldRootTimersHaveNoOutputs(const TRecordingOutputCollector& output)
{
    THashSet<TMessageId> heldRootTimerIds;
    for (const auto& task : output.GetTasks()) {
        heldRootTimerIds.insert(task.RootTimer->MessageId);
    }
    auto validate = [&] (const std::vector<TMessageId>& parentIds, TStringBuf outputKind) {
        for (const auto& parentId : parentIds) {
            THROW_ERROR_EXCEPTION_IF(heldRootTimerIds.contains(parentId),
                "Timer %v adds a task and %v in one epoch; outputs are allowed only at the step that acknowledges the timer",
                parentId,
                outputKind);
        }
    };
    for (const auto& message : output.GetMessages()) {
        validate(message.ParentIds, "an output message");
    }
    for (const auto& timer : output.GetTimers()) {
        validate(timer.ParentIds, "a timer");
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionTestHarness::TProcessFunctionTestHarness(
    TTestStateEnvironment& env,
    IProcessFunctionBasePtr function,
    IRuntimeContextPtr context)
    : Env_(env)
    , Function_(std::move(function))
    , Batch_(WrapAsBatch(Function_))
    // Recover the sync phase by cross-cast: the function is constructed directly, not via the registry.
    , SyncFunction_(dynamic_cast<ISyncProcessFunction*>(Function_.Get()))
    , Context_(std::move(context))
    , Output_(New<TRecordingOutputCollector>())
{ }

TProcessFunctionTestHarness::TProcessFunctionTestHarness(
    TTestStateEnvironment& env,
    IProcessFunctionBasePtr function,
    IRuntimeContextPtr context,
    TProcessFunctionContextPtr processFunctionContext)
    : TProcessFunctionTestHarness(env, std::move(function), std::move(context))
{
    FrozenProcessFunctionContext_ = std::move(processFunctionContext);
}

void TProcessFunctionTestHarness::RunEpoch(const IInputContextPtr& input)
{
    EnsureInitialized();

    auto completedTasks = RunTasks();

    // A result is processed with the state of the key of its root timer.
    auto timers = input->GetTimers();
    for (const auto& task : completedTasks) {
        timers.push_back(task->RootTimer);
    }
    Env_.PreloadEpoch(New<TInputContext>(input->GetMessages(), std::move(timers), input->GetVisits()));

    Output_ = New<TRecordingOutputCollector>();
    // Another harness of the environment may have set its own context.
    Env_.SetResultHandlerContext(Context_);
    // The results go first, as in the worker; the root timer is the parent of what the handler adds.
    for (const auto& task : completedTasks) {
        task->Processor->ResultHandler(task, Output_->SetParents({}, {task->RootTimer}, {}));
    }
    Batch_->Process(input, Output_->SetParents(input->GetMessages(), input->GetTimers(), input->GetVisits()), Context_);
    ValidateHeldRootTimersHaveNoOutputs(*Output_);

    Env_.CommitEpoch([&] (const IRetryableTransactionPtr& transaction) {
        if (SyncFunction_) {
            SyncFunction_->Sync(transaction, Context_);
        }
    });
    // As in the worker, only the tasks of a committed epoch run.
    CommittedTasks_ = Output_->GetTasks();
}

void TProcessFunctionTestHarness::RunEpoch(
    std::vector<TInputMessageConstPtr> messages,
    std::vector<TInputTimerConstPtr> timers,
    std::vector<TInputVisitConstPtr> visits)
{
    RunEpoch(New<TInputContext>(std::move(messages), std::move(timers), std::move(visits)));
}

const std::vector<TRecordingOutputCollector::TRecordedMessage>& TProcessFunctionTestHarness::GetMessages() const
{
    return Output_->GetMessages();
}

const std::vector<TRecordingOutputCollector::TRecordedTimer>& TProcessFunctionTestHarness::GetTimers() const
{
    return Output_->GetTimers();
}

const std::vector<TFailedMessage>& TProcessFunctionTestHarness::GetFailedMessages() const
{
    return Output_->GetFailedMessages();
}

const std::vector<TAsyncTaskRequest>& TProcessFunctionTestHarness::GetTasks() const
{
    return Output_->GetTasks();
}

const IRuntimeContextPtr& TProcessFunctionTestHarness::GetContext() const
{
    return Context_;
}

std::vector<TUntypedCompletedAsyncTaskPtr> TProcessFunctionTestHarness::RunTasks()
{
    auto requests = std::exchange(CommittedTasks_, {});
    std::vector<TFuture<TYsonMessagePtr>> futures;
    futures.reserve(requests.size());
    for (const auto& request : requests) {
        TFuture<TYsonMessagePtr> future;
        try {
            future = request.Processor->Start(request.Task);
        } catch (const std::exception& ex) {
            future = MakeFuture<TYsonMessagePtr>(TError(ex));
        }
        if (!future) {
            future = MakeFuture<TYsonMessagePtr>(TError("Async processor %Qv returned a null future", request.Processor->Name));
        }
        futures.push_back(std::move(future));
    }
    auto results = NConcurrency::WaitFor(AllSet(std::move(futures)))
        .ValueOrThrow();

    std::vector<TUntypedCompletedAsyncTaskPtr> completedTasks;
    completedTasks.reserve(requests.size());
    for (int index = 0; index < std::ssize(requests); ++index) {
        const auto& request = requests[index];
        auto task = New<TUntypedCompletedAsyncTask>();
        task->Processor = request.Processor;
        task->RootTimer = request.RootTimer;
        task->Task = request.Task;
        task->Result = results[index];
        if (task->Result.IsOK() && !task->Result.Value()) {
            task->Result = TError("Async processor %Qv returned a null result", request.Processor->Name);
        }
        completedTasks.push_back(std::move(task));
    }
    return completedTasks;
}

void TProcessFunctionTestHarness::EnsureInitialized()
{
    if (Initialized_) {
        return;
    }
    // Lazy, so the test can tweak the environment before the first epoch.
    if (FrozenProcessFunctionContext_) {
        Env_.InitProcessFunction(Function_, FrozenProcessFunctionContext_);
    } else {
        Env_.InitProcessFunction(Function_);
    }
    Initialized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
