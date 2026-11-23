#include "file_parsing_functions.h"

#include "parse_executor_resource.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

void TEnqueueFileFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto task = New<TParseTask>();
    task->Url = GetColumnValue<std::string>(message, "url");
    task->Size = GetColumnValue<i64>(message, "size");
    // The write time of the input row, unlike the current time, is the same when the source replays the row.
    task->FirstSeenAt = GetColumnValue<ui64>(message, WriteTimeColumn);
    output->AddMessage(context->ConvertToMessage(task));
}

////////////////////////////////////////////////////////////////////////////////

void TParseFunctionParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_attempts", &TThis::MaxAttempts)
        .Default(100)
        .GreaterThan(0);
}

void TParseState::Register(TRegistrar registrar)
{
    registrar.Parameter("task", &TThis::Task)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TParseFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    MaxAttempts_ = initContext->GetParameters<TParseFunctionParameters>()->MaxAttempts;
    initContext->InitClient(StateClient_, "requests");

    // A task counts as running from the moment it is handed to the executor, so the limit of running tasks
    // of the processor in the dynamic spec must exceed the requests a partition may have: a task held back
    // by the limit never reaches the executor and is neither started there nor rejected to another worker.
    auto executor = initContext->GetStaticResource(ParseExecutorResourceId)->As<TParseExecutorResource>();
    initContext->InitAsyncProcessor(
        Parser_,
        ParseProcessorName,
        BIND(&TParseExecutorResource::Parse, std::move(executor)),
        BIND(&TParseFunction::OnParsed, Unretained(this)));
}

void TParseFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = StateClient_.GetState(message);
    // The key has a parse in flight: the request is its duplicate. So every state has a single timer.
    if (!state.IsEmpty()) {
        return;
    }
    state->Task = context->ConvertToYsonMessage<TParseTask>(message);
    output->AddTimer(ParseTimerStreamId, context->GetCurrentTimestamp());
}

void TParseFunction::ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& /*context*/)
{
    output->Add(Parser_, StateClient_.GetState(timer)->Task);
}

void TParseFunction::OnParsed(
    const TCompletedAsyncTask<TParseTask, TParseResult>& completedTask,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    const auto& task = completedTask.GetTask();
    const auto& result = completedTask.GetResult();
    StateClient_.GetState(completedTask.GetRootTimer()).Clear();

    bool rejected = result.IsOK() && result.Value()->Rejected;
    if (result.IsOK() && !rejected) {
        auto parsed = New<TParsedFile>();
        parsed->Meta->StreamId = ParsedStreamId;
        parsed->Url = task.Url;
        parsed->RowCount = result.Value()->RowCount;
        parsed->Attempt = task.Attempt;
        parsed->Worker = result.Value()->Worker;
        output->AddMessage(context->ConvertToMessage(parsed));
        return;
    }

    if (rejected && task.Attempt + 1 < MaxAttempts_) {
        // The next attempt has another key and may land on another worker.
        auto retry = New<TParseTask>();
        retry->Meta->StreamId = RetriesStreamId;
        retry->Url = task.Url;
        retry->Size = task.Size;
        retry->Attempt = task.Attempt + 1;
        retry->FirstSeenAt = task.FirstSeenAt;
        output->AddMessage(context->ConvertToMessage(retry));
        return;
    }

    auto failed = New<TFailedFile>();
    failed->Meta->StreamId = FailedStreamId;
    failed->Url = task.Url;
    failed->Attempt = task.Attempt;
    auto error = rejected
        ? TError("File is rejected in all %v attempts", task.Attempt + 1)
        : TError(result);
    failed->Error = ToString(error);
    output->AddMessage(context->ConvertToMessage(failed));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
