#include "recording_output_collector.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <algorithm>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

TRecordingOutputCollector::TRecordingOutputCollector()
    : Sink_(New<TSink>())
{ }

TRecordingOutputCollector::TRecordingOutputCollector(
    TSinkPtr sink,
    std::vector<TMessageId> parentIds,
    std::vector<TInputTimerConstPtr> parentTimers)
    : Sink_(std::move(sink))
    , ParentIds_(std::move(parentIds))
    , ParentTimers_(std::move(parentTimers))
{ }

IOutputCollectorPtr TRecordingOutputCollector::SetParents(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    std::vector<TMessageId> parentIds;
    parentIds.reserve(messages.size() + timers.size() + visits.size());
    for (const auto& message : messages) {
        parentIds.push_back(message->MessageId);
    }
    for (const auto& timer : timers) {
        parentIds.push_back(timer->MessageId);
    }
    for (const auto& visit : visits) {
        parentIds.push_back(visit->MessageId);
    }
    return New<TRecordingOutputCollector>(Sink_, std::move(parentIds), timers);
}

void TRecordingOutputCollector::DoAddMessage(TMessage&& message, TAddMessageOptions options)
{
    Sink_->Messages.push_back(TRecordedMessage{
        .Message = std::move(message),
        .Distribute = options.Distribute,
        .MessageIdSuffix = std::move(options.MessageIdSuffix),
        .ParentIds = ParentIds_,
    });
}

void TRecordingOutputCollector::DoAddFailedMessage(const TInputMessageConstPtr& message, const TError& reason)
{
    Sink_->FailedMessages.push_back({message, reason});
}

const std::vector<TFailedMessage>& TRecordingOutputCollector::GetFailedMessages() const
{
    return Sink_->FailedMessages;
}

void TRecordingOutputCollector::DoAddTask(const TAsyncProcessorDescriptorPtr& processor, TYsonMessagePtr task)
{
    THROW_ERROR_EXCEPTION_UNLESS(task, "Task of async processor %Qv is null", processor->Name);
    THROW_ERROR_EXCEPTION_UNLESS(ParentTimers_.size() == 1 && ParentIds_.size() == 1,
        "A task of async processor %Qv must have a single parent, a swift timer; got %v parents, %v of them timers",
        processor->Name,
        ParentIds_.size(),
        ParentTimers_.size());

    const auto& rootTimer = ParentTimers_[0];
    auto rootTimerHasTask = std::ranges::any_of(Sink_->Tasks, [&] (const TAsyncTaskRequest& request) {
        return request.RootTimer->MessageId == rootTimer->MessageId;
    });
    THROW_ERROR_EXCEPTION_IF(rootTimerHasTask,
        "Timer %v gets two tasks in one epoch, but a chain of tasks is linear",
        rootTimer->MessageId);

    Sink_->Tasks.push_back({
        .Processor = processor,
        .Task = std::move(task),
        .RootTimer = rootTimer,
    });
}

const std::vector<TAsyncTaskRequest>& TRecordingOutputCollector::GetTasks() const
{
    return Sink_->Tasks;
}

void TRecordingOutputCollector::AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp)
{
    Sink_->Timers.push_back(TRecordedTimer{
        .TriggerTimestamp = triggerTimestamp,
        .EventTimestamp = eventTimestamp,
        .ParentIds = ParentIds_,
    });
}

void TRecordingOutputCollector::AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp)
{
    Sink_->Timers.push_back(TRecordedTimer{
        .StreamId = streamId,
        .TriggerTimestamp = triggerTimestamp,
        .EventTimestamp = eventTimestamp,
        .ParentIds = ParentIds_,
    });
}

void TRecordingOutputCollector::AddTimer(TTimer&& timer)
{
    auto triggerTimestamp = timer.TriggerTimestamp;
    auto streamId = timer.StreamId;
    auto eventTimestamp = timer.EventTimestamp;
    Sink_->Timers.push_back(TRecordedTimer{
        .Timer = std::move(timer),
        .StreamId = streamId,
        .TriggerTimestamp = triggerTimestamp,
        .EventTimestamp = eventTimestamp,
        .ParentIds = ParentIds_,
    });
}

const std::vector<TRecordingOutputCollector::TRecordedMessage>& TRecordingOutputCollector::GetMessages() const
{
    return Sink_->Messages;
}

const std::vector<TRecordingOutputCollector::TRecordedTimer>& TRecordingOutputCollector::GetTimers() const
{
    return Sink_->Timers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
