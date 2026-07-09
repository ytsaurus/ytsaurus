#include "recording_output_collector.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

TRecordingOutputCollector::TRecordingOutputCollector()
    : Sink_(New<TSink>())
{ }

TRecordingOutputCollector::TRecordingOutputCollector(TSinkPtr sink, std::vector<TMessageId> parentIds)
    : Sink_(std::move(sink))
    , ParentIds_(std::move(parentIds))
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
    return New<TRecordingOutputCollector>(Sink_, std::move(parentIds));
}

void TRecordingOutputCollector::AddMessage(TMessage&& message, bool distribute)
{
    Sink_->Messages.push_back(TRecordedMessage{
        .Message = std::move(message),
        .Distribute = distribute,
        .ParentIds = ParentIds_,
    });
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
