#pragma once

#include <yt/yt/flow/library/cpp/computation/async_processor_manager.h>

#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <optional>
#include <vector>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! IOutputCollector for unit tests: records added messages, timers and tasks into vectors for
//! assertion via GetMessages() / GetTimers() / GetTasks(). SetParents() returns a child collector
//! that stamps the given parents onto entities added through it.
class TRecordingOutputCollector
    : public IOutputCollector
{
public:
    struct TRecordedMessage
    {
        TMessage Message;
        bool Distribute = true;
        TOutputMessageIdSuffix MessageIdSuffix = TOutputMessageIdSuffix::FromSequenceNumber();
        std::vector<TMessageId> ParentIds;
    };

    struct TRecordedTimer
    {
        //! Set when added via AddTimer(TTimer&&); empty for the timestamp-only overloads.
        std::optional<TTimer> Timer;
        std::optional<TStreamId> StreamId;
        TSystemTimestamp TriggerTimestamp;
        std::optional<TSystemTimestamp> EventTimestamp;
        std::vector<TMessageId> ParentIds;
    };

    //! Shared recording sink so child collectors returned by SetParents() append to the
    //! same vectors as the root.
    struct TSink
        : public TRefCounted
    {
        std::vector<TRecordedMessage> Messages;
        std::vector<TRecordedTimer> Timers;
        std::vector<TFailedMessage> FailedMessages;
        std::vector<TAsyncTaskRequest> Tasks;
    };

    using TSinkPtr = TIntrusivePtr<TSink>;

    TRecordingOutputCollector();
    //! Internal: builds a child sharing |sink| and stamping |parentIds|; |parentTimers| are the timers
    //! among the parents. Public only so New<> can construct it; prefer SetParents() to obtain children.
    TRecordingOutputCollector(
        TSinkPtr sink,
        std::vector<TMessageId> parentIds,
        std::vector<TInputTimerConstPtr> parentTimers);

    IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits) override;
    void AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(TTimer&& timer) override;

    const std::vector<TRecordedMessage>& GetMessages() const;
    const std::vector<TRecordedTimer>& GetTimers() const;
    const std::vector<TFailedMessage>& GetFailedMessages() const;
    //! The tasks sent to async processors, each with the timer it descends from.
    const std::vector<TAsyncTaskRequest>& GetTasks() const;

private:
    void DoAddMessage(TMessage&& message, TAddMessageOptions options) override;

    const TSinkPtr Sink_;
    const std::vector<TMessageId> ParentIds_;
    const std::vector<TInputTimerConstPtr> ParentTimers_;

    void DoAddFailedMessage(const TInputMessageConstPtr& message, const TError& reason) override;
    void DoAddTask(const TAsyncProcessorDescriptorPtr& processor, TYsonMessagePtr task) override;
};

DEFINE_REFCOUNTED_TYPE(TRecordingOutputCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
