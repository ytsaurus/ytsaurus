#pragma once

#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <optional>
#include <vector>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! IOutputCollector for unit tests: records added messages and timers into vectors for
//! assertion via GetMessages() / GetTimers(). SetParents() returns a child collector that
//! stamps the given parents onto entities added through it.
class TRecordingOutputCollector
    : public IOutputCollector
{
public:
    struct TRecordedMessage
    {
        TMessage Message;
        bool Distribute = true;
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
    };

    using TSinkPtr = TIntrusivePtr<TSink>;

    TRecordingOutputCollector();
    //! Internal: builds a child sharing |sink| and stamping |parentIds|. Public only so
    //! New<> can construct it; prefer SetParents() to obtain children.
    TRecordingOutputCollector(TSinkPtr sink, std::vector<TMessageId> parentIds);

    IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits) override;
    void AddMessage(TMessage&& message, bool distribute) override;
    void AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(TTimer&& timer) override;

    const std::vector<TRecordedMessage>& GetMessages() const;
    const std::vector<TRecordedTimer>& GetTimers() const;

private:
    const TSinkPtr Sink_;
    const std::vector<TMessageId> ParentIds_;
};

DEFINE_REFCOUNTED_TYPE(TRecordingOutputCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
