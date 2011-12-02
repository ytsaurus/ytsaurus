#include "stdafx.h"
#include "message_rearranger.h"

#include "../misc/assert.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    IParamAction<IMessage::TPtr>* onMessage,
    TDuration timeout)
    : OnMessageDequeued(onMessage)
    , Timeout(timeout)
    , ExpectedSequenceId(-1)
{
    YASSERT(onMessage != NULL);
}

void TMessageRearranger::EnqueueMessage(IMessage::TPtr message, TSequenceId sequenceId)
{
    YASSERT(~message != NULL);

    TGuard<TSpinLock> guard(SpinLock);

    if (sequenceId == ExpectedSequenceId || ExpectedSequenceId < 0) {
        LOG_DEBUG("Pass-through message (Message: %p, SequenceId: %" PRId64 ")",
            ~message,
            sequenceId);

        ScheduleTimeout();
        ExpectedSequenceId = sequenceId + 1;

        OnMessageDequeued->Do(message);
        return;
    }

    if (sequenceId < ExpectedSequenceId) {
        LOG_DEBUG("Late message (Message: %p, SequenceId: %" PRId64 ", ExpectedSequenceId: %" PRId64 ")",
            ~message,
            sequenceId,
            ExpectedSequenceId);
        // Just drop the message.
        return;
    }
    
    LOG_DEBUG("Postponed message (Message: %p, SequenceId: %" PRId64 ", ExpectedSequenceId: %" PRId64 ")",
        ~message,
        sequenceId,
        ExpectedSequenceId);

    if (MessageMap.empty()) {
        ScheduleTimeout();
    }

    YVERIFY(MessageMap.insert(MakePair(sequenceId, message)).second);
}

void TMessageRearranger::OnTimeout()
{
    TGuard<TSpinLock> guard(SpinLock);
    
    if (MessageMap.empty())
        return;

    ExpectedSequenceId = MessageMap.begin()->first;

    LOG_DEBUG("Message rearrange timeout (ExpectedSequenceId: %" PRId64 ")",
        ExpectedSequenceId);

    while (true) {
        auto it = MessageMap.begin();
        auto sequenceId = it->first;
        if (sequenceId != ExpectedSequenceId)
            break;

        auto message = it->second;
        
        LOG_DEBUG("Flushed message (Message: %p, SequenceId: %" PRId64 ")",
            ~message,
            sequenceId);

        OnMessageDequeued->Do(message);
        MessageMap.erase(it);

        ++ExpectedSequenceId;
    }
   
    ScheduleTimeout();
}

void TMessageRearranger::ScheduleTimeout()
{
    if (TimeoutCookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(TimeoutCookie);
        TimeoutCookie = TDelayedInvoker::TCookie();
    }
    TimeoutCookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TMessageRearranger::OnTimeout, TPtr(this)),
        Timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
