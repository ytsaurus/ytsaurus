#include "message_rearranger.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TRpcManager::Get()->GetLogger();

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    IParamAction<IMessage::TPtr>::TPtr onMessage,
    TDuration timeout)
    : OnMessageDequeued(onMessage)
    , Timeout(timeout)
    , ExpectedSequenceId(-1)
{ }

void TMessageRearranger::EnqueueMessage(IMessage::TPtr message, TSequenceId sequenceId)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (sequenceId == ExpectedSequenceId || ExpectedSequenceId < 0) {
        LOG_DEBUG("Pass-through message (Message: %p, SequenceId: %" PRId64 ")",
            ~message,
            sequenceId);

        ScheduleTimeout();
        ExpectedSequenceId = sequenceId + 1;

        guard.Release();

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

    VERIFY(
        MessageMap.insert(MakePair(sequenceId, message)).second,
        "Duplicate sequence id");
}

void TMessageRearranger::OnTimeout()
{
    yvector<IMessage::TPtr> readyMessages;
    
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (MessageMap.empty())
            return;

        ExpectedSequenceId = MessageMap.begin()->first;

        LOG_DEBUG("Message rearrange timeout (ExpectedSequenceId: %" PRId64 ")",
            ExpectedSequenceId);

        while (true) {
            TMessageMap::iterator it = MessageMap.begin();
            TSequenceId sequenceId = it->first;
            if (sequenceId != ExpectedSequenceId)
                break;

            IMessage::TPtr message = it->second;
            MessageMap.erase(it);

            LOG_DEBUG("Flushed message (Message: %p, SequenceId: %" PRId64 ")",
                ~message,
                sequenceId);

            readyMessages.push_back(message);
            ++ExpectedSequenceId;
        }
    }

    for (yvector<IMessage::TPtr>::iterator it = readyMessages.begin();
         it != readyMessages.end();
         ++it)
    {
        OnMessageDequeued->Do(*it);
    }

    ScheduleTimeout();
}

void TMessageRearranger::CancelTimeout()
{
    if (TimeoutCookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(TimeoutCookie);
        TimeoutCookie = TDelayedInvoker::TCookie();
    }
}

void TMessageRearranger::ScheduleTimeout()
{
    CancelTimeout();
    TimeoutCookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TMessageRearranger::OnTimeout, this),
        Timeout);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
