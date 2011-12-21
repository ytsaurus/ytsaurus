#include "stdafx.h"
#include "message_rearranger.h"

#include "../misc/assert.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    IParamAction<IMessage*>* onMessage,
    TDuration timeout)
    : OnMessageDequeued(onMessage)
    , Timeout(timeout)
    , ExpectedSequenceId(-1)
{
    YASSERT(onMessage);
}

void TMessageRearranger::EnqueueMessage(IMessage* message, TSequenceId sequenceId)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(message);

    TGuard<TSpinLock> guard(SpinLock);

    if (sequenceId == ExpectedSequenceId || ExpectedSequenceId < 0) {
        LOG_DEBUG("Pass-through message (Message: %p, SequenceId: %" PRId64 ")",
            message,
            sequenceId);

        RescheduleTimeout();
        ExpectedSequenceId = sequenceId + 1;

        OnMessageDequeued->Do(message);
        return;
    }

    if (sequenceId < ExpectedSequenceId) {
        LOG_DEBUG("Message is late (Message: %p, SequenceId: %" PRId64 ", ExpectedSequenceId: %" PRId64 ")",
            message,
            sequenceId,
            ExpectedSequenceId);
        // Just drop the message.
        return;
    }
    
    LOG_DEBUG("Message postponed (Message: %p, SequenceId: %" PRId64 ", ExpectedSequenceId: %" PRId64 ")",
        message,
        sequenceId,
        ExpectedSequenceId);

    YVERIFY(MessageMap.insert(MakePair(sequenceId, message)).second);
    RescheduleTimeout();
}

void TMessageRearranger::OnTimeout()
{
    VERIFY_THREAD_AFFINITY_ANY();

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
        
        LOG_DEBUG("Message flushed (Message: %p, SequenceId: %" PRId64 ")",
            ~message,
            sequenceId);

        OnMessageDequeued->Do(~message);

        MessageMap.erase(it);

        ++ExpectedSequenceId;
    }
   
    RescheduleTimeout();
}

void TMessageRearranger::RescheduleTimeout()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    if (TimeoutCookie != TDelayedInvoker::NullCookie) {
        TDelayedInvoker::CancelAndClear(TimeoutCookie);
    }

    if (!MessageMap.empty()) {
        TimeoutCookie = TDelayedInvoker::Submit(
            ~FromMethod(&TMessageRearranger::OnTimeout, TPtr(this)),
            Timeout);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
