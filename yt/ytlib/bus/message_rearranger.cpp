#include "stdafx.h"
#include "message_rearranger.h"

#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    const TSessionId& sessionId,
    IParamAction<IMessage*>* onMessage,
    TDuration timeout)
    : SessionId(sessionId)
    , OnMessageDequeued(onMessage)
    , Timeout(timeout)
    , ExpectedSequenceId(0)
    , Logger(BusLogger)
{
    YASSERT(onMessage);

    Logger.AddTag(Sprintf("SessionId: %s", ~sessionId.ToString()));
}

void TMessageRearranger::EnqueueMessage(
    IMessage* message,
    const TGuid& requestId,
    TSequenceId sequenceId)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(message);

    TGuard<TSpinLock> guard(SpinLock);

    if (sequenceId == ExpectedSequenceId) {
        LOG_DEBUG("Pass-through message (RequestId: %s, SequenceId: %" PRId64 ")",
            ~requestId.ToString(),
            sequenceId);

        OnMessageDequeued->Do(message);
        ExpectedSequenceId = sequenceId + 1;
        TryFlush();
        RescheduleTimeout();
    } else if (sequenceId < ExpectedSequenceId) {
        LOG_DEBUG("Message is late, discarded (RequestId: %s, SequenceId: %" PRId64 ", ExpectedSequenceId: %" PRId64 ")",
            ~requestId.ToString(),
            sequenceId,
            ExpectedSequenceId);

        // Do nothing.
    } else {
        LOG_DEBUG("Message postponed (RequestId: %s, SequenceId: %" PRId64 ", ExpectedSequenceId: %" PRId64 ")",
            ~requestId.ToString(),
            sequenceId,
            ExpectedSequenceId);

        YVERIFY(PostponedMessages.insert(MakePair(
            sequenceId,
            TPostponedMessage(requestId, message))).second);
        RescheduleTimeout();
    }
}

void TMessageRearranger::OnTimeout()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);
    
    if (PostponedMessages.empty())
        return;

    ExpectedSequenceId = PostponedMessages.begin()->first;

    LOG_DEBUG("Message rearrange timeout (ExpectedSequenceId: %" PRId64 ")", ExpectedSequenceId);

    TryFlush();
    RescheduleTimeout();
}

void TMessageRearranger::TryFlush()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    while (true) {
        auto it = PostponedMessages.begin();
        auto sequenceId = it->first;
        if (sequenceId != ExpectedSequenceId)
            break;

        auto message = it->second;

        LOG_DEBUG("Message flushed (RequestId: %s, SequenceId: %" PRId64 ")",
            ~message.RequestId.ToString(),
            sequenceId);

        OnMessageDequeued->Do(~message.Message);
        PostponedMessages.erase(it);
        ++ExpectedSequenceId;
    }
}

void TMessageRearranger::RescheduleTimeout()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    if (TimeoutCookie != TDelayedInvoker::NullCookie) {
        TDelayedInvoker::CancelAndClear(TimeoutCookie);
    }

    if (!PostponedMessages.empty()) {
        TimeoutCookie = TDelayedInvoker::Submit(
            ~FromMethod(&TMessageRearranger::OnTimeout, TPtr(this)),
            Timeout);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
