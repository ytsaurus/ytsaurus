#include "stdafx.h"
#include "message_rearranger.h"

#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    const TSessionId& sessionId,
    TCallback<void(IMessage::TPtr)> onMessage,
    TDuration timeout)
    : SessionId(sessionId)
    , OnMessageDequeued(onMessage)
    , Timeout(timeout)
    , ExpectedSequenceId(0)
    , Logger(BusLogger)
{
    YASSERT(!onMessage.IsNull());

    Logger.AddTag(Sprintf("SessionId: %s", ~sessionId.ToString()));
}

void TMessageRearranger::EnqueueMessage(
    IMessage::TPtr message,
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

        OnMessageDequeued.Run(message);
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

        if (PostponedMessages.size() == 1) {
            RescheduleTimeout();
        }
    }
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

        OnMessageDequeued.Run(~message.Message);
        PostponedMessages.erase(it);
        ++ExpectedSequenceId;
    }

    if (!PostponedMessages.empty()) {
        RescheduleTimeout();
    }
}

void TMessageRearranger::RescheduleTimeout()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    TDelayedInvoker::CancelAndClear(TimeoutCookie);
    TimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TMessageRearranger::OnTimeout, MakeWeak(this)),
        Timeout);
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
