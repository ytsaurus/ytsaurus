#pragma once

#include "common.h"
#include "message.h"

#include "../misc/delayed_invoker.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

class TMessageRearranger
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMessageRearranger> TPtr;

    TMessageRearranger(
        IParamAction<IMessage::TPtr>* onDequeuedMessage,
        TDuration timeout);

    void EnqueueMessage(
        IMessage::TPtr message,
        TSequenceId sequenceId);

private:
    typedef ymap<TSequenceId, IMessage::TPtr> TMessageMap;

    TSpinLock SpinLock;
    IParamAction<IMessage::TPtr>::TPtr OnMessageDequeued;
    TDuration Timeout;
    TDelayedInvoker::TCookie TimeoutCookie;
    TSequenceId ExpectedSequenceId;
    TMessageMap MessageMap;

    void ScheduleTimeout();
    void OnTimeout();
    void FlushRearrangedMessages();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
