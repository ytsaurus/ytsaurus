#pragma once

#include "common.h"
#include "message.h"

#include "../misc/delayed_invoker.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TMessageRearranger
    : public TNonCopyable
{
public:
    TMessageRearranger(
        IParamAction<IMessage::TPtr>::TPtr onDequeuedMessage,
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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
