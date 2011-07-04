#pragma once

#include "common.h"
#include "message.h"

#include "../misc/delayed_invoker.h"

namespace NYT {
namespace NRpc{

////////////////////////////////////////////////////////////////////////////////

class TMessageRearranger
    : public TNonCopyable
{
public:
    TMessageRearranger(
        IParamAction<IMessage::TPtr>::TPtr onMessage,
        TDuration timeout);

    void EnqueueMessage(
        IMessage::TPtr message,
        TSequenceId sequenceId);

private:
    typedef ymap<TSequenceId, IMessage::TPtr> TMessageMap;

    TSpinLock SpinLock;
    IParamAction<IMessage::TPtr>::TPtr OnMessage;
    TDuration Timeout;
    TDelayedInvoker::TCookie TimeoutCookie; // for delay
    TSequenceId ExpectedSequenceId;
    TMessageMap MessageMap;

    void ScheduleTimeout();
    void CancelTimeout();
    void OnTimeout();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
