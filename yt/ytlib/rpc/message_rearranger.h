#pragma once

#include "common.h"
#include "message.h"

#include "../actions/delayed_invoker.h"

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

    void ArrangeMessage(
        IMessage::TPtr message,
        TSequenceId sequenceId);

private:
    typedef ymap<TSequenceId, IMessage::TPtr> TMessageMap;

    IParamAction<IMessage::TPtr>::TPtr OnMessage;
    TDuration Timeout;
    TDelayedInvoker::TCookie TimeoutCookie; // for delay
    TSpinLock SpinLock;
    TSequenceId ExpectedSequenceId;
    TMessageMap MessageMap;

    void OnExpired();
    TDelayedInvoker::TCookie ScheduleExpiration();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
