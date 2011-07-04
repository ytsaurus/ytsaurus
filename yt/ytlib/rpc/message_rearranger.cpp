#include "message_rearranger.h"

namespace NYT {
namespace NRpc{

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    IParamAction<IMessage::TPtr>::TPtr onMessage,
    TDuration maxDelay)
    : OnMessage(onMessage)
    , MaxDelay(maxDelay)
    , WaitingId(-1)
{ }

void TMessageRearranger::ArrangeMessage(IMessage::TPtr message, TSequenceId sequenceId)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (sequenceId == WaitingId) {
        TDelayedInvoker::Get()->Cancel(TimeoutCookie);
        OnMessage->Do(message);
        TimeoutCookie = TDelayedInvoker::Get()->Submit(
            FromMethod(&TMessageRearranger::OnExpired,this), MaxDelay);
        WaitingId = sequenceId + 1;
    } else {
        if (MessageMap.empty()) {
            TimeoutCookie = TDelayedInvoker::Get()->Submit(
                FromMethod(&TMessageRearranger::OnExpired,this), MaxDelay);
        }
        MessageMap[sequenceId] = message;
    }
}

void TMessageRearranger::OnExpired()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (MessageMap.empty()) {
        return;
    }
    OnMessage->Do(MessageMap.begin()->second);
    MessageMap.erase(MessageMap.begin());
    TimeoutCookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TMessageRearranger::OnExpired,this), MaxDelay);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
