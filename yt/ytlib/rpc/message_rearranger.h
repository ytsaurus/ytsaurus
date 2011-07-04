#pragma once

#include "common.h"

#include "message.h"

#include "../actions/delayed_invoker.h"

namespace NYT {
namespace NRpc{

////////////////////////////////////////////////////////////////////////////////

typedef i64 TSequenceId;
class TMessageRearranger
{
public:
    TMessageRearranger(IParamAction<IMessage::TPtr>::TPtr onMessage, TDuration maxDelay);

    void ArrangeMessage(IMessage::TPtr message, TSequenceId sequenceId);

private:
    IParamAction<IMessage::TPtr>::TPtr OnMessage;
    TDuration MaxDelay;
    TDelayedInvoker::TCookie Cookie; // for delay

    typedef ymap<TSequenceId, IMessage::TPtr> TMessageMap;
    TMessageMap MessageMap;

    void ExpireDelay();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
