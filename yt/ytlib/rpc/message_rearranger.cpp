#include "message_rearranger.h"

namespace NYT {
namespace NRpc{

////////////////////////////////////////////////////////////////////////////////

TMessageRearranger::TMessageRearranger(
    IParamAction<IMessage::TPtr>::TPtr onMessage,
    TDuration maxDelay)
    : OnMessage(onMessage)
    , MaxDelay(maxDelay)
{

}

void TMessageRearranger::ExpireDelay()
{

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
