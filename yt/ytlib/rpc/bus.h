#pragma once

#include "common.h"
#include "message.h"

#include "../misc/lease_manager.h"

#include <util/system/thread.h>
#include <quality/NetLiba/UdpHttp.h>
#include <quality/NetLiba/UdpAddress.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

// TODO: move somvewhere (needed for both server and client)
const int MaxRequestsPerCall = 100;
static const TDuration MessageRearrangeTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

struct IBus
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBus> TPtr;

    enum ESendResult
    {
        OK,
        Failed
    };

    typedef TAsyncResult<ESendResult> TSendResult;

    virtual ~IBus() {}
    virtual TSendResult::TPtr Send(IMessage::TPtr message) = 0;
    virtual void Terminate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMessageHandler
{
    virtual ~IMessageHandler() {}
    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus) = 0;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NRpc
} // namespace NYT
