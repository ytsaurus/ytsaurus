#pragma once

#include "common.h"
#include "message.h"

// TODO: drop redundant includes
#include "../misc/lease_manager.h"

#include <util/system/thread.h>
#include <quality/NetLiba/UdpHttp.h>
#include <quality/NetLiba/UdpAddress.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IBus
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBus> TPtr;

    BEGIN_DECLARE_ENUM(ESendResult,
        (OK)
        (Failed)
    )
    END_DECLARE_ENUM();

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

} // namespace NBus
} // namespace NYT
