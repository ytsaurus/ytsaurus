#pragma once

#include "common.h"
#include "message.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IBus
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBus> TPtr;

    DECLARE_ENUM(ESendResult,
        (OK)
        (Failed)
    );

    typedef TAsyncResult<ESendResult> TSendResult;

    virtual TSendResult::TPtr Send(IMessage::TPtr message) = 0;
    virtual void Terminate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMessageHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMessageHandler> TPtr;

    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
