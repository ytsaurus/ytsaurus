#pragma once

#include "common.h"
#include "message.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

//! A bus, i.e. something capable of transmitting messages.
struct IBus
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBus> TPtr;

    DECLARE_ENUM(ESendResult,
        (OK)
        (Failed)
    );

    typedef TFuture<ESendResult> TSendResult;

    //! Asynchronously sends a message via the bus.
    /*!
     *  \param message A message to send.
     *  \return An asynchronous flag indicating if the delivery (not the processing!) of the message
     *  was successful.
     * 
     *  \note Thread affinity: any
     */
    virtual TSendResult::TPtr Send(IMessage::TPtr message) = 0;

    //! Terminates the bus.
    /*!
     *  It is safe to call this method multiple times.
     *  After the first call the instance is no longer usable.

     *  \note Thread affinity: any.
     */
    virtual void Terminate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Handles incoming bus messages.
struct IMessageHandler
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMessageHandler> TPtr;

    //! Raised whenever a new message arrives via the bus.
    /*!
     *  \param message The just arrived message.
     *  \param replyBus A bus that can be used for replying back.
     *  
     *  \note Thread affinity: the method is called from an unspecified thread
     *  and must return ASAP.
     *  
     */
    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
