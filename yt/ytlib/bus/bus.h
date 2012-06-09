#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/actions/signal.h>
#include <ytlib/actions/future.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

//! Describes a result of sending a message via a bus.
DECLARE_ENUM(ESendResult,
    (OK)
    (Failed)
);

//! A bus, i.e. something capable of transmitting messages.
struct IBus
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IBus> TPtr;

    typedef TFuture<ESendResult> TSendResult;
    typedef TPromise<ESendResult> TSendPromise;

    //! Asynchronously sends a message via the bus.
    /*!
     *  \param message A message to send.
     *  \return An asynchronous flag indicating if the delivery (not the processing!) of the message
     *  was successful.
     * 
     *  \note Thread affinity: any
     */
    virtual TSendResult Send(IMessagePtr message) = 0;

    //! Terminates the bus.
    /*!
     *  Does not block -- termination typically happens in background.
     *  It is safe to call this method multiple times.
     *  On terminated the instance is no longer usable.

     *  \note Thread affinity: any.
     */
    virtual void Terminate(const TError& error = TError("Bus terminated")) = 0;

    //! Invoked upon bus termination
    //! (either due to call to #Terminate or other party's failure).
    DECLARE_INTERFACE_SIGNAL(void(TError), Terminated);
};

////////////////////////////////////////////////////////////////////////////////

//! Handles incoming bus messages.
struct IMessageHandler
    : public virtual TRefCounted
{
    //! Raised whenever a new message arrives via the bus.
    /*!
     *  \param message The just arrived message.
     *  \param replyBus A bus that can be used for replying back.
     *  
     *  \note Thread affinity: the method is called from an unspecified thread
     *  and must return ASAP.
     *  
     */
    virtual void OnMessage(IMessagePtr message, IBusPtr replyBus) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
