#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/enum.h>

#include <core/actions/signal.h>
#include <core/actions/future.h>

#include <core/ytree/public.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDeliveryTrackingLevel,
    (None)
    (ErrorOnly)
    (Full)
);

//! A bus, i.e. something capable of transmitting messages.
struct IBus
    : public virtual TRefCounted
{
    //! Returns a textual representation of bus' endpoint.
    //! For informative uses only.
    virtual NYTree::TYsonString GetEndpointDescription() const = 0;

    //! Asynchronously sends a message via the bus.
    /*!
     *  \param message A message to send.
     *  \return An asynchronous flag indicating if the delivery (not the processing!) of the message
     *  was successful.
     *
     *  \note Thread affinity: any
     */
    virtual TFuture<void> Send(TSharedRefArray message, EDeliveryTrackingLevel level) = 0;

    //! Terminates the bus.
    /*!
     *  Does not block -- termination typically happens in background.
     *  It is safe to call this method multiple times.
     *  On terminated the instance is no longer usable.

     *  \note Thread affinity: any.
     */
    virtual void Terminate(const TError& error) = 0;

    //! Invoked upon bus termination
    //! (either due to call to #Terminate or other party's failure).
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Terminated);
};

DEFINE_REFCOUNTED_TYPE(IBus)

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
     *  \note
     *  Thread affinity: the method is called from an unspecified thread
     *  and must return ASAP.
     *
     */
    virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMessageHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
