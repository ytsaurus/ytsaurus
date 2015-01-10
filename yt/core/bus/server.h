#pragma once

#include "public.h"

#include <core/ytree/public.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

//! A server-side bus listener.
/*!
 *  An instance on this interface listens for incoming
 *  messages and notifies IMessageHandlerPtr.
 */
struct IBusServer
    : public virtual TRefCounted
{
    //! Starts the listener.
    /*
     *  \param handler Incoming messages handler.
     */
    virtual void Start(IMessageHandlerPtr handler) = 0;

    //! Stops the listener.
    /*!
     *  After this call the instance is no longer usable.
     *  No new incoming messages are accepted.
     */
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBusServer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
