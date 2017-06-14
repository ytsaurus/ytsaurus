#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Describes the types of events a pollable entity may be interested in.
DEFINE_BIT_ENUM(EPollControl,
    ((None) (0x0))
    ((Read) (0x1))
    ((Write)(0x2))
);

////////////////////////////////////////////////////////////////////////////////

//! Describes an FD-backed pollable entity.
struct IPollable
    : public virtual TRefCounted
{
    //! Returns a human-readable string used for diagnostic purposes.
    virtual const TString& GetLoggingId() const = 0;

    //! Called by the poller when the appropriate event is trigged for the FD.
    virtual void OnEvent(EPollControl control) = 0;

    //! Called by the poller when the pollable entity is unregistered.
    virtual void OnShutdown() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPollable)

////////////////////////////////////////////////////////////////////////////////

//! Enables polling a collection of IPollable-s.
/*!
 *  A poller is typically implemented as a thread or a pool of threads running
 *  a polling loop.
 *
 *  Additionally a poller provides means to execute arbitrary callbacks via IInvoker interface,
 *  \see IPoller::GetInvoker.
 */
struct IPoller
    : public virtual TRefCounted
{
    //! Shuts the poller down; e.g. reliably terminates the threads.
    //! The poller is not longer usable after this call.
    virtual void Shutdown() = 0;

    //! Registers a pollable entity but does not arm the poller yet.
    virtual void Register(const IPollablePtr& pollable) = 0;

    //! Unregisters the previously registered entity.
    /*!
     *  If the pollable entity was previously armed, one should unarm it first
     *  before unregistering. Not doing so is OK for the poller, however
     *  in this case #IPollable::OnShutdown and #IPollable::OnEvent could be invoked concurrently.
     *
     *  At the same time, if the poller was properly unarmed before unregistering,
     *  it is guaranteed that #IPollable::OnShutdown and #IPollable::OnEvent will
     *  not be run concurrently.
     *
     *  The entity gets unregistered asynchronously.
     *
     *  \returns a future that is set when the entity becomes unregistered
     *  (after #IPollable::OnShutdown is invoked)
     */
    virtual TFuture<void> Unregister(const IPollablePtr& pollable) = 0;

    //! Arms the poller to handle events of a given type for a given entity.
    virtual void Arm(int fd, const IPollablePtr& pollable, EPollControl control) = 0;

    //! Unarms the poller.
    virtual void Unarm(int fd) = 0;

    //! Returns the invoker capable of executing arbitrary callbacks
    //! in the poller's context.
    virtual IInvokerPtr GetInvoker() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPoller)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
