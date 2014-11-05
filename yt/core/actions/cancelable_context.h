#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A trivial wrapper that maintains a flag indicating if the context is canceled
//! and enables creating invokers that do nothing for canceled contexts.
/*!
 *  \note
 *  Thread-affinity: any
 */
class TCancelableContext
    : public TRefCounted
{
public:
    //! Returns True iff the context is canceled.
    bool IsCanceled() const;

    //! Marks the context as canceled.
    void Cancel();

    //! Creates a new invoker wrapping the existing one.
    /*!
     *  Actions are executed by the underlying invoker as long as the context
     *  is not canceled. Double check is employed: the first one happens
     *  at the moment an action is enqueued and the second one -- when
     *  the action gets executed.
     */
    IInvokerPtr CreateInvoker(IInvokerPtr underlyingInvoker);

private:
    class TCancelableInvoker;

    volatile bool Canceled_ = false;

};

DEFINE_REFCOUNTED_TYPE(TCancelableContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
