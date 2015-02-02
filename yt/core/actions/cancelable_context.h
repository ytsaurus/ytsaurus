#pragma once

#include "public.h"
#include "signal.h"
#include "future.h"

#include <core/misc/weak_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Maintains a flag indicating if the context is canceled.
//! Propagates cancelation to other contexts and futures.
/*!
 *  \note
 *  Thread-affinity: any
 */
class TCancelableContext
    : public TRefCounted
{
public:
    //! Returns |true| iff the context is canceled.
    bool IsCanceled() const;

    //! Marks the context as canceled raising the handlers
    //! and propagates cancelation.
    void Cancel();

    //! Raised when the context is canceled.
    DECLARE_SIGNAL(void(), Canceled);

    //! Registers another context for propagating cancelation.
    void PropagateTo(TCancelableContextPtr context);

    //! Registers a future for propagating cancelation.
    template <class T>
    void PropagateTo(TFuture<T> future);
    void PropagateTo(TFuture<void> future);

    //! Creates a new invoker wrapping the existing one.
    /*!
     *  Callbacks are executed by the underlying invoker as long as the context
     *  is not canceled. Double check is employed: the first one happens
     *  at the instant the callback is enqueued and the second one -- when
     *  the callback starts executing.
     */
    IInvokerPtr CreateInvoker(IInvokerPtr underlyingInvoker);

private:
    class TCancelableInvoker;

    TSpinLock SpinLock_;
    bool Canceled_ = false;
    TCallbackList<void()> Handlers_;
    yhash_set<TWeakPtr<TCancelableContext>> PropagateToContexts_;
    yhash_set<TFuture<void>> PropagateToFutures_;

};

DEFINE_REFCOUNTED_TYPE(TCancelableContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CANCELABLE_CONTEXT_INL_H_
#include "cancelable_context-inl.h"
#undef CANCELABLE_CONTEXT_INL_H_
