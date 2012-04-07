#pragma once

#include "common.h"
#include "bind.h"
#include "callback.h"

#include <util/system/event.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Represents a result of an asynchronous computation.
/*!
 *  Thread-affinity: any.
 */
template <class T>
class TFuture
    : public TIntrinsicRefCounted
{
    volatile bool IsSet_;
    T Value;
    mutable TSpinLock SpinLock;
    mutable ::THolder<Event> ReadyEvent;

    yvector< TCallback<void(T)> > Subscribers;

public:
    typedef TIntrusivePtr<TFuture> TPtr;

    //! Initializes an empty (not set) instance.
    TFuture();

    //! Initializes an instance carrying a synchronously computed value.
    explicit TFuture(T value);

    // TODO: T -> const T&
    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set(T value);

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    T Get() const;

    //! Returns an already set value, if any.
    /*
     *  \param value The value.
     *  \return True iff the value is set.
     */
    bool TryGet(T* value) const;

    //! Checks if the value is set.
    bool IsSet() const;
    
    //! Attaches a listener.
    /*!
     *  \param action An action to call when the value gets set.
     *  
     *  \note
     *  If the value is set before the call to #Subscribe the
     *  #action gets called synchronously.
     */
    void Subscribe(TCallback<void(T)> action);

    //! Chains the asynchronous computation with another synchronous function.
    template <class R>
    TIntrusivePtr< TFuture<R> >
    Apply(TCallback<R(T)> func);

    //! Chains the asynchronous computation with another asynchronous function.
    template <class R>
    TIntrusivePtr< TFuture<R> >
    Apply(TCallback<TIntrusivePtr< TFuture<R> >(T)> func);

    //! Casts the result when its ready.
    template <class R>
    TIntrusivePtr< TFuture<R> > CastTo();
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a pre-set future from a value.
template <class T>
typename TFuture<T>::TPtr MakeFuture(const T& value)
{
    return New< TFuture<T> >(value);
}

template <class T>
void WaitForFuture(
    TIntrusivePtr< TFuture<T> > promise,
    TDuration timeout,
    TCallback<void(T)> onResult,
    TCallback<void()> onTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FUTURE_INL_H_
#include "future-inl.h"
#undef FUTURE_INL_H_
