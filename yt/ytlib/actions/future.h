#pragma once

#include "common.h"
#include "callback_forward.h"

#include <ytlib/misc/nullable.h>

#include <util/system/event.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Internal state holding the value.
template <class T>
class TPromiseState;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFuture;

template <class T>
class TPromise;

//! Constructs an empty promise.
template <class T>
TPromise<T> NewPromise();

//! Constructs a pre-set future.
template <class T>
TFuture< typename NMpl::TDecay<T>::TType > MakeFuture(T&& value);

TFuture<void> MakeFuture();

//! Constructs a pre-set promise.
template <class T>
TPromise< typename NMpl::TDecay<T>::TType > MakePromise(T&& value);

TPromise<void> MakePromise();

////////////////////////////////////////////////////////////////////////////////

//! Represents a read-only view of an asynchronous computation.
/*
 *  Futures and Promises come in pairs and provide means for one party
 *  to wait for the result of the computation performed by the other party.
 *  
 *  TPromise encapsulates the value-returning mechanism while
 *  TFuture enables the clients to wait for this value.
 *  
 *  TPromise is implicitly convertible to TFuture while the reverse conversion
 *  is not allowed. This prevents a "malicious" client from setting the value
 *  by itself.
 *  
 *  Futures and Promises are thread-safe.
 */
template <class T>
class TFuture
{
public:
    typedef T TValueType;

    //! Empty constructor.
    TFuture();

    //! Copy constructor.
    TFuture(const TFuture& other);

    //! Move constructor.
    TFuture(TFuture&& other);

    typedef TIntrusivePtr< NYT::NDetail::TPromiseState<T> > TFuture::* TUnspecifiedBoolType;
    //! Checks if the future is associated with a state.
    operator TUnspecifiedBoolType() const;

    //! Checks if the future is associated with a state.
    bool IsNull() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TFuture& other);

    //! Copy assignment.
    TFuture<T>& operator=(const TFuture<T>& other);

    //! Move assignment.
    TFuture<T>& operator=(TFuture<T>&& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    const T& Get() const;

    //! Gets the value if set.
    /*!
     *  This call will not block until the value is set.
     */
    TNullable<T> TryGet() const;

    //! Attaches a listener.
    /*!
     *  \param callback A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(const TCallback<void(T)>& listener);

    //! Attaches a listener.
    /*!
     *  \param timeout Asynchronously wait for the specified time before
     *  dropping the subscription.
     *  \param onValue A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  \param onTimeout A callback to call when the timeout exceeded.
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(
        TDuration timeout,
        const TCallback<void(T)>& onValue,
        const TClosure& onTimeout);

    //! Chains the asynchronous computation with another synchronous function.
    TFuture<void> Apply(const TCallback<void(T)>& mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    TFuture<void> Apply(const TCallback<TFuture<void>(T)>& mutator);

    //! Chains the asynchronous computation with another synchronous function.
    template <class R>
    TFuture<R> Apply(const TCallback<R(T)>& mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    template <class R>
    TFuture<R> Apply(const TCallback<TFuture<R>(T)>& mutator);

private:
    explicit TFuture(const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state);
    explicit TFuture(TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state);

    TIntrusivePtr< NYT::NDetail::TPromiseState<T> > Impl;

private:
    friend class TPromise<T>;

    template <class U>
    friend TFuture< typename NMpl::TDecay<U>::TType > MakeFuture(U&& value);

    template <class U>
    friend bool operator==(const TFuture<U>& lhs, const TFuture<U>& rhs);
    template <class U>
    friend bool operator!=(const TFuture<U>& lhs, const TFuture<U>& rhs);

};

////////////////////////////////////////////////////////////////////////////////

//! #TFuture<> specialized for |void| type.
template <>
class TFuture<void>
{
public:
    typedef void TValueType;

    //! Empty constructor.
    TFuture();

    //! Copy constructor.
    TFuture(const TFuture& other);

    //! Move constructor.
    TFuture(TFuture&& other);

    typedef TIntrusivePtr< NYT::NDetail::TPromiseState<void> > TFuture::* TUnspecifiedBoolType;
    //! Checks if the future is associated with a state.
    operator TUnspecifiedBoolType() const;

    //! Checks if the future is associated with a state.
    bool IsNull() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TFuture& other);

    //! Copy assignment.
    TFuture<void>& operator=(const TFuture<void>& other);

    //! Move assignment.
    TFuture<void>& operator=(TFuture<void>&& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Synchronously waits until #Set is called.
    void Get() const;

    //! Attaches a listener.
    /*!
     *  \param callback A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(const TClosure& listener);

    //! Attaches a listener.
    /*!
     *  \param timeout Asynchronously wait for the specified time before
     *  dropping the subscription.
     *  \param onValue A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  \param onTimeout A callback to call when the timeout exceeded.
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(
        TDuration timeout,
        const TClosure& onValue,
        const TClosure& onTimeout);

    //! Chains the asynchronous computation with another synchronous function.
    TFuture<void> Apply(const TCallback<void()>& mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    TFuture<void> Apply(const TCallback<TFuture<void>()>& mutator);

    //! Chains the asynchronous computation with another synchronous function.
    template <class R>
    TFuture<R> Apply(const TCallback<R()>& mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    template <class R>
    TFuture<R> Apply(const TCallback<TFuture<R>()>& mutator);

private:
    explicit TFuture(const TIntrusivePtr< NYT::NDetail::TPromiseState<void> >& state);
    explicit TFuture(TIntrusivePtr< NYT::NDetail::TPromiseState<void> >&& state);

    TIntrusivePtr< NYT::NDetail::TPromiseState<void> > Impl;

private:
    friend class TPromise<void>;

    friend TFuture<void> MakeFuture();

    template <class U>
    friend bool operator==(const TFuture<U>& lhs, const TFuture<U>& rhs);
    template <class U>
    friend bool operator!=(const TFuture<U>& lhs, const TFuture<U>& rhs);

};

////////////////////////////////////////////////////////////////////////////////

//! #TFuture<> equality operator.
template <class T>
bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs);

//! #TFuture<> inequality operator.
template <class T>
bool operator!=(const TFuture<T>& lhs, const TFuture<T>& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Encapsulates the value-returning mechanism.
template <class T>
class TPromise
{
public:
    typedef T TValueType;

    //! Empty constructor.
    //TPromise();

    //! Empty constructor.
    TPromise(TNull);

    //! Copy constructor.
    TPromise(const TPromise& other);

    //! Move constructor.
    TPromise(TPromise&& other);

    typedef TIntrusivePtr< NYT::NDetail::TPromiseState<T> > TPromise::*TUnspecifiedBoolType;
    //! Checks if the promise is associated with a state.
    operator TUnspecifiedBoolType() const;

    //! Checks if the promise is associated with a state.
    bool IsNull() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TPromise& other);

    //! Copy assignment.
    TPromise<T>& operator=(const TPromise<T>& other);

    //! Move assignment.
    TPromise<T>& operator=(TPromise<T>&& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set(const T& value);
    void Set(T&& value);

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    const T& Get() const;

    //! Gets the value if set.
    /*!
     *  This call will not block until the value is set.
     */
    TNullable<T> TryGet() const;

    //! Attaches a listener.
    /*!
     *  \param callback A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(const TCallback<void(T)>& action);

    //! Attaches a listener.
    /*!
     *  \param timeout Asynchronously wait for the specified time before
     *  dropping the subscription.
     *  \param onValue A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  \param onTimeout A callback to call when the timeout exceeded.
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(
        TDuration timeout,
        const TCallback<void(T)>& onValue,
        const TClosure& onTimeout);

    TFuture<T> ToFuture() const;
    operator TFuture<T>() const;

private:
    explicit TPromise(const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state);
    explicit TPromise(TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state);

    TIntrusivePtr< NYT::NDetail::TPromiseState<T> > Impl;

private:
    TPromise();
    friend class TFuture<T>;

    template <class U>
    friend TPromise<U> NewPromise();
    template <class U>
    friend TPromise< typename NMpl::TDecay<U>::TType > MakePromise(U&& value);

    template <class U>
    friend bool operator==(const TPromise<U>& lhs, const TPromise<U>& rhs);
    template <class U>
    friend bool operator!=(const TPromise<U>& lhs, const TPromise<U>& rhs);

};

////////////////////////////////////////////////////////////////////////////////

//! #TPromise<> specialized for |void| type.

//! Encapsulates the value-returning mechanism.
template <>
class TPromise<void>
{
public:
    typedef void TValueType;

    //! Empty constructor.
    //TPromise();

    //! Empty constructor.
    TPromise(TNull);

    //! Copy constructor.
    TPromise(const TPromise& other);

    //! Move constructor.
    TPromise(TPromise&& other);

    typedef TIntrusivePtr< NYT::NDetail::TPromiseState<void> > TPromise::* TUnspecifiedBoolType;
    //! Checks if the promise is associated with a state.
    operator TUnspecifiedBoolType() const;

    //! Checks if the promise is associated with a state.
    bool IsNull() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TPromise& other);

    //! Copy assignment.
    TPromise<void>& operator=(const TPromise<void>& other);

    //! Move assignment.
    TPromise<void>& operator=(TPromise<void>&& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set();

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    void Get() const;

    //! Attaches a listener.
    /*!
     *  \param callback A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(const TClosure& listener);

    //! Attaches a listener.
    /*!
     *  \param timeout Asynchronously wait for the specified time before
     *  dropping the subscription.
     *  \param onValue A callback to call when the value gets set
     *  (passing the value as a parameter).
     *  \param onTimeout A callback to call when the timeout exceeded.
     *  
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(
        TDuration timeout,
        const TClosure& onValue,
        const TClosure& onTimeout);

    TFuture<void> ToFuture() const;
    operator TFuture<void>() const;

private:
    explicit TPromise(const TIntrusivePtr< NYT::NDetail::TPromiseState<void> >& state);
    explicit TPromise(TIntrusivePtr< NYT::NDetail::TPromiseState<void> >&& state);

    TIntrusivePtr< NYT::NDetail::TPromiseState<void> > Impl;

private:
    TPromise();
    friend class TFuture<void>;

    template <class U>
    friend TPromise<U> NewPromise();
    friend TPromise<void> MakePromise();

    template <class U>
    friend bool operator==(const TPromise<U>& lhs, const TPromise<U>& rhs);
    template <class U>
    friend bool operator!=(const TPromise<U>& lhs, const TPromise<U>& rhs);

};

////////////////////////////////////////////////////////////////////////////////

//! #TPromise<> equality operator.
template <class T>
bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs);

//! #TPromise<> inequality operator.
template <class T>
bool operator!=(const TPromise<T>& lhs, const TPromise<T>& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FUTURE_INL_H_
#include "future-inl.h"
#undef FUTURE_INL_H_
