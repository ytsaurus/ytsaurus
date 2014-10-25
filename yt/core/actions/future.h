#pragma once

#include "public.h"

#include <core/misc/public.h>
#include <core/misc/nullable.h>
#include <core/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Internal state holding the value.
template <class T>
class TPromiseState;

//! Internal state holding the value.
template <>
class TPromiseState<void>;
void Ref(TPromiseState<void>* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(TPromiseState<void>* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Creates an empty (unset) promise.
template <class T>
TPromise<T> NewPromise();

//! Creates an empty (unset) void promise.
template <>
TPromise<void> NewPromise<>();

//! Creates an empty (unset) void promise.
TPromise<void> NewPromise();

//! Constructs a pre-set future.
template <class T>
TFuture<typename NMpl::TDecay<T>::TType> MakeFuture(T&& value);

//! Constructs a pre-set void future.
TFuture<void> MakeFuture();

//! Constructs a pre-set promise.
template <class T>
TPromise<typename NMpl::TDecay<T>::TType> MakePromise(T&& value);

//! Constructs a pre-set void promise.
TPromise<void> MakePromise();

//! Constructs a future that gets set when a given #delay elapses.
TFuture<void> MakeDelayed(TDuration delay);

// A bunch of widely-used preset futures.
 
//! A pre-set |void| future.
extern TFuture<void> VoidFuture;

//! A pre-set |bool| future with |true| value.
extern TFuture<bool> TrueFuture;

//! A pre-set |bool| future with |false| value.
extern TFuture<bool> FalseFuture;

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

    //! Empty constructor.
    TFuture(TNull);

    //! Checks if the future is associated with a state.
    explicit operator bool() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TFuture& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Checks if the future is canceled.
    bool IsCanceled() const;

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

    //! Attaches a result listener.
    /*!
     *  \param onResult A callback to call when the value gets set
     *  (passing the value as a parameter).
     *
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(TCallback<void(T)> onResult);

    //! Does exactly same thing as its TPromise counterpart.
    //! Gives the consumder a chance to handle cancelation.
    void OnCanceled(TClosure onCancel);

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel();

    //! Chains the asynchronous computation with another synchronous function.
    TFuture<void> Apply(TCallback<void(T)> mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    TFuture<void> Apply(TCallback<TFuture<void>(T)> mutator);

    //! Chains the asynchronous computation with another synchronous function.
    template <class R>
    TFuture<R> Apply(TCallback<R(T)> mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(T)> mutator);

    //! Converts into a void future by effectively discarding the value.
    TFuture<void> IgnoreResult();

    //! Returns a void future that is set when the original future
    //! is either set or canceled.
    TFuture<void> Finally();

    //! Returns a future that is either set to an actual value (if the original one is set in timely manner)
    //! or to |EErrorCode::Timeout| (in case of timeout).
    TFuture<typename TErrorTraits<T>::TWrapped> WithTimeout(TDuration timeout);

private:
    explicit TFuture(const TIntrusivePtr<NYT::NDetail::TPromiseState<T>>& state);
    explicit TFuture(TIntrusivePtr<NYT::NDetail::TPromiseState<T>>&& state);

    TIntrusivePtr<NYT::NDetail::TPromiseState<T>> Impl_;

private:
    friend class TPromise<T>;

    template <class U>
    friend TFuture<typename NMpl::TDecay<U>::TType> MakeFuture(U&& value);

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

    //! Empty constructor.
    TFuture(TNull);

    //! Checks if the future is associated with a state.
    explicit operator bool() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TFuture& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Checks if the future is canceled.
    bool IsCanceled() const;

    //! Synchronously waits until #Set is called.
    void Get() const;

    //! Attaches a result listener.
    /*!
     *  \param onResult A callback to call when the value gets set
     *  (passing the value as a parameter).
     *
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(TClosure onResult);

    //! Does exactly same thing as its TPromise counterpart.
    //! Gives the consumer a chance to handle cancelation.
    void OnCanceled(TClosure onCancel);

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel();

    //! Chains the asynchronous computation with another synchronous function.
    TFuture<void> Apply(TCallback<void()> mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    TFuture<void> Apply(TCallback<TFuture<void>()> mutator);

    //! Chains the asynchronous computation with another synchronous function.
    template <class R>
    TFuture<R> Apply(TCallback<R()> mutator);

    //! Chains the asynchronous computation with another asynchronous function.
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>()> mutator);

    //! Returns a void future that is set when the original future
    //! is either set or canceled.
    TFuture<void> Finally();

    //! Returns a future that is either set to OK (if the original one is set in timely manner)
    //! or to |EErrorCode::Timeout| (in case of timeout).
    TFuture<TError> WithTimeout(TDuration timeout);

private:
    explicit TFuture(const TIntrusivePtr<NYT::NDetail::TPromiseState<void>>& state);
    explicit TFuture(TIntrusivePtr<NYT::NDetail::TPromiseState<void>>&& state);

    TIntrusivePtr<NYT::NDetail::TPromiseState<void>> Impl_;

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
    TPromise();

    //! Empty constructor.
    TPromise(TNull);

    //! Checks if the promise is associated with a state.
    explicit operator bool() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TPromise& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set(const T& value);
    void Set(T&& value);

    //! Sets the value when #another future is set.
    template <class U>
    void SetFrom(TFuture<U> another);

    //! Atomically invokes |Set|, if not already set or canceled.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool TrySet(const T& value);
    bool TrySet(T&& value);

    //! Similar to #SetFrom but calls #TrySet instead of #Set.
    template <class U>
    void TrySetFrom(TFuture<U> another);

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

    //! Attaches a result listener.
    /*!
     *  \param onResult A callback to call when the value gets set
     *  (passing the value as a parameter).
     *
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(TCallback<void(T)> onResult);

    //! Attaches a cancellation listener.
    /*!
     *  \param onCancel A callback to call when #TFuture<T>::Cancel is triggered
     *  by the client.
     *
     *  \note
     *  If the value is set before the call to #OnCanceled, then
     *  #onCancel is discarded.
     */
    void OnCanceled(TClosure onCancel);

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel();

    TFuture<T> ToFuture() const;
    operator TFuture<T>() const;

private:
    explicit TPromise(const TIntrusivePtr<NYT::NDetail::TPromiseState<T>>& state);
    explicit TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<T>>&& state);

    TIntrusivePtr<NYT::NDetail::TPromiseState<T>> Impl_;

private:
    friend class TFuture<T>;

    template <class U>
    friend TPromise<U> NewPromise();
    template <class U>
    friend TPromise<typename NMpl::TDecay<U>::TType> MakePromise(U&& value);

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
    TPromise();

    //! Empty constructor.
    TPromise(TNull);

    //! Checks if the promise is associated with a state.
    explicit operator bool() const;

    //! Drops underlying associated state.
    void Reset();

    //! Swaps underlying associated state.
    void Swap(TPromise& other);

    //! Checks if the value is set.
    bool IsSet() const;

    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set();

    //! Sets the value from #another future when the latter is set.
    template <class U>
    void SetFrom(TFuture<U> another);

    //! Sets the value from #another future when the latter is set.
    void SetFrom(TFuture<void> another);

    //! Atomically sets the promise, if not already set or canceled.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    bool TrySet();

    //! Similar to #SetFrom but calls #TrySet instead of #Set.
    template <class U>
    void TrySetFrom(TFuture<U> another);

    //! Similar to #SetFrom but calls #TrySet instead of #Set.
    void TrySetFrom(TFuture<void> another);

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    void Get() const;

    //! Attaches a result listener.
    /*!
     *  \param onResult A callback to call when the value gets set
     *  (passing the value as a parameter).
     *
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #onResult gets called synchronously.
     */
    void Subscribe(TClosure onResult);

    //! Attaches a cancellation listener.
    /*!
     *  \param onCancel A callback to call when #TFuture<void>::Cancel is triggered
     *  by the client.
     *
     *  \note
     *  If the value is set before the call to #OnCanceled, then
     *  #onCancel is discarded.
     */
    void OnCanceled(TClosure onCancel);

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel();

    TFuture<void> ToFuture() const;
    operator TFuture<void>() const;

private:
    explicit TPromise(const TIntrusivePtr<NYT::NDetail::TPromiseState<void>>& state);
    explicit TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<void>>&& state);

    TIntrusivePtr<NYT::NDetail::TPromiseState<void>> Impl_;

private:
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

//! Cancels a given future at the end of the scope.
/*!
 *  \note
 *  Cancelation has no effect if the future is already set.
 */
template <class T>
class TFutureCancelationGuard
{
public:
    explicit TFutureCancelationGuard(TFuture<T> future);
    TFutureCancelationGuard(TFutureCancelationGuard<T>&& other);
    ~TFutureCancelationGuard();

    TFutureCancelationGuard<T>& operator=(TFutureCancelationGuard<T>&& other);

    template <class U>
    friend void swap(TFutureCancelationGuard<U>& lhs, TFutureCancelationGuard<U>& rhs);

    void Release();

    explicit operator bool() const;

private:
    TFuture<T> Future_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FUTURE_INL_H_
#include "future-inl.h"
#undef FUTURE_INL_H_
