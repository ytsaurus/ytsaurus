#pragma once

#include "public.h"
#include "callback.h"
#include "invoker.h"

#include <core/misc/nullable.h>
#include <core/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*
 *  Futures and Promises come in pairs and provide means for one party
 *  to wait for the result of the computation performed by the other party.
 *
 *  TPromise<T> encapsulates the value-returning mechanism while
 *  TFuture<T> enables the clients to wait for this value.
 *  The value type is always TErrorOr<T> (which reduces to just TError for |T = void|).
 *
 *  TPromise<T> is implicitly convertible to TFuture<T> while the reverse conversion
 *  is not allowed. This prevents a "malicious" client from setting the value
 *  by itself.
 *
 *  TPromise<T> and TFuture<T> are lightweight refcounted handles pointing to the internal
 *  shared state. TFuture<T> acts as a weak reference while TPromise<T> acts as
 *  a strong reference. When no outstanding strong references (i.e. futures) to
 *  the shared state remain, the state automatically becomes failed
 *  with NYT::EErrorCode::Canceled error code.
 *
 *  Futures and Promises are thread-safe.
 */

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
class TPromiseState;

template <class T>
void Ref(TPromiseState<T>* state);
template <class T>
void Unref(TPromiseState<T>* state);

template <class T>
class TFutureState;

template <class T>
void Ref(TFutureState<T>* state);
template <class T>
void Unref(TFutureState<T>* state);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Creates an empty (unset) promise.
template <class T>
TPromise<T> NewPromise();

//! Constructs a pre-set promise.
// FIXME(babenko): pass by const-ref, pass by rvalue-ref
template <class T>
TPromise<T> MakePromise(TErrorOr<T> value);
template <class T>
TPromise<T> MakePromise(T value);

//! Constructs a successful pre-set future.
template <class T>
TFuture<T> MakeFuture(TErrorOr<T> value);
template <class T>
TFuture<T> MakeFuture(T value);

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs);

template <class T>
bool operator!=(const TFuture<T>& lhs, const TFuture<T>& rhs);

template <class T>
void swap(TFuture<T>& lhs, TFuture<T>& rhs);

template <class T>
bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs);

template <class T>
bool operator!=(const TPromise<T>& lhs, const TPromise<T>& rhs);

template <class T>
void swap(TPromise<T>& lhs, TPromise<T>& rhs);

////////////////////////////////////////////////////////////////////////////////
// A bunch of widely-used preset futures.
 
//! A pre-set successful |void| future.
extern const TFuture<void> VoidFuture;

//! A pre-set successful |bool| future with |true| value.
extern const TFuture<bool> TrueFuture;

//! A pre-set successful |bool| future with |false| value.
extern const TFuture<bool> FalseFuture;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFutureBase;

template <class T>
class TPromiseBase;

////////////////////////////////////////////////////////////////////////////////

//! A base class for both TFuture<T> and its specialization TFuture<void>.
template <class T>
class TFutureBase
{
public:
    using TValueType = T;

    //! Creates a null future.
    TFutureBase() = default;

    //! Checks if the future is null.
    explicit operator bool() const;

    //! Drops underlying associated state resetting the future to null.
    void Reset();

    //! Checks if the value is set.
    bool IsSet() const;

    //! Checks if the future is canceled.
    bool IsCanceled() const;

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    const TErrorOr<T>& Get() const;

    //! Gets the value if set.
    /*!
     *  This call does not block.
     */
    TNullable<TErrorOr<T>> TryGet() const;

    //! Attaches a result handler.
    /*!
     *  \param handler A callback to call when the value gets set
     *  (passing the value as a parameter).
     *
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously.
     */
    void Subscribe(TCallback<void(const TErrorOr<T>&)> handler);

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel();

    //! Returns a future that is either set to an actual value (if the original one is set in timely manner)
    //! or to |EErrorCode::Timeout| (in case of timeout).
    TFuture<T> WithTimeout(TDuration timeout);

    //! Chains the asynchronous computation with another synchronous function.
    template <class R>
    TFuture<R> Apply(TCallback<R(const TErrorOr<T>&)> callback);

    //! Chains the asynchronous computation with another asynchronous function.
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(const TErrorOr<T>&)> callback);

    //! Converts (successful) result to |U|; propagates errors as is.
    template <class U>
    TFuture<U> As();

protected:
    explicit TFutureBase(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl);

    TIntrusivePtr<NYT::NDetail::TFutureState<T>> Impl_;

    template <class U>
    friend bool operator==(const TFuture<U>& lhs, const TFuture<U>& rhs);
    template <class U>
    friend bool operator!=(const TFuture<U>& lhs, const TFuture<U>& rhs);
    template <class U>
    friend void swap(TFuture<U>& lhs, TFuture<U>& rhs);

};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFuture
    : public TFutureBase<T>
{
public:
    TFuture() = default;
    TFuture(TNull);

    template <class R>
    TFuture<R> Apply(TCallback<R(const T&)> callback);
    template <class R>
    TFuture<R> Apply(TCallback<R(T)> callback);
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(const T&)> callback);
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(T)> callback);
    using TFutureBase<T>::Apply;

private:
    explicit TFuture(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl);

    template <class U>
    friend TFuture<U> MakeFuture(TErrorOr<U> value);
    template <class U>
    friend TFuture<U> MakeFuture(U value);
    template <class U>
    friend class TPromiseBase;

};

////////////////////////////////////////////////////////////////////////////////

template <>
class TFuture<void>
    : public TFutureBase<void>
{
public:
    TFuture() = default;
    TFuture(TNull);

    template <class R>
    TFuture<R> Apply(TCallback<R()> callback);
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>()> callback);
    using TFutureBase<void>::Apply;

private:
    explicit TFuture(const TIntrusivePtr<NYT::NDetail::TFutureState<void>> impl);

    template <class U>
    friend TFuture<U> MakeFuture(TErrorOr<U> value);
    template <class U>
    friend class TPromiseBase;

};

////////////////////////////////////////////////////////////////////////////////

//! A base class for both TPromise<T> and its specialization TPromise<void>.
template <class T>
class TPromiseBase
{
public:
    using TValueType = T;

    //! Creates a null promise.
    TPromiseBase() = default;

    //! Checks if the promise is null.
    explicit operator bool() const;

    //! Drops underlying associated state resetting the promise to null.
    void Reset();

    //! Checks if the value is set.
    bool IsSet() const;

    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set(const TErrorOr<T>& value);
    void Set(TErrorOr<T>&& value);

    //! Sets the value when #another future is set.
    template <class U>
    void SetFrom(TFuture<U> another);

    //! Atomically invokes |Set|, if not already set or canceled.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool TrySet(const TErrorOr<T>& value);
    bool TrySet(TErrorOr<T>&& value);

    //! Similar to #SetFrom but calls #TrySet instead of #Set.
    template <class U>
    void TrySetFrom(TFuture<U> another);

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    const TErrorOr<T>& Get() const;

    //! Gets the value if set.
    /*!
     *  This call does not block.
     */
    TNullable<TErrorOr<T>> TryGet() const;

    //! Checks if the promise is canceled.
    bool IsCanceled() const;

    //! Attaches a cancellation handler.
    /*!
     *  \param handler A callback to call when TFuture<T>::Cancel is triggered
     *  by the client.
     *
     *  \note
     *  If the value is set before the call to #handlered, then
     *  #handler is discarded.
     */
    void OnCanceled(TClosure handler);

    //! Converts promise into future.
    operator TFuture<T>() const;
    TFuture<T> ToFuture() const;

protected:
    explicit TPromiseBase(const TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl);

    TIntrusivePtr<NYT::NDetail::TPromiseState<T>> Impl_;

    template <class U>
    friend bool operator==(const TPromise<U>& lhs, const TPromise<U>& rhs);
    template <class U>
    friend bool operator!=(const TPromise<U>& lhs, const TPromise<U>& rhs);
    template <class U>
    friend void swap(TPromise<U>& lhs, TPromise<U>& rhs);

};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPromise
    : public TPromiseBase<T>
{
public:
    TPromise() = default;
    TPromise(TNull);

    void Set(const T& value);
    void Set(T&& value);
    void Set(const TError& error);
    void Set(TError&& error);
    using TPromiseBase<T>::Set;

    bool TrySet(const T& value);
    bool TrySet(T&& value);
    bool TrySet(const TError& error);
    bool TrySet(TError&& error);
    using TPromiseBase<T>::TrySet;

private:
    explicit TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl);

    template <class U>
    friend TPromise<U> NewPromise();
    template <class U>
    friend TPromise<U> MakePromise(TErrorOr<U> value);
    template <class U>
    friend TPromise<U> MakePromise(U value);

};

////////////////////////////////////////////////////////////////////////////////

template <>
class TPromise<void>
    : public TPromiseBase<void>
{
public:
    TPromise() = default;
    TPromise(TNull);

    void Set();
    using TPromiseBase<void>::Set;

    bool TrySet();
    using TPromiseBase<void>::TrySet;

private:
    explicit TPromise(const TIntrusivePtr<NYT::NDetail::TPromiseState<void>> state);

    template <class U>
    friend TPromise<U> NewPromise();
    template <class U>
    friend TPromise<U> MakePromise(TErrorOr<U> value);

};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFutureCombineTraits
{
    using TCombined = std::vector<T>;
};

template <>
struct TFutureCombineTraits<void>
{
    using TCombined = void;
};

template <class T>
TFuture<typename TFutureCombineTraits<T>::TCombined>
Combine(std::vector<TFuture<T>> items);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FUTURE_INL_H_
#include "future-inl.h"
#undef FUTURE_INL_H_
