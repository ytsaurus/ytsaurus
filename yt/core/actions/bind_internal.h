#pragma once

#include "bind_helpers.h"
#include "bind_mpl.h"
#include "callback_internal.h"

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
#include <core/misc/source_location.h>
#endif

namespace NYT {
namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
//
// CONCEPTS
//
// === Callable ===
// A typeclass that can be called (i. e. invoked with operator()). This one
// includes canonical C++ functors, C++11 lambdas and many other things with
// properly declared operator().
//
// === TRunnable ===
// A typeclass that has a single Run() method and a TSignature
// typedef that corresponds to the type of Run(). A TRunnable can declare that
// it should treated like a method call by including a typedef named IsMethod.
// The value of this typedef is not inspected, only the existence (see
// "bind_mpl.h").
// When a TRunnable declares itself a method, #Bind() will enforce special
// weak reference handling semantics for the first argument which is expected
// to be an object (an invocation target).
//
// === TFunctor ===
// A copyable type representing something that should be called. All function
// pointers, #TCallback<>s, Callables and Runnables are functors even if
// the invocation syntax differs.
//
// === TSignature ===
// A function type (as opposed to function _pointer_ type) for a Run() function.
// Usually just a convenience typedef.
//
// === T(Bound)Args ===
// A function type that is being (ab)used to store the types of set of
// arguments. The "return" type is always void here. We use this hack so 
// that we do not need a new type name for each arity of type. (eg., BindState1, 
// BindState2, ...). This makes forward declarations and friending much much easier.
//
//
// TYPES
//
// === TCallableAdapter ===
// Wraps Callable objects into an object that adheres to the TRunnable interface.
// There are |ARITY| TCallableAdapter types.
//
// === TRunnableAdapter ===
// Wraps the various "function" pointer types into an object that adheres to the
// TRunnable interface.
//
// === TSignatureTraits ===
// Type traits that unwrap a function signature into a set of easier to use
// typedefs. Used mainly for compile time asserts.
//
// === TIgnoreResultInSignature ===
// Helper class for translating function signatures to equivalent forms with
// a "void" return type.
//
// === TFunctorTraits ===
// Type traits used determine the correct TSignature and TRunnable for
// a TRunnable. This is where function signature adapters are applied.
//
// === MakeRunnable ===
// Takes a TFunctor and returns an object in the TRunnable typeclass that
// represents the underlying TFunctor.
//
// === TInvokerHelper ===
// Take a TRunnable and arguments and actully invokes it. Also handles
// the differing syntaxes needed for #TWeakPtr<> support and for ignoring
// return values. This is separate from TInvoker to avoid creating multiple
// version of #TInvoker<> which grows at |O(n^2)| with the arity.
//
// === TInvoker ===
// Unwraps the curried parameters and executes the TRunnable.
//
// === TBindState ===
// Stores the curried parameters, and is the main entry point into the #Bind()
// system, doing most of the type resolution.
//

////////////////////////////////////////////////////////////////////////////////
// #TCallableAdapter<>
////////////////////////////////////////////////////////////////////////////////

template <class T, class TSignature>
class TCallableAdapter;

template <class R, class T, class... TArgs>
class TCallableAdapter<T, R(T::*)(TArgs...)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = sizeof...(TArgs) };
    typedef R (TSignature)(TArgs...);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(TArgs&&... args)
    {
        return Functor(std::forward<TArgs>(args)...);
    }

private:
    T Functor;
};

template <class R, class T, class... TArgs>
class TCallableAdapter<T, R(T::*)(TArgs...) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = sizeof...(TArgs) };
    typedef R (TSignature)(TArgs...);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(TArgs&&... args)
    {
        return Functor(std::forward<TArgs>(args)...);
    }

private:
    const T Functor;
};

////////////////////////////////////////////////////////////////////////////////
// #TRunnableAdapter<>
////////////////////////////////////////////////////////////////////////////////
//
// The #TRunnableAdapter<> templates provide a uniform interface for invoking
// a function pointer, method pointer, or const method pointer. The adapter
// exposes a Run() method with an appropriate signature. Using this wrapper
// allows for writing code that supports all three pointer types without
// undue repetition.  Without it, a lot of code would need to be repeated 3
// times.
//
// For method pointers and const method pointers the first argument to Run()
// is considered to be the received of the method.  This is similar to STL's
// mem_fun().
//
// This class also exposes a TSignature typedef that is the function type of the
// Run() function.
//
// If and only if the wrapper contains a method or const method pointer, an
// IsMethod typedef is exposed.  The existence of this typedef (NOT the value)
// marks that the wrapper should be considered a method wrapper.
//

template <class T>
class TRunnableAdapter
    : public TCallableAdapter<T, decltype(&T::operator())>
{
    typedef TCallableAdapter<T, decltype(&T::operator())> TBase;

public:
    explicit TRunnableAdapter(const T& functor)
        : TBase(functor)
    { }

    explicit TRunnableAdapter(T&& functor)
        : TBase(std::move(functor))
    { }
};


// Function Adapter
template <class R, class... TArgs>
class TRunnableAdapter<R(*)(TArgs...)>
{
public:
    enum { Arity = sizeof...(TArgs) };
    typedef R (TSignature)(TArgs...);

    explicit TRunnableAdapter(R(*function)(TArgs...))
        : Function(function)
    { }

    R Run(TArgs&&... args)
    {
        return Function(std::forward<TArgs>(args)...);
    }

private:
    R (*Function)(TArgs...);
};

// Bound Method Adapter
template <class R, class T, class... TArgs>
class TRunnableAdapter<R(T::*)(TArgs...)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + sizeof...(TArgs) };
    typedef R (TSignature)(T*, TArgs...);

    explicit TRunnableAdapter(R(T::*method)(TArgs...))
        : Method(method)
    { }

    R Run(T* target, TArgs&&... args)
    {
        return (target->*Method)(std::forward<TArgs>(args)...);
    }

private:
    R (T::*Method)(TArgs...);
};

// Const Bound Method Adapter
template <class R, class T, class... TArgs>
class TRunnableAdapter<R(T::*)(TArgs...) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + sizeof...(TArgs) };
    typedef R (TSignature)(const T*, TArgs...);

    explicit TRunnableAdapter(R(T::*method)(TArgs...) const)
        : Method(method)
    { }

    R Run(const T* target, TArgs&&... args)
    {
        return (target->*Method)(std::forward<TArgs>(args)...);
    }

private:
    R (T::*Method)(TArgs...) const;
};


// Callback Adapter
template <class S1, class S2>
class TCallbackRunnableAdapter;

template <class S1, class R2, class... TArgs2>
class TCallbackRunnableAdapter<S1, R2(TArgs2...)>
{
public:
    explicit TCallbackRunnableAdapter(TCallback<S1> callback)
        : Callback(std::move(callback))
    { }

    R2 Run(TArgs2&&... args)
    {
        return Callback.Run(std::forward<TArgs2>(args)...);
    }

private:
    TCallback<S1> Callback;

};

// Callback Adapter (specialized for void return type)
template <class S1, class... TArgs2>
class TCallbackRunnableAdapter<S1, void(TArgs2...)>
{
public:
    explicit TCallbackRunnableAdapter(TCallback<S1> callback)
        : Callback(std::move(callback))
    { }

    void Run(TArgs2&&... args)
    {
        Callback.Run(std::forward<TArgs2>(args)...);
    }

private:
    TCallback<S1> Callback;

};

////////////////////////////////////////////////////////////////////////////////
// #TIgnoreResultInSignature<>
////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
struct TIgnoreResultInSignature;

template <class R, class... TArgs>
struct TIgnoreResultInSignature<R(TArgs...)>
{
    typedef void(TSignature)(TArgs...);
};

////////////////////////////////////////////////////////////////////////////////
// #TFunctorTraits<>
////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFunctorTraits
{
    typedef TRunnableAdapter<T> TRunnable;
    typedef typename TRunnable::TSignature TSignature;
};

template <class T>
struct TFunctorTraits< TIgnoreResultWrapper<T>>
{
    typedef typename TFunctorTraits<T>::TRunnable TRunnable;
    typedef typename TIgnoreResultInSignature<
        typename TRunnable::TSignature
    >::TSignature TSignature;
};

template <class T>
struct TFunctorTraits< TCallback<T>>
{
    typedef TCallback<T> TRunnable;
    typedef typename TCallback<T>::TSignature TSignature;
};

////////////////////////////////////////////////////////////////////////////////
// #MakeRunnable()
////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TFunctorTraits<T>::TRunnable
MakeRunnable(const T& x)
{
    return TRunnableAdapter<T>(x);
}

template <class T>
typename TFunctorTraits<T>::TRunnable
MakeRunnable(const TIgnoreResultWrapper<T>& wrapper)
{
    return MakeRunnable(wrapper.Functor);
}

template <class T>
const typename TFunctorTraits< TCallback<T>>::TRunnable&
MakeRunnable(const TCallback<T>& x)
{
    return x;
}

////////////////////////////////////////////////////////////////////////////////
// #TInvokerHelper<>
////////////////////////////////////////////////////////////////////////////////
//
// There are 3 logical #TInvokerHelper<> specializations: normal, void-return,
// weak method calls.
//
// The normal type just calls the underlying runnable.
//
// We need a TInvokerHelper to handle void return types in order to support
// IgnoreResult().  Normally, if the TRunnable's TSignature had a void return,
// the template system would just accept "return functor.Run()" ignoring
// the fact that a void function is being used with return. This piece of
// sugar breaks though when the TRunnable's TSignature is not void.  Thus, we
// need a partial specialization to change the syntax to drop the "return"
// from the invocation call.
//
// WeakCalls similarly need special syntax that is applied to the first
// argument to check if they should no-op themselves.
//

template <bool IsWeakMethod, class TRunnable, class R, class TArgs>
struct TInvokerHelper;

template <class TRunnable, class R, class... TArgs>
struct TInvokerHelper<false, TRunnable, R, void(TArgs...)>
{
    static inline R Run(TRunnable runnable, TArgs&&... args)
    {
        return runnable.Run(std::forward<TArgs>(args)...);
    }
};

template <class TRunnable, class... TArgs>
struct TInvokerHelper<false, TRunnable, void, void(TArgs...)>
{
    static inline void Run(TRunnable runnable, TArgs&&... args)
    {
        runnable.Run(std::forward<TArgs>(args)...);
    }
};

template <class TRunnable, class A0, class... TArgs>
struct TInvokerHelper<true, TRunnable, void, void(A0, TArgs...)>
{
    static inline void Run(TRunnable runnable, A0&& a0, TArgs&&... args)
    {
        if (!a0) {
            return;
        }

        runnable.Run(std::forward<A0>(a0), std::forward<TArgs>(args)...);
    }
};

template <class TRunnable, class R, class TArgs>
struct TInvokerHelper<true, TRunnable, R, TArgs>
{
    // Weak calls are only supported for functions with a void return type.
    // Otherwise, the function result would be undefined if the #TWeakPtr<>
    // is expired.
    static_assert(NMpl::TIsVoid<R>::Value,
        "Weak calls are only supported for functions with a void return type");
};

////////////////////////////////////////////////////////////////////////////////
// #TInvoker<>
////////////////////////////////////////////////////////////////////////////////

template <class TTypedBindState, class R, class TBoundArgsPack, class TRunArgsPack, class TSequence>
struct TInvoker;

template <class TTypedBindState, class R, class... TRunArgs>
struct TInvoker<TTypedBindState, R, NMpl::TTypesPack<>, NMpl::TTypesPack<TRunArgs...>, NMpl::TSequence<>>
{
    typedef R(TUnboundSignature)(TRunArgs...);

    static R Run(TBindStateBase* stateBase, TRunArgs&&... runArgs)
    {
        auto* state = static_cast<TTypedBindState*>(stateBase);

        static_assert(!TTypedBindState::IsMethod::Value,
            "The target object for a bound method have to be bound.");

        typedef TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnable,
            R,
            void(TRunArgs...)
        > TInvokerHelperType;

        NTracing::TTraceContextGuard guard(state->TraceContext);
        return TInvokerHelperType::Run(
            state->Runnable,
            std::forward<TRunArgs>(runArgs)...);
    }
};

template <class TTypedBindState, class R, class BA0, class... TBoundArgs, class... TRunArgs, unsigned... BoundIndexes>
struct TInvoker<TTypedBindState, R, NMpl::TTypesPack<BA0, TBoundArgs...>, NMpl::TTypesPack<TRunArgs...>, NMpl::TSequence<0, BoundIndexes...>>
{
    typedef R(TUnboundSignature)(TRunArgs...);

    static R Run(TBindStateBase* stateBase, TRunArgs&&... runArgs)
    {
        auto* state = static_cast<TTypedBindState*>(stateBase);

        typedef TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnable,
            R,
            void(BA0, TBoundArgs..., TRunArgs...)
        > TInvokerHelperType;

        typedef TUnwrapTraits<typename std::tuple_element<0, typename TTypedBindState::TTuple>::type> TBoundUnwrapTraits0;
        typedef typename TBoundUnwrapTraits0::TType TBoundArg0;

        NTracing::TTraceContextGuard guard(state->TraceContext);
        return TInvokerHelperType::Run(
            state->Runnable,
            TMaybeCopyHelper<BA0>::Do(
                TMaybeLockHelper<TTypedBindState::IsMethod::Value, TBoundArg0>(
                    std::forward<TBoundArg0>(
                        TBoundUnwrapTraits0::Unwrap(std::get<0>(state->State)))).Lock().Get()),
            TMaybeCopyHelper<TBoundArgs>::Do(Unwrap(std::get<BoundIndexes>(state->State)))...,
            std::forward<TRunArgs>(runArgs)...);
    }
};

////////////////////////////////////////////////////////////////////////////////
// #TBindState<>
////////////////////////////////////////////////////////////////////////////////
//
// This stores all the state passed into Bind() and is also where most
// of the template resolution magic occurs.
//
// TRunnable is the functor we are binding arguments to.
// TSignature is type of the Run() function that the TInvoker<> should use.
// Normally, this is the same as the TSignature of the TRunnable, but it can
// be different if an adapter like IgnoreResult() has been used.
//
// TBoundArgs contains the storage type for all the bound arguments by
// (ab)using a function type.
//

template <class TRunnable, class TSignature, class TBoundArgs>
struct TBindState;

template <bool IsMethod, class... S>
struct TBindStateIsWeakMethodHelper
    : public NMpl::TFalseType
{ };

template <bool IsMethod, class S0, class... S>
struct TBindStateIsWeakMethodHelper<IsMethod, S0, S...>
    : public TIsWeakMethodHelper<IsMethod, S0>
{ };

template <class TRunnable_, class R, class... TArgs, class... S>
struct TBindState<TRunnable_, R(TArgs...), void(S...)>
    : public TBindStateBase
{
    typedef TRunnable_ TRunnable;

    typedef TIsMethodHelper<TRunnable> IsMethod;
    typedef TBindStateIsWeakMethodHelper<IsMethod::Value, S...> IsWeakMethod;

    typedef NMpl::TSplitVariadic<sizeof...(S), NMpl::TTypesPack<>, NMpl::TTypesPack<TArgs...>> TSplitVariadicType;
    typedef typename TSplitVariadicType::THead TBoundArgsPack;
    typedef typename TSplitVariadicType::TTail TRunArgsPack;

    typedef TInvoker<TBindState, R, TBoundArgsPack, TRunArgsPack, typename NMpl::TGenerateSequence<TBoundArgsPack::Size>::TType> TInvokerType;
    typedef typename TInvokerType::TUnboundSignature TUnboundSignature;

    template<class... P>
    TBindState(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const TSourceLocation& location,
#endif
        const TRunnable& runnable,
        P&&... p)
        : TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
        )
        , Runnable(runnable)
        , State(std::forward<P>(p)...)
    { }

    virtual ~TBindState()
    { }

    TRunnable Runnable;

    typedef std::tuple<S...> TTuple;
    TTuple State;
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail
} // namespace NYT
