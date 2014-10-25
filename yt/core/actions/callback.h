#pragma once

/*
//==============================================================================
// The following code is merely an adaptation of Chromium's Binds and Callbacks.
// Kudos to Chromium authors.
//
// Original Chromium revision:
//   - git-treeish: 206a2ae8a1ebd2b040753fff7da61bbca117757f
//   - git-svn-id:  svn://svn.chromium.org/chrome/trunk/src@115607
//
// See bind.h for an extended commentary.
//==============================================================================
*/
// NOTE: Header files that do not require the full definition of #TCallback<> or
// #TClosure should include "public.h" instead of this file.

///////////////////////////////////////////////////////////////////////////////
//
// WHAT IS THIS
//
// The templated #TCallback<> class is a generalized function object.
// Together with the #Bind() function in "bind.h" they provide a type-safe
// method for performing currying of arguments and creating a "closure".
//
// In programming languages, a closure is a first-class function where all its
// parameters have been bound (usually via currying). Closures are well-suited
// for representing and passing around a unit of delayed execution.
//
//
// MEMORY MANAGEMENT AND PASSING
//
// The #TCallback<> objects themselves should be passed by const reference, and
// stored by copy. They internally store their state in a reference-counted
// class and thus do not need to be deleted.
//
// The reason to pass via a const reference is to avoid unnecessary
// Ref/Unref pairs to the internal state.
//
// However, the #TCallback<> have Ref/Unref-efficient move constructors and
// assignment operators so they also may be efficiently moved.
//
//
// EXAMPLE USAGE
//
// (see "bind_ut.cpp")
//
//
// HOW THE IMPLEMENTATION WORKS:
//
// There are three main components to the system:
//   1) The #TCallback<> classes.
//   2) The #Bind() functions.
//   3) The arguments wrappers (e.g., #Unretained() and #ConstRef()).
//
// The #TCallback<> classes represent a generic function pointer. Internally,
// it stores a reference-counted piece of state that represents the target
// function and all its bound parameters. Each #TCallback<> specialization has
// a templated constructor that takes an #TBindState<>*. In the context of
// the constructor, the static type of this #TBindState<> pointer uniquely
// identifies the function it is representing, all its bound parameters,
// and a Run() method that is capable of invoking the target.
//
// #TCallback<>'s constructor takes the #TBindState<>* that has the full static
// type and erases the target function type as well as the types of the bound
// parameters. It does this by storing a pointer to the specific Run()
// function, and upcasting the state of #TBindState<>* to a #TBindStateBase*.
// This is safe as long as this #TBindStateBase pointer is only used with
// the stored Run() pointer.
//
// To #TBindState<> objects are created inside the #Bind() functions.
// These functions, along with a set of internal templates, are responsible for:
//
//   - Unwrapping the function signature into return type, and parameters,
//   - Determining the number of parameters that are bound,
//   - Creating the #TBindState<> storing the bound parameters,
//   - Performing compile-time asserts to avoid error-prone behavior,
//   - Returning a #TCallback<> with an arity matching the number of unbound
//     parameters and that knows the correct reference counting semantics for
//     the target object if we are binding a method.
//
// The #Bind() functions do the above using type-inference, and template
// specializations.
//
// By default #Bind() will store copies of all bound parameters, and attempt
// to reference count a target object if the function being bound is
// a class method.
//
// To change this behavior, we introduce a set of argument wrappers
// (e.g., #Unretained(), and #ConstRef()). These are simple container templates
// that are passed by value, and wrap a pointer to an argument.
// See the file-level comment in "bind_helpers.h" for more information.
//
// These types are passed to #Unwrap() functions, and #TMaybeRefCountHelper()
// functions respectively to modify the behavior of #Bind(). #Unwrap()
// and #TMaybeRefCountHelper() functions change behavior by doing partial
// specialization based on whether or not a parameter is a wrapper type.
//
// #ConstRef() is similar to #tr1::cref().
// #Unretained() is specific.
//
////////////////////////////////////////////////////////////////////////////////

#include "public.h"
#include "callback_internal.h"
#include "bind_internal.h"

#include <core/misc/mpl.h>

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
#include <core/misc/source_location.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TErrorOr;

template <class T>
TPromise<T> NewPromise();

/*! \internal */
////////////////////////////////////////////////////////////////////////////////
//
// First, we forward declare the #TCallback<> class template. This informs the
// compiler that the template only has 1 type parameter which is the function
// signature that the #TCallback<> is representing.
//
// After this, create template specializations for 0-7 parameters. Note that
// even though the template type list grows, the specialization still has
// only one type: the function signature.
//
// If you are thinking of forward declaring #TCallback<> in your own header
// file, please include "public.h" instead.
//

namespace NDetail {

template <class TRunnable, class TSignature, class TBoundArgs>
struct TBindState;

// TODO(sandello): Move these somewhere closer to TFuture & TPromise.
template <class R>
struct TFutureHelper
{
    typedef TFuture<R> TFutureType;
    typedef TPromise<R> TPromiseType;
    typedef R TValueType;
    static const bool WrappedInFuture = false;
};

template <class R>
struct TFutureHelper< TFuture<R> >
{
    typedef TFuture<R> TFutureType;
    typedef TPromise<R> TPromiseType;
    typedef R TValueType;
    static const bool WrappedInFuture = true;
};

template <class R>
struct TFutureHelper< TPromise<R> >
{
    typedef TFuture<R> TFutureType;
    typedef TPromise<R> TPromiseType;
    typedef R TValueType;
    static const bool WrappedInFuture = true;
};

template <class S1, class S2>
class TCallbackRunnableAdapter;

} // namespace NDetail

template <class R, class... TArgs>
class TCallback<R(TArgs...)>
    : public NYT::NDetail::TCallbackBase
{
public:
    typedef R(TSignature)(TArgs...);

    TCallback()
        : TCallbackBase(TIntrusivePtr< NYT::NDetail::TBindStateBase >())
    { }

    TCallback(const TCallback& other)
        : TCallbackBase(other)
    { }

    TCallback(TCallback&& other)
        : TCallbackBase(std::move(other))
    { }

    template <class TRunnable, class TSignature, class TBoundArgs>
    explicit TCallback(TIntrusivePtr<
            NYT::NDetail::TBindState<TRunnable, TSignature, TBoundArgs>
        >&& bindState)
        : TCallbackBase(std::move(bindState))
    {
        // Force the assignment to a local variable of TTypedInvokeFunction
        // so the compiler will typecheck that the passed in Run() method has
        // the correct type.
        auto invokeFunction = &NYT::NDetail::TBindState<TRunnable, TSignature, TBoundArgs>::TInvokerType::Run;
        UntypedInvoke = reinterpret_cast<TUntypedInvokeFunction>(invokeFunction);
    }

    template <class R2, class... TArgs2>
    operator TCallback<R2(TArgs2...)>() const
    {
        typedef NYT::NDetail::TCallbackRunnableAdapter<R(TArgs...), R2(TArgs2...)> TRunnable;
        return TCallback<R2(TArgs2...)>(
            New<NYT::NDetail::TBindState<TRunnable,TSignature, void()>>(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
                BindState->Location,
#endif
                TRunnable(*this))
            );
    }

    using TCallbackBase::operator ==;
    using TCallbackBase::operator !=;

    TCallback& operator=(const TCallback& other)
    {
        TCallback(other).Swap(*this);
        return *this;
    }

    TCallback& operator=(TCallback&& other)
    {
        TCallback(std::move(other)).Swap(*this);
        return *this;
    }

    R Run(TArgs... args) const
    {
        auto invokeFunction = reinterpret_cast<TTypedInvokeFunction>(UntypedInvoke);
        return invokeFunction(
            BindState.Get(),
            std::forward<TArgs>(args)...);
    }


    TCallback Via(TIntrusivePtr<IInvoker> invoker);

    TCallback<typename NYT::NDetail::TFutureHelper<R>::TFutureType(TArgs...)>
    AsyncVia(TIntrusivePtr<IInvoker> invoker);

    // TODO(babenko): currently only implemented for simple return types (no TErrorOr).
    TCallback<TErrorOr<R>(TArgs...)> Guarded();

private:
    typedef R(*TTypedInvokeFunction)(
        NYT::NDetail::TBindStateBase*,
        TArgs&& ...);

};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NYT

#include "bind.h"
