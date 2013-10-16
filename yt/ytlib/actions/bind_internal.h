// WARNING: This file was auto-generated.
// Please, consider incorporating any changes into the generator.

// Generated on Wed Oct 16 13:07:52 2013.


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
#include "bind_helpers.h"
#include "bind_mpl.h"
#include "callback_internal.h"

#ifdef ENABLE_BIND_LOCATION_TRACKING
#include <ytlib/misc/source_location.h>
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
// === Runnable ===
// A typeclass that has a single Run() method and a Signature
// typedef that corresponds to the type of Run(). A Runnable can declare that
// it should treated like a method call by including a typedef named IsMethod.
// The value of this typedef is not inspected, only the existence (see "
// bind_mpl.h").
// When a Runnable declares itself a method, #Bind() will enforce special
// weak reference handling semantics for the first argument which is expected
// to be an object (an invocation target).
//
// === Functor ===
// A copyable type representing something that should be called. All function
// pointers, #TCallback<>s, Callables and Runnables are functors even if
// the invocation syntax differs.
//
// === Signature ===
// A function type (as opposed to function _pointer_ type) for a Run() function.
// Usually just a convenience typedef.
//
// === (Bound)Args ===
// A function type that is being (ab)used to store the types of set of
// arguments.
// The "return" type is always void here. We use this hack so that we do not
// need
// a new type name for each arity of type. (eg., BindState1, BindState2, ...).
// This makes forward declarations and friending much much easier.
//
//
// TYPES
//
// === TCallableAdapter ===
// Wraps Callable objects into an object that adheres to the Runnable interface.
// There are |ARITY| TCallableAdapter types.
//
// === TRunnableAdapter ===
// Wraps the various "function" pointer types into an object that adheres to the
// Runnable interface.
// There are |3 * ARITY| TRunnableAdapter types.
//
// === TSignatureTraits ===
// Type traits that unwrap a function signature into a set of easier to use
// typedefs. Used mainly for compile time asserts.
// There are |ARITY| TSignatureTraits types.
//
// === TIgnoreResultInSignature ===
// Helper class for translating function signatures to equivalent forms with
// a "void" return type.
// There are |ARITY| TIgnoreResultInSignature types.
//
// === TFunctorTraits ===
// Type traits used determine the correct Signature and TRunnableType for
// a Runnable. This is where function signature adapters are applied.
// There are |O(1)| TFunctorTraits types.
//
// === MakeRunnable ===
// Takes a Functor and returns an object in the Runnable typeclass that
// represents the underlying Functor.
// There are |O(1)| MakeRunnable types.
//
// === TInvokerHelper ===
// Take a Runnable and arguments and actully invokes it. Also handles
// the differing syntaxes needed for #TWeakPtr<> support and for ignoring
// return values. This is separate from TInvoker to avoid creating multiple
// version of #TInvoker<> which grows at |O(n^2)| with the arity.
// There are |k * ARITY| TInvokerHelper types.
//
// === TInvoker ===
// Unwraps the curried parameters and executes the Runnable.
// There are |(ARITY^2 + ARITY)/2| Invoke types.
//
// === TBindState ===
// Stores the curried parameters, and is the main entry point into the #Bind()
// system, doing most of the type resolution.
// There are |ARITY| TBindState types.
//

////////////////////////////////////////////////////////////////////////////////
// #TCallableAdapter<>
////////////////////////////////////////////////////////////////////////////////

template <class T, class Signature>
class TCallableAdapter;

// === Arity 0.

template <class R, class T>
class TCallableAdapter<T, R(T::*)()>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 0 };
    typedef R (Signature)();

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run()
    {
        return Functor();
    }

private:
    T Functor;
};

template <class R, class T>
class TCallableAdapter<T, R(T::*)() const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 0 };
    typedef R (Signature)();

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run()
    {
        return Functor();
    }

private:
    const T Functor;
};

// === Arity 1.

template <class R, class T, class A1>
class TCallableAdapter<T, R(T::*)(A1)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 1 };
    typedef R (Signature)(A1);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1)
    {
        return Functor(std::forward<A1>(a1));
    }

private:
    T Functor;
};

template <class R, class T, class A1>
class TCallableAdapter<T, R(T::*)(A1) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 1 };
    typedef R (Signature)(A1);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1)
    {
        return Functor(std::forward<A1>(a1));
    }

private:
    const T Functor;
};

// === Arity 2.

template <class R, class T, class A1, class A2>
class TCallableAdapter<T, R(T::*)(A1, A2)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 2 };
    typedef R (Signature)(A1, A2);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2));
    }

private:
    T Functor;
};

template <class R, class T, class A1, class A2>
class TCallableAdapter<T, R(T::*)(A1, A2) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 2 };
    typedef R (Signature)(A1, A2);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2));
    }

private:
    const T Functor;
};

// === Arity 3.

template <class R, class T, class A1, class A2, class A3>
class TCallableAdapter<T, R(T::*)(A1, A2, A3)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 3 };
    typedef R (Signature)(A1, A2, A3);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }

private:
    T Functor;
};

template <class R, class T, class A1, class A2, class A3>
class TCallableAdapter<T, R(T::*)(A1, A2, A3) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 3 };
    typedef R (Signature)(A1, A2, A3);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }

private:
    const T Functor;
};

// === Arity 4.

template <class R, class T, class A1, class A2, class A3, class A4>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 4 };
    typedef R (Signature)(A1, A2, A3, A4);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }

private:
    T Functor;
};

template <class R, class T, class A1, class A2, class A3, class A4>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 4 };
    typedef R (Signature)(A1, A2, A3, A4);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }

private:
    const T Functor;
};

// === Arity 5.

template <class R, class T, class A1, class A2, class A3, class A4, class A5>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4, A5)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 5 };
    typedef R (Signature)(A1, A2, A3, A4, A5);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }

private:
    T Functor;
};

template <class R, class T, class A1, class A2, class A3, class A4, class A5>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4, A5) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 5 };
    typedef R (Signature)(A1, A2, A3, A4, A5);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }

private:
    const T Functor;
};

// === Arity 6.

template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4, A5, A6)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 6 };
    typedef R (Signature)(A1, A2, A3, A4, A5, A6);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }

private:
    T Functor;
};

template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4, A5, A6) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 6 };
    typedef R (Signature)(A1, A2, A3, A4, A5, A6);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }

private:
    const T Functor;
};

// === Arity 7.

template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4, A5, A6, A7)>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 7 };
    typedef R (Signature)(A1, A2, A3, A4, A5, A6, A7);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6, A7&& a7)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }

private:
    T Functor;
};

template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
class TCallableAdapter<T, R(T::*)(A1, A2, A3, A4, A5, A6, A7) const>
{
public:
    typedef NMpl::TTrueType IsCallable;

    enum { Arity = 7 };
    typedef R (Signature)(A1, A2, A3, A4, A5, A6, A7);

    explicit TCallableAdapter(const T& functor)
        : Functor(functor)
    { }

    explicit TCallableAdapter(T&& functor)
        : Functor(std::move(functor))
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6, A7&& a7)
    {
        return Functor(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
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
// This class also exposes a Signature typedef that is the function type of the
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

// === Arity 0.

// Function Adapter
template <class R>
class TRunnableAdapter<R(*)()>
{
public:
    enum { Arity = 0 };
    typedef R (Signature)();

    explicit TRunnableAdapter(R(*function)())
        : Function(function)
    { }

    R Run()
    {
        return Function();
    }

private:
    R (*Function)();
};

// Bound Method Adapter
template <class R, class T>
class TRunnableAdapter<R(T::*)()>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 0 };
    typedef R (Signature)(T*);

    explicit TRunnableAdapter(R(T::*method)())
        : Method(method)
    { }

    R Run(T* target)
    {
        return (target->*Method)();
    }

private:
    R (T::*Method)();
};

// Const Bound Method Adapter
template <class R, class T>
class TRunnableAdapter<R(T::*)() const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 0 };
    typedef R (Signature)(const T*);

    explicit TRunnableAdapter(R(T::*method)() const)
        : Method(method)
    { }

    R Run(const T* target)
    {
        return (target->*Method)();
    }

private:
    R (T::*Method)() const;
};

// === Arity 1.

// Function Adapter
template <class R, class A1>
class TRunnableAdapter<R(*)(A1)>
{
public:
    enum { Arity = 1 };
    typedef R (Signature)(A1);

    explicit TRunnableAdapter(R(*function)(A1))
        : Function(function)
    { }

    R Run(A1&& a1)
    {
        return Function(std::forward<A1>(a1));
    }

private:
    R (*Function)(A1);
};

// Bound Method Adapter
template <class R, class T, class A1>
class TRunnableAdapter<R(T::*)(A1)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 1 };
    typedef R (Signature)(T*, A1);

    explicit TRunnableAdapter(R(T::*method)(A1))
        : Method(method)
    { }

    R Run(T* target, A1&& a1)
    {
        return (target->*Method)(std::forward<A1>(a1));
    }

private:
    R (T::*Method)(A1);
};

// Const Bound Method Adapter
template <class R, class T, class A1>
class TRunnableAdapter<R(T::*)(A1) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 1 };
    typedef R (Signature)(const T*, A1);

    explicit TRunnableAdapter(R(T::*method)(A1) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1)
    {
        return (target->*Method)(std::forward<A1>(a1));
    }

private:
    R (T::*Method)(A1) const;
};

// === Arity 2.

// Function Adapter
template <class R, class A1, class A2>
class TRunnableAdapter<R(*)(A1, A2)>
{
public:
    enum { Arity = 2 };
    typedef R (Signature)(A1, A2);

    explicit TRunnableAdapter(R(*function)(A1, A2))
        : Function(function)
    { }

    R Run(A1&& a1, A2&& a2)
    {
        return Function(std::forward<A1>(a1), std::forward<A2>(a2));
    }

private:
    R (*Function)(A1, A2);
};

// Bound Method Adapter
template <class R, class T, class A1, class A2>
class TRunnableAdapter<R(T::*)(A1, A2)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 2 };
    typedef R (Signature)(T*, A1, A2);

    explicit TRunnableAdapter(R(T::*method)(A1, A2))
        : Method(method)
    { }

    R Run(T* target, A1&& a1, A2&& a2)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2));
    }

private:
    R (T::*Method)(A1, A2);
};

// Const Bound Method Adapter
template <class R, class T, class A1, class A2>
class TRunnableAdapter<R(T::*)(A1, A2) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 2 };
    typedef R (Signature)(const T*, A1, A2);

    explicit TRunnableAdapter(R(T::*method)(A1, A2) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1, A2&& a2)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2));
    }

private:
    R (T::*Method)(A1, A2) const;
};

// === Arity 3.

// Function Adapter
template <class R, class A1, class A2, class A3>
class TRunnableAdapter<R(*)(A1, A2, A3)>
{
public:
    enum { Arity = 3 };
    typedef R (Signature)(A1, A2, A3);

    explicit TRunnableAdapter(R(*function)(A1, A2, A3))
        : Function(function)
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3)
    {
        return Function(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }

private:
    R (*Function)(A1, A2, A3);
};

// Bound Method Adapter
template <class R, class T, class A1, class A2, class A3>
class TRunnableAdapter<R(T::*)(A1, A2, A3)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 3 };
    typedef R (Signature)(T*, A1, A2, A3);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3))
        : Method(method)
    { }

    R Run(T* target, A1&& a1, A2&& a2, A3&& a3)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }

private:
    R (T::*Method)(A1, A2, A3);
};

// Const Bound Method Adapter
template <class R, class T, class A1, class A2, class A3>
class TRunnableAdapter<R(T::*)(A1, A2, A3) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 3 };
    typedef R (Signature)(const T*, A1, A2, A3);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1, A2&& a2, A3&& a3)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }

private:
    R (T::*Method)(A1, A2, A3) const;
};

// === Arity 4.

// Function Adapter
template <class R, class A1, class A2, class A3, class A4>
class TRunnableAdapter<R(*)(A1, A2, A3, A4)>
{
public:
    enum { Arity = 4 };
    typedef R (Signature)(A1, A2, A3, A4);

    explicit TRunnableAdapter(R(*function)(A1, A2, A3, A4))
        : Function(function)
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        return Function(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }

private:
    R (*Function)(A1, A2, A3, A4);
};

// Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 4 };
    typedef R (Signature)(T*, A1, A2, A3, A4);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4))
        : Method(method)
    { }

    R Run(T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }

private:
    R (T::*Method)(A1, A2, A3, A4);
};

// Const Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 4 };
    typedef R (Signature)(const T*, A1, A2, A3, A4);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }

private:
    R (T::*Method)(A1, A2, A3, A4) const;
};

// === Arity 5.

// Function Adapter
template <class R, class A1, class A2, class A3, class A4, class A5>
class TRunnableAdapter<R(*)(A1, A2, A3, A4, A5)>
{
public:
    enum { Arity = 5 };
    typedef R (Signature)(A1, A2, A3, A4, A5);

    explicit TRunnableAdapter(R(*function)(A1, A2, A3, A4, A5))
        : Function(function)
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5)
    {
        return Function(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }

private:
    R (*Function)(A1, A2, A3, A4, A5);
};

// Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4, class A5>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4, A5)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 5 };
    typedef R (Signature)(T*, A1, A2, A3, A4, A5);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4, A5))
        : Method(method)
    { }

    R Run(T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }

private:
    R (T::*Method)(A1, A2, A3, A4, A5);
};

// Const Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4, class A5>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4, A5) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 5 };
    typedef R (Signature)(const T*, A1, A2, A3, A4, A5);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4, A5) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }

private:
    R (T::*Method)(A1, A2, A3, A4, A5) const;
};

// === Arity 6.

// Function Adapter
template <class R, class A1, class A2, class A3, class A4, class A5, class A6>
class TRunnableAdapter<R(*)(A1, A2, A3, A4, A5, A6)>
{
public:
    enum { Arity = 6 };
    typedef R (Signature)(A1, A2, A3, A4, A5, A6);

    explicit TRunnableAdapter(R(*function)(A1, A2, A3, A4, A5, A6))
        : Function(function)
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6)
    {
        return Function(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }

private:
    R (*Function)(A1, A2, A3, A4, A5, A6);
};

// Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4, A5, A6)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 6 };
    typedef R (Signature)(T*, A1, A2, A3, A4, A5, A6);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4, A5, A6))
        : Method(method)
    { }

    R Run(T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }

private:
    R (T::*Method)(A1, A2, A3, A4, A5, A6);
};

// Const Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4, A5, A6) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 6 };
    typedef R (Signature)(const T*, A1, A2, A3, A4, A5, A6);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4, A5, A6) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }

private:
    R (T::*Method)(A1, A2, A3, A4, A5, A6) const;
};

// === Arity 7.

// Function Adapter
template <class R, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
class TRunnableAdapter<R(*)(A1, A2, A3, A4, A5, A6, A7)>
{
public:
    enum { Arity = 7 };
    typedef R (Signature)(A1, A2, A3, A4, A5, A6, A7);

    explicit TRunnableAdapter(R(*function)(A1, A2, A3, A4, A5, A6, A7))
        : Function(function)
    { }

    R Run(A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6, A7&& a7)
    {
        return Function(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }

private:
    R (*Function)(A1, A2, A3, A4, A5, A6, A7);
};

// Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4, A5, A6, A7)>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 7 };
    typedef R (Signature)(T*, A1, A2, A3, A4, A5, A6, A7);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4, A5, A6, A7))
        : Method(method)
    { }

    R Run(T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5, A6&& a6,
        A7&& a7)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }

private:
    R (T::*Method)(A1, A2, A3, A4, A5, A6, A7);
};

// Const Bound Method Adapter
template <class R, class T, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
class TRunnableAdapter<R(T::*)(A1, A2, A3, A4, A5, A6, A7) const>
{
public:
    typedef NMpl::TTrueType IsMethod;

    enum { Arity = 1 + 7 };
    typedef R (Signature)(const T*, A1, A2, A3, A4, A5, A6, A7);

    explicit TRunnableAdapter(R(T::*method)(A1, A2, A3, A4, A5, A6, A7) const)
        : Method(method)
    { }

    R Run(const T* target, A1&& a1, A2&& a2, A3&& a3, A4&& a4, A5&& a5,
        A6&& a6, A7&& a7)
    {
        return (target->*Method)(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }

private:
    R (T::*Method)(A1, A2, A3, A4, A5, A6, A7) const;
};

////////////////////////////////////////////////////////////////////////////////
// #TSignatureTraits<>s
////////////////////////////////////////////////////////////////////////////////

template <class Signature>
struct TSignatureTraits;

// === Arity 0.
template <class R>
struct TSignatureTraits<R()>
{
    typedef R ReturnType;
};

// === Arity 1.
template <class R, class A1>
struct TSignatureTraits<R(A1)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
};

// === Arity 2.
template <class R, class A1, class A2>
struct TSignatureTraits<R(A1, A2)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
    typedef A2 A2Type;
};

// === Arity 3.
template <class R, class A1, class A2, class A3>
struct TSignatureTraits<R(A1, A2, A3)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
    typedef A2 A2Type;
    typedef A3 A3Type;
};

// === Arity 4.
template <class R, class A1, class A2, class A3, class A4>
struct TSignatureTraits<R(A1, A2, A3, A4)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
    typedef A2 A2Type;
    typedef A3 A3Type;
    typedef A4 A4Type;
};

// === Arity 5.
template <class R, class A1, class A2, class A3, class A4, class A5>
struct TSignatureTraits<R(A1, A2, A3, A4, A5)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
    typedef A2 A2Type;
    typedef A3 A3Type;
    typedef A4 A4Type;
    typedef A5 A5Type;
};

// === Arity 6.
template <class R, class A1, class A2, class A3, class A4, class A5, class A6>
struct TSignatureTraits<R(A1, A2, A3, A4, A5, A6)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
    typedef A2 A2Type;
    typedef A3 A3Type;
    typedef A4 A4Type;
    typedef A5 A5Type;
    typedef A6 A6Type;
};

// === Arity 7.
template <class R, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
struct TSignatureTraits<R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R ReturnType;
    typedef A1 A1Type;
    typedef A2 A2Type;
    typedef A3 A3Type;
    typedef A4 A4Type;
    typedef A5 A5Type;
    typedef A6 A6Type;
    typedef A7 A7Type;
};

////////////////////////////////////////////////////////////////////////////////
// #TIgnoreResultInSignature<>
////////////////////////////////////////////////////////////////////////////////

template <class Signature>
struct TIgnoreResultInSignature;

// === Arity 0.
template <class R>
struct TIgnoreResultInSignature<R()>
{
    typedef void(Signature)();
};

// === Arity 1.
template <class R, class A1>
struct TIgnoreResultInSignature<R(A1)>
{
    typedef void(Signature)(A1);
};

// === Arity 2.
template <class R, class A1, class A2>
struct TIgnoreResultInSignature<R(A1, A2)>
{
    typedef void(Signature)(A1, A2);
};

// === Arity 3.
template <class R, class A1, class A2, class A3>
struct TIgnoreResultInSignature<R(A1, A2, A3)>
{
    typedef void(Signature)(A1, A2, A3);
};

// === Arity 4.
template <class R, class A1, class A2, class A3, class A4>
struct TIgnoreResultInSignature<R(A1, A2, A3, A4)>
{
    typedef void(Signature)(A1, A2, A3, A4);
};

// === Arity 5.
template <class R, class A1, class A2, class A3, class A4, class A5>
struct TIgnoreResultInSignature<R(A1, A2, A3, A4, A5)>
{
    typedef void(Signature)(A1, A2, A3, A4, A5);
};

// === Arity 6.
template <class R, class A1, class A2, class A3, class A4, class A5, class A6>
struct TIgnoreResultInSignature<R(A1, A2, A3, A4, A5, A6)>
{
    typedef void(Signature)(A1, A2, A3, A4, A5, A6);
};

// === Arity 7.
template <class R, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
struct TIgnoreResultInSignature<R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef void(Signature)(A1, A2, A3, A4, A5, A6, A7);
};

////////////////////////////////////////////////////////////////////////////////
// #TFunctorTraits<>
////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFunctorTraits
{
    typedef TRunnableAdapter<T> TRunnableType;
    typedef typename TRunnableType::Signature Signature;
};

template <class T>
struct TFunctorTraits< TIgnoreResultWrapper<T> >
{
    typedef typename TFunctorTraits<T>::TRunnableType TRunnableType;
    typedef typename TIgnoreResultInSignature<
        typename TRunnableType::Signature
    >::Signature Signature;
};

template <class T>
struct TFunctorTraits< TCallback<T> >
{
    typedef TCallback<T> TRunnableType;
    typedef typename TCallback<T>::Signature Signature;
};

////////////////////////////////////////////////////////////////////////////////
// #MakeRunnable()
////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TFunctorTraits<T>::TRunnableType
MakeRunnable(const T& x)
{
    return TRunnableAdapter<T>(x);
}

template <class T>
typename TFunctorTraits<T>::TRunnableType
MakeRunnable(const TIgnoreResultWrapper<T>& wrapper)
{
    return MakeRunnable(wrapper.Functor);
}

template <class T>
const typename TFunctorTraits< TCallback<T> >::TRunnableType&
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
// IgnoreResult().  Normally, if the Runnable's Signature had a void return,
// the template system would just accept "return functor.Run()" ignoring
// the fact that a void function is being used with return. This piece of
// sugar breaks though when the Runnable's Signature is not void.  Thus, we
// need a partial specialization to change the syntax to drop the "return"
// from the invocation call.
//
// WeakCalls similarly need special syntax that is applied to the first
// argument to check if they should no-op themselves.
//

template <bool IsWeakMethod, class Runnable, class ReturnType, class Args>
struct TInvokerHelper;

// === Arity 0.

template <class Runnable, class R>
struct TInvokerHelper<false, Runnable, R,
    void()>
{
    static inline R Run(Runnable runnable)
    {
        return runnable.Run();
    }
};

template <class Runnable>
struct TInvokerHelper<false, Runnable, void,
    void()>
{
    static inline void Run(Runnable runnable)
    {
        runnable.Run();
    }
};

// === Arity 1.

template <class Runnable, class R, class A1>
struct TInvokerHelper<false, Runnable, R,
    void(A1)>
{
    static inline R Run(Runnable runnable, A1&& a1)
    {
        return runnable.Run(std::forward<A1>(a1));
    }
};

template <class Runnable, class A1>
struct TInvokerHelper<false, Runnable, void,
    void(A1)>
{
    static inline void Run(Runnable runnable, A1&& a1)
    {
        runnable.Run(std::forward<A1>(a1));
    }
};

template <class Runnable, class A1>
struct TInvokerHelper<true, Runnable, void,
    void(A1)>
{
    static inline void Run(Runnable runnable, A1&& a1)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1));
    }
};

// === Arity 2.

template <class Runnable, class R, class A1, class A2>
struct TInvokerHelper<false, Runnable, R,
    void(A1, A2)>
{
    static inline R Run(Runnable runnable, A1&& a1, A2&& a2)
    {
        return runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2));
    }
};

template <class Runnable, class A1, class A2>
struct TInvokerHelper<false, Runnable, void,
    void(A1, A2)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2)
    {
        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2));
    }
};

template <class Runnable, class A1, class A2>
struct TInvokerHelper<true, Runnable, void,
    void(A1, A2)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2));
    }
};

// === Arity 3.

template <class Runnable, class R, class A1, class A2, class A3>
struct TInvokerHelper<false, Runnable, R,
    void(A1, A2, A3)>
{
    static inline R Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3)
    {
        return runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }
};

template <class Runnable, class A1, class A2, class A3>
struct TInvokerHelper<false, Runnable, void,
    void(A1, A2, A3)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3)
    {
        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }
};

template <class Runnable, class A1, class A2, class A3>
struct TInvokerHelper<true, Runnable, void,
    void(A1, A2, A3)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }
};

// === Arity 4.

template <class Runnable, class R, class A1, class A2, class A3, class A4>
struct TInvokerHelper<false, Runnable, R,
    void(A1, A2, A3, A4)>
{
    static inline R Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        return runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4>
struct TInvokerHelper<false, Runnable, void,
    void(A1, A2, A3, A4)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4)
    {
        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4>
struct TInvokerHelper<true, Runnable, void,
    void(A1, A2, A3, A4)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }
};

// === Arity 5.

template <class Runnable, class R, class A1, class A2, class A3, class A4,
    class A5>
struct TInvokerHelper<false, Runnable, R,
    void(A1, A2, A3, A4, A5)>
{
    static inline R Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3, A4&& a4,
        A5&& a5)
    {
        return runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4, class A5>
struct TInvokerHelper<false, Runnable, void,
    void(A1, A2, A3, A4, A5)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4, A5&& a5)
    {
        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4, class A5>
struct TInvokerHelper<true, Runnable, void,
    void(A1, A2, A3, A4, A5)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4, A5&& a5)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }
};

// === Arity 6.

template <class Runnable, class R, class A1, class A2, class A3, class A4,
    class A5, class A6>
struct TInvokerHelper<false, Runnable, R,
    void(A1, A2, A3, A4, A5, A6)>
{
    static inline R Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3, A4&& a4,
        A5&& a5, A6&& a6)
    {
        return runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4, class A5,
    class A6>
struct TInvokerHelper<false, Runnable, void,
    void(A1, A2, A3, A4, A5, A6)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4, A5&& a5, A6&& a6)
    {
        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4, class A5,
    class A6>
struct TInvokerHelper<true, Runnable, void,
    void(A1, A2, A3, A4, A5, A6)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4, A5&& a5, A6&& a6)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }
};

// === Arity 7.

template <class Runnable, class R, class A1, class A2, class A3, class A4,
    class A5, class A6, class A7>
struct TInvokerHelper<false, Runnable, R,
    void(A1, A2, A3, A4, A5, A6, A7)>
{
    static inline R Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3, A4&& a4,
        A5&& a5, A6&& a6, A7&& a7)
    {
        return runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
struct TInvokerHelper<false, Runnable, void,
    void(A1, A2, A3, A4, A5, A6, A7)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4, A5&& a5, A6&& a6, A7&& a7)
    {
        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

template <class Runnable, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
struct TInvokerHelper<true, Runnable, void,
    void(A1, A2, A3, A4, A5, A6, A7)>
{
    static inline void Run(Runnable runnable, A1&& a1, A2&& a2, A3&& a3,
        A4&& a4, A5&& a5, A6&& a6, A7&& a7)
    {
        if (!a1) {
            return;
        }

        runnable.Run(std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

template <class Runnable, class R, class Args>
struct TInvokerHelper<true, Runnable, R, Args>
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

template <int BoundArguments, class TTypedBindState, class Signature>
struct TInvoker;

// === Arity 0 -> 0 unbound.
template <class TTypedBindState, class R>
struct TInvoker<0, TTypedBindState, R()>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void()
        >::Run(state->Runnable_);
    }
};

// === Arity 1 -> 1 unbound.
template <class TTypedBindState, class R, class A1>
struct TInvoker<0, TTypedBindState, R(A1)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&);
    typedef R(UnboundSignature)(A1);

    static R Run(TBindStateBase* stateBase, A1&& a1)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1)
        >::Run(state->Runnable_, std::forward<A1>(a1));
    }
};

// === Arity 1 -> 0 unbound.
template <class TTypedBindState, class R, class A1>
struct TInvoker<1, TTypedBindState, R(A1)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()));
    }
};

// === Arity 2 -> 2 unbound.
template <class TTypedBindState, class R, class A1, class A2>
struct TInvoker<0, TTypedBindState, R(A1, A2)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&, A2&&);
    typedef R(UnboundSignature)(A1, A2);

    static R Run(TBindStateBase* stateBase, A1&& a1, A2&& a2)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2)
        >::Run(state->Runnable_, std::forward<A1>(a1), std::forward<A2>(a2));
    }
};

// === Arity 2 -> 1 unbound.
template <class TTypedBindState, class R, class A1, class A2>
struct TInvoker<1, TTypedBindState, R(A1, A2)>
{
    typedef R(RunSignature)(TBindStateBase*, A2&&);
    typedef R(UnboundSignature)(A2);

    static R Run(TBindStateBase* stateBase, A2&& a2)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            std::forward<A2>(a2));
    }
};

// === Arity 2 -> 0 unbound.
template <class TTypedBindState, class R, class A1, class A2>
struct TInvoker<2, TTypedBindState, R(A1, A2)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);

        static_assert(!TTypedBindState::IsMethod::Value || (2 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)));
    }
};

// === Arity 3 -> 3 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3>
struct TInvoker<0, TTypedBindState, R(A1, A2, A3)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&, A2&&, A3&&);
    typedef R(UnboundSignature)(A1, A2, A3);

    static R Run(TBindStateBase* stateBase, A1&& a1, A2&& a2, A3&& a3)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3)
        >::Run(state->Runnable_, std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3));
    }
};

// === Arity 3 -> 2 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3>
struct TInvoker<1, TTypedBindState, R(A1, A2, A3)>
{
    typedef R(RunSignature)(TBindStateBase*, A2&&, A3&&);
    typedef R(UnboundSignature)(A2, A3);

    static R Run(TBindStateBase* stateBase, A2&& a2, A3&& a3)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            std::forward<A2>(a2), std::forward<A3>(a3));
    }
};

// === Arity 3 -> 1 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3>
struct TInvoker<2, TTypedBindState, R(A1, A2, A3)>
{
    typedef R(RunSignature)(TBindStateBase*, A3&&);
    typedef R(UnboundSignature)(A3);

    static R Run(TBindStateBase* stateBase, A3&& a3)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);

        static_assert(!TTypedBindState::IsMethod::Value || (2 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            std::forward<A3>(a3));
    }
};

// === Arity 3 -> 0 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3>
struct TInvoker<3, TTypedBindState, R(A1, A2, A3)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);

        static_assert(!TTypedBindState::IsMethod::Value || (3 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)));
    }
};

// === Arity 4 -> 4 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4>
struct TInvoker<0, TTypedBindState, R(A1, A2, A3, A4)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&, A2&&, A3&&, A4&&);
    typedef R(UnboundSignature)(A1, A2, A3, A4);

    static R Run(TBindStateBase* stateBase, A1&& a1, A2&& a2, A3&& a3, A4&& a4)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4)
        >::Run(state->Runnable_, std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }
};

// === Arity 4 -> 3 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4>
struct TInvoker<1, TTypedBindState, R(A1, A2, A3, A4)>
{
    typedef R(RunSignature)(TBindStateBase*, A2&&, A3&&, A4&&);
    typedef R(UnboundSignature)(A2, A3, A4);

    static R Run(TBindStateBase* stateBase, A2&& a2, A3&& a3, A4&& a4)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            std::forward<A2>(a2), std::forward<A3>(a3), std::forward<A4>(a4));
    }
};

// === Arity 4 -> 2 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4>
struct TInvoker<2, TTypedBindState, R(A1, A2, A3, A4)>
{
    typedef R(RunSignature)(TBindStateBase*, A3&&, A4&&);
    typedef R(UnboundSignature)(A3, A4);

    static R Run(TBindStateBase* stateBase, A3&& a3, A4&& a4)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);

        static_assert(!TTypedBindState::IsMethod::Value || (2 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            std::forward<A3>(a3), std::forward<A4>(a4));
    }
};

// === Arity 4 -> 1 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4>
struct TInvoker<3, TTypedBindState, R(A1, A2, A3, A4)>
{
    typedef R(RunSignature)(TBindStateBase*, A4&&);
    typedef R(UnboundSignature)(A4);

    static R Run(TBindStateBase* stateBase, A4&& a4)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);

        static_assert(!TTypedBindState::IsMethod::Value || (3 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            std::forward<A4>(a4));
    }
};

// === Arity 4 -> 0 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4>
struct TInvoker<4, TTypedBindState, R(A1, A2, A3, A4)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);

        static_assert(!TTypedBindState::IsMethod::Value || (4 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)));
    }
};

// === Arity 5 -> 5 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5>
struct TInvoker<0, TTypedBindState, R(A1, A2, A3, A4, A5)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&, A2&&, A3&&, A4&&, A5&&);
    typedef R(UnboundSignature)(A1, A2, A3, A4, A5);

    static R Run(TBindStateBase* stateBase, A1&& a1, A2&& a2, A3&& a3, A4&& a4,
        A5&& a5)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5)
        >::Run(state->Runnable_, std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }
};

// === Arity 5 -> 4 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5>
struct TInvoker<1, TTypedBindState, R(A1, A2, A3, A4, A5)>
{
    typedef R(RunSignature)(TBindStateBase*, A2&&, A3&&, A4&&, A5&&);
    typedef R(UnboundSignature)(A2, A3, A4, A5);

    static R Run(TBindStateBase* stateBase, A2&& a2, A3&& a3, A4&& a4, A5&& a5)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            std::forward<A2>(a2), std::forward<A3>(a3), std::forward<A4>(a4),
            std::forward<A5>(a5));
    }
};

// === Arity 5 -> 3 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5>
struct TInvoker<2, TTypedBindState, R(A1, A2, A3, A4, A5)>
{
    typedef R(RunSignature)(TBindStateBase*, A3&&, A4&&, A5&&);
    typedef R(UnboundSignature)(A3, A4, A5);

    static R Run(TBindStateBase* stateBase, A3&& a3, A4&& a4, A5&& a5)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);

        static_assert(!TTypedBindState::IsMethod::Value || (2 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5));
    }
};

// === Arity 5 -> 2 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5>
struct TInvoker<3, TTypedBindState, R(A1, A2, A3, A4, A5)>
{
    typedef R(RunSignature)(TBindStateBase*, A4&&, A5&&);
    typedef R(UnboundSignature)(A4, A5);

    static R Run(TBindStateBase* stateBase, A4&& a4, A5&& a5)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);

        static_assert(!TTypedBindState::IsMethod::Value || (3 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            std::forward<A4>(a4), std::forward<A5>(a5));
    }
};

// === Arity 5 -> 1 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5>
struct TInvoker<4, TTypedBindState, R(A1, A2, A3, A4, A5)>
{
    typedef R(RunSignature)(TBindStateBase*, A5&&);
    typedef R(UnboundSignature)(A5);

    static R Run(TBindStateBase* stateBase, A5&& a5)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);

        static_assert(!TTypedBindState::IsMethod::Value || (4 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            std::forward<A5>(a5));
    }
};

// === Arity 5 -> 0 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5>
struct TInvoker<5, TTypedBindState, R(A1, A2, A3, A4, A5)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TTypedBindState::TBound5UnwrapTraits
            TBound5UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;
        typedef typename TBound5UnwrapTraits::TType BoundA5;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);
        BoundA5 a5 = TBound5UnwrapTraits::Unwrap(state->S5_);

        static_assert(!TTypedBindState::IsMethod::Value || (5 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            TMaybeCopyHelper<A5>::Do(std::forward<BoundA5>(a5)));
    }
};

// === Arity 6 -> 6 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<0, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&, A2&&, A3&&, A4&&, A5&&,
        A6&&);
    typedef R(UnboundSignature)(A1, A2, A3, A4, A5, A6);

    static R Run(TBindStateBase* stateBase, A1&& a1, A2&& a2, A3&& a3, A4&& a4,
        A5&& a5, A6&& a6)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_, std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }
};

// === Arity 6 -> 5 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<1, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*, A2&&, A3&&, A4&&, A5&&, A6&&);
    typedef R(UnboundSignature)(A2, A3, A4, A5, A6);

    static R Run(TBindStateBase* stateBase, A2&& a2, A3&& a3, A4&& a4, A5&& a5,
        A6&& a6)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            std::forward<A2>(a2), std::forward<A3>(a3), std::forward<A4>(a4),
            std::forward<A5>(a5), std::forward<A6>(a6));
    }
};

// === Arity 6 -> 4 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<2, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*, A3&&, A4&&, A5&&, A6&&);
    typedef R(UnboundSignature)(A3, A4, A5, A6);

    static R Run(TBindStateBase* stateBase, A3&& a3, A4&& a4, A5&& a5, A6&& a6)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);

        static_assert(!TTypedBindState::IsMethod::Value || (2 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6));
    }
};

// === Arity 6 -> 3 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<3, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*, A4&&, A5&&, A6&&);
    typedef R(UnboundSignature)(A4, A5, A6);

    static R Run(TBindStateBase* stateBase, A4&& a4, A5&& a5, A6&& a6)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);

        static_assert(!TTypedBindState::IsMethod::Value || (3 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            std::forward<A4>(a4), std::forward<A5>(a5), std::forward<A6>(a6));
    }
};

// === Arity 6 -> 2 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<4, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*, A5&&, A6&&);
    typedef R(UnboundSignature)(A5, A6);

    static R Run(TBindStateBase* stateBase, A5&& a5, A6&& a6)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);

        static_assert(!TTypedBindState::IsMethod::Value || (4 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            std::forward<A5>(a5), std::forward<A6>(a6));
    }
};

// === Arity 6 -> 1 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<5, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*, A6&&);
    typedef R(UnboundSignature)(A6);

    static R Run(TBindStateBase* stateBase, A6&& a6)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TTypedBindState::TBound5UnwrapTraits
            TBound5UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;
        typedef typename TBound5UnwrapTraits::TType BoundA5;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);
        BoundA5 a5 = TBound5UnwrapTraits::Unwrap(state->S5_);

        static_assert(!TTypedBindState::IsMethod::Value || (5 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            TMaybeCopyHelper<A5>::Do(std::forward<BoundA5>(a5)),
            std::forward<A6>(a6));
    }
};

// === Arity 6 -> 0 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6>
struct TInvoker<6, TTypedBindState, R(A1, A2, A3, A4, A5, A6)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TTypedBindState::TBound5UnwrapTraits
            TBound5UnwrapTraits;
        typedef typename TTypedBindState::TBound6UnwrapTraits
            TBound6UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;
        typedef typename TBound5UnwrapTraits::TType BoundA5;
        typedef typename TBound6UnwrapTraits::TType BoundA6;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);
        BoundA5 a5 = TBound5UnwrapTraits::Unwrap(state->S5_);
        BoundA6 a6 = TBound6UnwrapTraits::Unwrap(state->S6_);

        static_assert(!TTypedBindState::IsMethod::Value || (6 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            TMaybeCopyHelper<A5>::Do(std::forward<BoundA5>(a5)),
            TMaybeCopyHelper<A6>::Do(std::forward<BoundA6>(a6)));
    }
};

// === Arity 7 -> 7 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<0, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A1&&, A2&&, A3&&, A4&&, A5&&,
        A6&&, A7&&);
    typedef R(UnboundSignature)(A1, A2, A3, A4, A5, A6, A7);

    static R Run(TBindStateBase* stateBase, A1&& a1, A2&& a2, A3&& a3, A4&& a4,
        A5&& a5, A6&& a6, A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.


        static_assert(!TTypedBindState::IsMethod::Value || (0 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).

        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_, std::forward<A1>(a1), std::forward<A2>(a2),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

// === Arity 7 -> 6 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<1, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A2&&, A3&&, A4&&, A5&&, A6&&,
        A7&&);
    typedef R(UnboundSignature)(A2, A3, A4, A5, A6, A7);

    static R Run(TBindStateBase* stateBase, A2&& a2, A3&& a3, A4&& a4, A5&& a5,
        A6&& a6, A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);

        static_assert(!TTypedBindState::IsMethod::Value || (1 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            std::forward<A2>(a2), std::forward<A3>(a3), std::forward<A4>(a4),
            std::forward<A5>(a5), std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

// === Arity 7 -> 5 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<2, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A3&&, A4&&, A5&&, A6&&, A7&&);
    typedef R(UnboundSignature)(A3, A4, A5, A6, A7);

    static R Run(TBindStateBase* stateBase, A3&& a3, A4&& a4, A5&& a5, A6&& a6,
        A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);

        static_assert(!TTypedBindState::IsMethod::Value || (2 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            std::forward<A3>(a3), std::forward<A4>(a4), std::forward<A5>(a5),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

// === Arity 7 -> 4 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<3, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A4&&, A5&&, A6&&, A7&&);
    typedef R(UnboundSignature)(A4, A5, A6, A7);

    static R Run(TBindStateBase* stateBase, A4&& a4, A5&& a5, A6&& a6, A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);

        static_assert(!TTypedBindState::IsMethod::Value || (3 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            std::forward<A4>(a4), std::forward<A5>(a5), std::forward<A6>(a6),
            std::forward<A7>(a7));
    }
};

// === Arity 7 -> 3 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<4, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A5&&, A6&&, A7&&);
    typedef R(UnboundSignature)(A5, A6, A7);

    static R Run(TBindStateBase* stateBase, A5&& a5, A6&& a6, A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);

        static_assert(!TTypedBindState::IsMethod::Value || (4 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            std::forward<A5>(a5), std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

// === Arity 7 -> 2 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<5, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A6&&, A7&&);
    typedef R(UnboundSignature)(A6, A7);

    static R Run(TBindStateBase* stateBase, A6&& a6, A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TTypedBindState::TBound5UnwrapTraits
            TBound5UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;
        typedef typename TBound5UnwrapTraits::TType BoundA5;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);
        BoundA5 a5 = TBound5UnwrapTraits::Unwrap(state->S5_);

        static_assert(!TTypedBindState::IsMethod::Value || (5 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            TMaybeCopyHelper<A5>::Do(std::forward<BoundA5>(a5)),
            std::forward<A6>(a6), std::forward<A7>(a7));
    }
};

// === Arity 7 -> 1 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<6, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*, A7&&);
    typedef R(UnboundSignature)(A7);

    static R Run(TBindStateBase* stateBase, A7&& a7)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TTypedBindState::TBound5UnwrapTraits
            TBound5UnwrapTraits;
        typedef typename TTypedBindState::TBound6UnwrapTraits
            TBound6UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;
        typedef typename TBound5UnwrapTraits::TType BoundA5;
        typedef typename TBound6UnwrapTraits::TType BoundA6;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);
        BoundA5 a5 = TBound5UnwrapTraits::Unwrap(state->S5_);
        BoundA6 a6 = TBound6UnwrapTraits::Unwrap(state->S6_);

        static_assert(!TTypedBindState::IsMethod::Value || (6 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            TMaybeCopyHelper<A5>::Do(std::forward<BoundA5>(a5)),
            TMaybeCopyHelper<A6>::Do(std::forward<BoundA6>(a6)),
            std::forward<A7>(a7));
    }
};

// === Arity 7 -> 0 unbound.
template <class TTypedBindState, class R, class A1, class A2, class A3,
    class A4, class A5, class A6, class A7>
struct TInvoker<7, TTypedBindState, R(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef R(RunSignature)(TBindStateBase*);
    typedef R(UnboundSignature)();

    static R Run(TBindStateBase* stateBase)
    {
        TTypedBindState* state = static_cast<TTypedBindState*>(stateBase);

        // Local references to make debugger stepping easier.
        // If in a debugger you really want to warp ahead and step through the
        // #TInvokerHelper<>::Run() call below.
        typedef typename TTypedBindState::TBound1UnwrapTraits
            TBound1UnwrapTraits;
        typedef typename TTypedBindState::TBound2UnwrapTraits
            TBound2UnwrapTraits;
        typedef typename TTypedBindState::TBound3UnwrapTraits
            TBound3UnwrapTraits;
        typedef typename TTypedBindState::TBound4UnwrapTraits
            TBound4UnwrapTraits;
        typedef typename TTypedBindState::TBound5UnwrapTraits
            TBound5UnwrapTraits;
        typedef typename TTypedBindState::TBound6UnwrapTraits
            TBound6UnwrapTraits;
        typedef typename TTypedBindState::TBound7UnwrapTraits
            TBound7UnwrapTraits;
        typedef typename TBound1UnwrapTraits::TType BoundA1;
        typedef typename TBound2UnwrapTraits::TType BoundA2;
        typedef typename TBound3UnwrapTraits::TType BoundA3;
        typedef typename TBound4UnwrapTraits::TType BoundA4;
        typedef typename TBound5UnwrapTraits::TType BoundA5;
        typedef typename TBound6UnwrapTraits::TType BoundA6;
        typedef typename TBound7UnwrapTraits::TType BoundA7;

        BoundA1 a1 = TBound1UnwrapTraits::Unwrap(state->S1_);
        BoundA2 a2 = TBound2UnwrapTraits::Unwrap(state->S2_);
        BoundA3 a3 = TBound3UnwrapTraits::Unwrap(state->S3_);
        BoundA4 a4 = TBound4UnwrapTraits::Unwrap(state->S4_);
        BoundA5 a5 = TBound5UnwrapTraits::Unwrap(state->S5_);
        BoundA6 a6 = TBound6UnwrapTraits::Unwrap(state->S6_);
        BoundA7 a7 = TBound7UnwrapTraits::Unwrap(state->S7_);

        static_assert(!TTypedBindState::IsMethod::Value || (7 > 0),
            "The target object for a bound method have to be bound.");

        // If someone would like to change this, please expand target locking
        // and extraction logic to the unbound arguments (specifically,
        // change the code generation logic in the pump file).
        typedef TMaybeCopyHelper<A1>
            TTargetCopy;
        typedef TMaybeLockHelper<TTypedBindState::IsMethod::Value, BoundA1>
            TTargetLock;
        return TInvokerHelper<
            TTypedBindState::IsWeakMethod::Value,
            typename TTypedBindState::TRunnableType,
            R,
            void(A1, A2, A3, A4, A5, A6, A7)
        >::Run(state->Runnable_,
            TTargetCopy::Do(TTargetLock(std::forward<BoundA1>(a1)).Lock().Get()),
            TMaybeCopyHelper<A2>::Do(std::forward<BoundA2>(a2)),
            TMaybeCopyHelper<A3>::Do(std::forward<BoundA3>(a3)),
            TMaybeCopyHelper<A4>::Do(std::forward<BoundA4>(a4)),
            TMaybeCopyHelper<A5>::Do(std::forward<BoundA5>(a5)),
            TMaybeCopyHelper<A6>::Do(std::forward<BoundA6>(a6)),
            TMaybeCopyHelper<A7>::Do(std::forward<BoundA7>(a7)));
    }
};

////////////////////////////////////////////////////////////////////////////////
// #TBindState<>
////////////////////////////////////////////////////////////////////////////////
//
// This stores all the state passed into Bind() and is also where most
// of the template resolution magic occurs.
//
// Runnable is the functor we are binding arguments to.
// Signature is type of the Run() function that the TInvoker<> should use.
// Normally, this is the same as the Signature of the Runnable, but it can
// be different if an adapter like IgnoreResult() has been used.
//
// BoundArgs contains the storage type for all the bound arguments by
// (ab)using a function type.
//

template <class Runnable, class Signature, class BoundArgs>
class TBindState;

template <class Runnable, class Signature>
class TBindState<Runnable, Signature, void()>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef NMpl::TFalseType IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<0, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
};

template <class Runnable, class Signature, class S1>
class TBindState<Runnable, Signature, void(S1)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<1, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;

    template<class P1>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
};

template <class Runnable, class Signature, class S1, class S2>
class TBindState<Runnable, Signature, void(S1, S2)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<2, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;
    typedef TUnwrapTraits<S2> TBound2UnwrapTraits;

    template<class P1, class P2>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1, P2&& p2)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
        , S2_(std::forward<P2>(p2))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
    S2 S2_;
};

template <class Runnable, class Signature, class S1, class S2, class S3>
class TBindState<Runnable, Signature, void(S1, S2, S3)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<3, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;
    typedef TUnwrapTraits<S2> TBound2UnwrapTraits;
    typedef TUnwrapTraits<S3> TBound3UnwrapTraits;

    template<class P1, class P2, class P3>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1, P2&& p2, P3&& p3)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
        , S2_(std::forward<P2>(p2))
        , S3_(std::forward<P3>(p3))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
    S2 S2_;
    S3 S3_;
};

template <class Runnable, class Signature, class S1, class S2, class S3,
    class S4>
class TBindState<Runnable, Signature, void(S1, S2, S3, S4)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<4, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;
    typedef TUnwrapTraits<S2> TBound2UnwrapTraits;
    typedef TUnwrapTraits<S3> TBound3UnwrapTraits;
    typedef TUnwrapTraits<S4> TBound4UnwrapTraits;

    template<class P1, class P2, class P3, class P4>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1, P2&& p2, P3&& p3, P4&& p4)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
        , S2_(std::forward<P2>(p2))
        , S3_(std::forward<P3>(p3))
        , S4_(std::forward<P4>(p4))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
    S2 S2_;
    S3 S3_;
    S4 S4_;
};

template <class Runnable, class Signature, class S1, class S2, class S3,
    class S4, class S5>
class TBindState<Runnable, Signature, void(S1, S2, S3, S4, S5)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<5, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;
    typedef TUnwrapTraits<S2> TBound2UnwrapTraits;
    typedef TUnwrapTraits<S3> TBound3UnwrapTraits;
    typedef TUnwrapTraits<S4> TBound4UnwrapTraits;
    typedef TUnwrapTraits<S5> TBound5UnwrapTraits;

    template<class P1, class P2, class P3, class P4, class P5>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
        , S2_(std::forward<P2>(p2))
        , S3_(std::forward<P3>(p3))
        , S4_(std::forward<P4>(p4))
        , S5_(std::forward<P5>(p5))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
    S2 S2_;
    S3 S3_;
    S4 S4_;
    S5 S5_;
};

template <class Runnable, class Signature, class S1, class S2, class S3,
    class S4, class S5, class S6>
class TBindState<Runnable, Signature, void(S1, S2, S3, S4, S5, S6)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<6, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;
    typedef TUnwrapTraits<S2> TBound2UnwrapTraits;
    typedef TUnwrapTraits<S3> TBound3UnwrapTraits;
    typedef TUnwrapTraits<S4> TBound4UnwrapTraits;
    typedef TUnwrapTraits<S5> TBound5UnwrapTraits;
    typedef TUnwrapTraits<S6> TBound6UnwrapTraits;

    template<class P1, class P2, class P3, class P4, class P5, class P6>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5,
            P6&& p6)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
        , S2_(std::forward<P2>(p2))
        , S3_(std::forward<P3>(p3))
        , S4_(std::forward<P4>(p4))
        , S5_(std::forward<P5>(p5))
        , S6_(std::forward<P6>(p6))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
    S2 S2_;
    S3 S3_;
    S4 S4_;
    S5 S5_;
    S6 S6_;
};

template <class Runnable, class Signature, class S1, class S2, class S3,
    class S4, class S5, class S6, class S7>
class TBindState<Runnable, Signature, void(S1, S2, S3, S4, S5, S6, S7)>
    : public TBindStateBase
{
public:
    typedef TIsMethodHelper<Runnable> IsMethod;
    typedef TIsWeakMethodHelper<IsMethod::Value, S1> IsWeakMethod;

    typedef Runnable TRunnableType;
    typedef TInvoker<7, TBindState, Signature> TInvokerType;
    typedef typename TInvokerType::UnboundSignature UnboundSignature;

    // Convenience typedefs for bound argument types.
    typedef TUnwrapTraits<S1> TBound1UnwrapTraits;
    typedef TUnwrapTraits<S2> TBound2UnwrapTraits;
    typedef TUnwrapTraits<S3> TBound3UnwrapTraits;
    typedef TUnwrapTraits<S4> TBound4UnwrapTraits;
    typedef TUnwrapTraits<S5> TBound5UnwrapTraits;
    typedef TUnwrapTraits<S6> TBound6UnwrapTraits;
    typedef TUnwrapTraits<S7> TBound7UnwrapTraits;

    template<class P1, class P2, class P3, class P4, class P5, class P6,
        class P7>
    TBindState(
#ifdef ENABLE_BIND_LOCATION_TRACKING
        const ::NYT::TSourceLocation& location,
#endif
        const Runnable& runnable, P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5,
            P6&& p6, P7&& p7)
#ifdef ENABLE_BIND_LOCATION_TRACKING
        : TBindStateBase(location)
        , Runnable_(runnable)
#else
        : Runnable_(runnable)
#endif
        , S1_(std::forward<P1>(p1))
        , S2_(std::forward<P2>(p2))
        , S3_(std::forward<P3>(p3))
        , S4_(std::forward<P4>(p4))
        , S5_(std::forward<P5>(p5))
        , S6_(std::forward<P6>(p6))
        , S7_(std::forward<P7>(p7))
    { }

    virtual ~TBindState()
    { }

    TRunnableType Runnable_;
    S1 S1_;
    S2 S2_;
    S3 S3_;
    S4 S4_;
    S5 S5_;
    S6 S6_;
    S7 S7_;
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail
} // namespace NYT
