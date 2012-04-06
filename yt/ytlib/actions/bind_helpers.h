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

#include "callback_forward.h"

#include <ytlib/misc/rvalue.h>
#include <ytlib/misc/new.h>

namespace NYT {

struct TVoid
{ };

////////////////////////////////////////////////////////////////////////////////
//
// This file defines a set of argument wrappers that can be used specify
// the reference counting and reference semantics of arguments that are bound
// by the #Bind() function in "bind.h".
//
////////////////////////////////////////////////////////////////////////////////
//
// ARGUMENT BINDING WRAPPERS
//
// The wrapper functions are #Unretained(), #Owned(), #Passed(), #ConstRef(),
// and #IgnoreResult().
//
// #Unretained() allows #Bind() to bind a non-reference counted class,
// and to disable reference counting on arguments that are reference counted
// objects.
//
// #Owned() transfers ownership of an object to the #TCallback<> resulting from
// binding; the object will be deleted when the #TCallback<> is deleted.
//
// #Passed() is for transferring movable-but-not-copyable types through
// a #TCallback<>. Logically, this signifies a destructive transfer of the state
// of the argument into the target function. Invoking #TCallback<>::Run() twice
// on a TCallback that was created with a #Passed() argument will YASSERT()
// because the first invocation would have already transferred ownership
// to the target function.
//
// #ConstRef() allows binding a const reference to an argument rather than
// a copy.
//
// #IgnoreResult() is used to adapt a function or #TCallback<> with a return type
// to one with a void return. This is most useful if you have a function with,
// say, a pesky ignorable boolean return that you want to use with something
// that expect a #TCallback<> with a void return.
//
// 
// EXAMPLE OF Unretained()
//
//   class TFoo {
//     public:
//       void Bar() { Cout << "Hello!" << Endl; }
//   };
//
//   // Somewhere else.
//   TFoo foo;
//   TClosure cb = Bind(&TFoo::Bar, Unretained(&foo));
//   cb.Run(); // Prints "Hello!".
//
// Without the #Unretained() wrapper on |&foo|, the above call would fail
// to compile because |TFoo| does not support the Ref() and Unref() methods.
//
//
// EXAMPLE OF Owned()
//
//   void Foo(int* arg) { Cout << *arg << Endl; }
//
//   int* px = new int(17);
//   TClosure cb = Bind(&Foo, Owned(px));
//
//   cb.Run(); // Prints "17"
//   cb.Run(); // Prints "17"
//   *n = 42;
//   cb.Run(); // Prints "42"
//
//   cb.Reset(); // |px| is deleted.
//   // Also will happen when |cb| goes out of scope.
//
// Without #Owned(), someone would have to know to delete |px| when the last
// reference to the #TCallback<> is deleted.
//
//
// EXAMPLE OF Passed()
//
//   void TakesOwnership(TIntrusivePtr<TFoo> arg) { ... }
//   TIntrusivePtr<TFoo> CreateFoo() { return New<TFoo>(); }
//   TIntrusivePtr<TFoo> foo = New<TFoo>();
//
//   // |cb| is given ownership of the |TFoo| instance. |foo| is now NULL.
//   // You may also use MoveRV(foo), but its more verbose.
//   TClosure cb = Bind(&TakesOwnership, Passed(&foo));
//
//   // Run was never called so |cb| still owns the instance and deletes
//   // it on #Reset().
//   cb.Reset();
//
//   // |cb| is given a new |TFoo| created by |CreateFoo()|.
//   TClosure cb = Bind(&TakesOwnership, Passed(CreateFoo()));
//
//   // |arg| in TakesOwnership() is given ownership of |TFoo|.
//   // |cb| no longer owns |TFoo| and, if reset, would not delete anything.
//   cb.Run(); // |TFoo| is now transferred to |arg| and deleted.
//   cb.Run(); // This YASSERT()s since |TFoo| already been used once.
//
//
// EXAMPLE OF ConstRef()
//
//   void Foo(int arg) { Cout << arg << Endl; }
//
//   int n = 1;
//   TClosure noRef = Bind(&Foo, n);
//   TClosure hasRef = Bind(&Foo, ConstRef(n));
//
//   noRef.Run();  // Prints "1"
//   hasRef.Run(); // Prints "1"
//   n = 2;
//   noRef.Run();  // Prints "1"
//   hasRef.Run(); // Prints "2"
//
// Note that because #ConstRef() takes a reference on |n|,
// |n| must outlive all its bound callbacks.
//
//
// EXAMPLE OF IgnoreResult()
//
//   int Foo(int arg) { Cout << arg << Endl; }
//
//   // Assign to a #TCallback<> with a void return type.
//   TCallback<void(int)> cb = Bind(IgnoreResult(&Foo));
//   cb.Run(1); // Prints "1".
//
////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

template <class T>
class TUnretainedWrapper
{
public:
    explicit TUnretainedWrapper(T* x)
        : T_(x)
    { }
    T* Get() const
    {
        return T_;
    }
private:
    T* T_;
};

template <class T>
class TOwnedWrapper
{
public:
    explicit TOwnedWrapper(T* x)
        : T_(x)
    { }
    TOwnedWrapper(const TOwnedWrapper& other)
        : T_(other.T_)
    {
        other.T_ = NULL;
    }
    ~TOwnedWrapper()
    {
        delete T_;
    }
    T* Get() const
    {
        return T_;
    }
private:
    mutable T* T_;
};

template <class T>
class TPassedWrapper
{
public:
    explicit TPassedWrapper(T x)
        : IsValid(true)
        , T_(MoveRV(x))
    { }
    TPassedWrapper(const TPassedWrapper& other)
        : IsValid(other.IsValid)
        , T_(MoveRV(other.T_))
    {
        other.IsValid = false;
    }
    TPassedWrapper(TPassedWrapper&& other)
        : IsValid(other.IsValid)
        , T_(MoveRV(other.T_))
    {
        other.IsValid = false;
    }
    T&& Get() const
    {
        YASSERT(IsValid);
        IsValid = false;
        return static_cast<T&&>(T_);
    }
private:
    mutable bool IsValid;
    mutable T T_;
};

template <class T>
class TConstRefWrapper
{
public:
    explicit TConstRefWrapper(const T& x) 
        : T_(&x)
    { }
    const T& Get() const
    {
        return *T_;
    }
private:
    const T* T_;
};

////////////////////////////////////////////////////////////////////////////////
// #TUnwrapTraits that define how precisely the values are unwrapped at the
// invocation site and what types do they have after unwrapping.

template <class T>
struct TUnwrapTraits
{
    typedef const T& TType;
    static inline TType Unwrap(T& x)
    {
        return x;
    }
};

template <class T>
struct TUnwrapTraits< TUnretainedWrapper<T> >
{
    typedef T* TType;
    static inline TType Unwrap(const TUnretainedWrapper<T>& wrapper)
    {
        return wrapper.Get();
    }
};

template <class T>
struct TUnwrapTraits< TOwnedWrapper<T> >
{
    typedef T* TType;
    static inline TType Unwrap(const TOwnedWrapper<T>& wrapper)
    {
        return wrapper.Get();
    }
};

template <class T>
struct TUnwrapTraits< TPassedWrapper<T> >
{
    typedef T&& TType;
    static inline TType Unwrap(const TPassedWrapper<T>& wrapper)
    {
        return wrapper.Get();
    }
};

template <class T>
struct TUnwrapTraits< TConstRefWrapper<T> >
{
    typedef const T& TType;
    static inline TType Unwrap(const TConstRefWrapper<T>& wrapper)
    {
        return wrapper.Get();
    }
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail

template <class T>
static inline NYT::NDetail::TUnretainedWrapper<T> Unretained(T* x)
{
    return NYT::NDetail::TUnretainedWrapper<T>(x);
}

template <class T>
static inline NYT::NDetail::TOwnedWrapper<T> Owned(T* x)
{
    return NYT::NDetail::TOwnedWrapper<T>(x);
}

template <class T>
static inline NYT::NDetail::TPassedWrapper<T> Passed(T&& x)
{
    return NYT::NDetail::TPassedWrapper<T>(ForwardRV<T>(x));
}

template <class T>
static inline NYT::NDetail::TPassedWrapper<T> Passed(T* x)
{
    return NYT::NDetail::TPassedWrapper<T>(MoveRV(*x));
}

template <class T>
static inline NYT::NDetail::TPassedWrapper<T*> Passed(THolder<T>&& x)
{
    return NYT::NDetail::TPassedWrapper<T*>(x.Release());
}

template <class T>
static inline NYT::NDetail::TConstRefWrapper<T> ConstRef(const T& x)
{
    return NYT::NDetail::TConstRefWrapper<T>(x);
}

////////////////////////////////////////////////////////////////////////////////
// #IgnoreResult() and the corresponding wrapper.

namespace NDetail {
/* \internal */

template <class T>
struct TIgnoreResultWrapper
{
    explicit TIgnoreResultWrapper(const T& functor)
        : Functor(functor)
    { }
    T Functor;
};

template <class T>
struct TIgnoreResultWrapper< TCallback<T> >
{
    explicit TIgnoreResultWrapper(const TCallback<T>& callback)
        : Functor(callback)
    { }
    const TCallback<T>& Functor;
};

/*! \endinternal */
} // namespace NDetail

template <class T>
static inline NYT::NDetail::TIgnoreResultWrapper<T>
IgnoreResult(const T& x)
{
    return NYT::NDetail::TIgnoreResultWrapper< T >(x);
}

template <class T>
static inline NYT::NDetail::TIgnoreResultWrapper< TCallback<T> >
IgnoreResult(const TCallback<T>& x)
{
    return NYT::NDetail::TIgnoreResultWrapper< TCallback<T> >(x);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY
