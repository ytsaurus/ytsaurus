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

//#include "public.h"

#include <core/misc/mpl.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
//
// This defines a set of MPL functors which are used to augment #Bind()
// behaviour for various types (i. e. to properly implement reference counting
// semantics) and to perform compile time assertions.
//
////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): These are forward declarations.
class TIntrinsicRefCounted;
class TExtrinsicRefCounted;

template <class T>
class TIntrusivePtr;

template <class T>
class TWeakPtr;

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////
//
// === TIsMethodHelper, TIsWeakMethodHelper ===
//
// #TIsMethod functor tests the given runnable type if it is a bound method.
// It does so by inspecting T for existance of IsMethod typedef (see "bind.h")
//

//! An MPL functor which tests for existance of IsMethod typedef.
template <class T>
struct TIsMethodHelper
{
private:
    template <class U>
    static NMpl::NDetail::TYesType& Test(typename U::IsMethod*);
    template <class U>
    static NMpl::NDetail::TNoType&  Test(...);

public:
    enum
    {
        Value = (sizeof(Test<T>(0)) == sizeof(NMpl::NDetail::TYesType))
    };
};

//! An MPL functor which tests if we are binding to a weak target.
/*!
 * It is used internally by #Bind() to select the correct invoker that will
 * no-op itself in case of expiration of the bound target object pointer.
 *
 * P1 should be the type of the object that will be received of the method.
 */
template <bool IsMethod, class P1>
struct TIsWeakMethodHelper
    : NMpl::TFalseType
{ };

template <class T>
struct TIsWeakMethodHelper<true, TWeakPtr<T> >
    : NMpl::TTrueType
{ };

template <class T>
struct TIsWeakMethodHelper<true, TConstRefWrapper< TWeakPtr<T> > >
    : NMpl::TTrueType
{ };

////////////////////////////////////////////////////////////////////////////////
//
// === TMaybeLockHelper ===
//
// #TMaybeLockHelper introduces extra scope object which supports .Lock().Get()
// syntax (i. e. TMaybeLockHelper<...>(x).Lock().Get()).
//
// The semantics are as following:
//   - for #TIntrusivePtr<>
//     .Lock() is a no-op,
//     .Get() extracts T* from the strong reference.
//   - for #TWeakPtr<>
//     .Lock() acquires a strong reference to the pointee,
//     .Get() extracts T* from the strong reference.
//   - for other types this scoper simply collapses to no-op.
//
// XXX(sandello): The code is a little bit messy, but works fine.
//

template <bool IsMethod, class T>
struct TMaybeLockHelper
{
    const T& T_;
    inline TMaybeLockHelper(const T& x)
        : T_(x)
    { }
    inline const TMaybeLockHelper& Lock() const
    {
        return *this;
    }
    inline const T& Get() const
    {
        return T_;
    }
};

template <bool IsMethod, class T>
struct TMaybeLockHelper< IsMethod, T&& >
{
    T&& T_;
    inline TMaybeLockHelper(T&& x)
        : T_(std::move(x))
    { }
    inline const TMaybeLockHelper& Lock() const
    {
        return *this;
    }
    inline T&& Get() const
    {
        return std::move(T_);
    }
};

template <class U>
struct TMaybeLockHelper< true, TIntrusivePtr<U> >
{
    typedef TIntrusivePtr<U> T;
    inline TMaybeLockHelper(const T& x)
    {
        static_assert(
            U::False,
            "Current implementation should pass smart pointers by reference.");
    }
};

template <class U>
struct TMaybeLockHelper< true, TIntrusivePtr<U>& >
{
    typedef TIntrusivePtr<U>& T;
    T T_;
    inline TMaybeLockHelper(T x)
        : T_(x)
    { }
    inline T Lock() const
    {
        return T_;
    }
};

template <class U>
struct TMaybeLockHelper< true, const TIntrusivePtr<U>& >
{
    typedef const TIntrusivePtr<U>& T;
    T T_;
    inline TMaybeLockHelper(T x)
        : T_(x)
    { }
    inline T Lock() const
    {
        return T_;
    }
};

template <class U>
struct TMaybeLockHelper< true, TWeakPtr<U> >
{
    typedef TWeakPtr<U> T;
    inline TMaybeLockHelper(const T& x)
    {
        static_assert(
            U::False,
            "Current implementation should pass smart pointers by reference.");
    }
};

template <class U>
struct TMaybeLockHelper< true, TWeakPtr<U>& >
{
    typedef TWeakPtr<U>& T;
    T T_;
    inline TMaybeLockHelper(T x)
        : T_(x)
    { }
    inline TIntrusivePtr<U> Lock() const
    {
        return T_.Lock();
    }
};

template <class U>
struct TMaybeLockHelper< true, const TWeakPtr<U>& >
{
    typedef const TWeakPtr<U>& T;
    T T_;
    inline TMaybeLockHelper(T x)
        : T_(x)
    { }
    inline TIntrusivePtr<U> Lock() const
    {
        return T_.Lock();
    }
};

////////////////////////////////////////////////////////////////////////////////
//
// === TMaybeCopyHelper ===
//
// #TMaybeCopyHelper makes a scoped copy of the argument if the type coercion
// semantics require so.
//

template <class T>
struct TMaybeCopyHelper
{
    static inline T&& Do(T&& x)
    {
        return std::move(x);
    }
    static inline T Do(const T& x)
    {
        return x;
    }
};

template <class T>
struct TMaybeCopyHelper<const T&>
{
    static inline const T& Do(const T& x)
    {
        return x;
    }
};

template <class T>
struct TMaybeCopyHelper<T&>
{
    static inline void Do()
    {
        static_assert(
            T::False,
            "Current implementation should not make copies of non-const lvalue-references.");
    }
};

template <class T>
struct TMaybeCopyHelper<T&&>
{
    static inline T&& Do(T&& x)
    {
        return std::move(x);
    }
};

////////////////////////////////////////////////////////////////////////////////
//
// === TMaybeRefCountHelper ===
//
// #TMaybeRefCountHelper handles different reference-counting semantics
// in the #Bind()
// function.

template <bool IsMethod, class T>
struct TMaybeRefCountHelper;

template <class T>
struct TMaybeRefCountHelper<false, T>
{
    static void Ref(const T&)
    { }
    static void Unref(const T&)
    { }
};

template <class T>
struct TMaybeRefCountHelper<true, T>
{
    static void Ref(const T&)
    { }
    static void Unref(const T&)
    { }
};

template <class T>
struct TMaybeRefCountHelper<true, T*>
{
    static void Ref(T* p)
    { p->Ref(); }
    static void Unref(T* p)
    { p->Unref(); }
};

template <class T>
struct TMaybeRefCountHelper<true, const T*>
{
    static void Ref(const T* p)
    { p->Ref(); }
    static void Unref(const T* p)
    { p->Unref(); }
};

template <class T>
struct TMaybeRefCountHelper<true, TIntrusivePtr<T> >
{
    static void Ref(const TIntrusivePtr<T>& ptr)
    { }
    static void Unref(const TIntrusivePtr<T>& ptr)
    { }
};

template <class T>
struct TMaybeRefCountHelper<true, TWeakPtr<T> >
{
    static void Ref(const TWeakPtr<T>& ptr)
    { }
    static void Unref(const TWeakPtr<T>& ptr)
    { }
};

//! Helpers to detect error-prone behaviours.
/*! \{ */

template <class T>
struct TIsNonConstReference
    : NMpl::TFalseType
{ };

template <class T>
struct TIsNonConstReference<T&>
    : NMpl::TTrueType
{ };

template <class T>
struct TIsNonConstReference<const T&>
    : NMpl::TFalseType
{ };

/*! \} */

template <class TRunnable, class... TParams>
struct TCheckFirstArgument
{ };

template <class TRunnable, class TFirstParam, class... TOtherParams>
struct TCheckFirstArgument<TRunnable, TFirstParam, TOtherParams...>
{
     static_assert(!(
         NYT::NDetail::TIsMethodHelper<TRunnable>::Value &&
         NMpl::TIsArray<TFirstParam>::Value),
         "First bound argument to a method cannot be an array");
};

template <class T>
struct TIsRawPtrToRefCountedType
{
    enum {
        Value = (NMpl::TIsPointer<T>::Value && (
            NMpl::TIsConvertible<T, TIntrinsicRefCounted*>::Value ||
            NMpl::TIsConvertible<T, TExtrinsicRefCounted*>::Value
        ))
    };
};

template <class T>
struct TCheckIsRawPtrToRefCountedTypeHelper
{
    static_assert(
        !NYT::NDetail::TIsRawPtrToRefCountedType<T>::Value,
        "T has reference-counted type and should not be bound by the raw pointer");
};

template <class T>
struct TCheckArgIsNonConstReference
{
    static_assert(
        !NYT::NDetail::TIsNonConstReference<T>::Value,
        "T is a non-const reference and should not be bound.");
};

template <class TSignature>
struct TCheckReferencesInSignature;

template <class R, class... TArgs>
struct TCheckReferencesInSignature<R(TArgs...)>
    : NYT::NMpl::TTypesPack<TCheckArgIsNonConstReference<TArgs>...>
{ };

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail
} // namespace NY
