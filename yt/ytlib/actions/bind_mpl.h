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

#include <ytlib/misc/mpl.h>

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
// === THas{Ref,Unref}Method ===
//
// Use the Substitution Failure Is Not An Error (SFINAE) trick to inspect T
// for the existence of Ref() and Unref() methods.
//
// http://en.wikipedia.org/wiki/Substitution_failure_is_not_an_error
// http://stackoverflow.com/questions/257288/is-it-possible-to-write-a-c-template-to-check-for-a-functions-existence
// http://stackoverflow.com/questions/4358584/sfinae-approach-comparison
// http://stackoverflow.com/questions/1966362/sfinae-to-check-for-inherited-member-functions
//
// The last link in particular show the method used below.
// Works on gcc-4.2, gcc-4.4, and Visual Studio 2008.
//

//! An MPL functor which tests for existance of Ref() method in a given type.
template <class T>
struct THasRefMethod
{
private:
    struct TMixin
    {
        void Ref();
    };
    // MSVC warns when you try to use TMixed if T has a private destructor,
    // the common pattern for reference-counted types. It does this even though
    // no attempt to instantiate TMixed is made.
    // We disable the warning for this definition.
#ifdef _win_
#pragma warning(disable:4624)
#endif
    struct TMixed
        : public T
        , public TMixin
    { };
#ifdef _win_
#pragma warning(default:4624)
#endif
    template <void(TMixin::*)()>
    struct THelper
    { };

    template <class U>
    static NMpl::NDetail::TNoType  Test(THelper<&U::Ref>*);
    template <class>
    static NMpl::NDetail::TYesType Test(...);

public:
    enum
    {
        Value = (sizeof(Test<TMixed>(0)) == sizeof(NMpl::NDetail::TYesType))
    };
};

//! An MPL functor which tests for existance of Unref() method in a given type.
template <class T>
struct THasUnrefMethod
{
private:
    struct TMixin
    {
        void Unref();
    };
#ifdef _win_
#pragma warning(disable:4624)
#endif
    struct TMixed
        : public T
        , public TMixin
    { };
#ifdef _win_
#pragma warning(default:4624)
#endif

    template <void(TMixin::*)()>
    struct THelper
    { };

    template <class U>
    static NMpl::NDetail::TNoType  Test(THelper<&U::Unref>*);
    template <class>
    static NMpl::NDetail::TYesType Test(...);

public:
    enum
    {
        Value = (sizeof(Test<TMixed>(0)) == sizeof(NMpl::NDetail::TYesType))
    };
};

//! An MPL functor which tests for existance of both Ref() and Unref() methods.
template <class T>
struct THasRefAndUnrefMethods
    : NMpl::TIntegralConstant<bool, NMpl::TAnd<
        THasRefMethod<T>,
        THasUnrefMethod<T>
    >::Value>
{ };

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

#ifdef _win_
#pragma warning(disable:4413)
#endif

template <bool IsMethod, class T>
struct TMaybeLockHelper
{
    T&& T_;
    inline TMaybeLockHelper(T&& x)
        : T_(static_cast<T&&>(x))
    { }
    inline const TMaybeLockHelper& Lock() const
    {
        return *this;
    }
    inline T&& Get() const
    {
        return static_cast<T&&>(T_);
    }
};

template <class U>
struct TMaybeLockHelper< true, TIntrusivePtr<U> >
{
    typedef TIntrusivePtr<U> T;
    inline TMaybeLockHelper(T&& x)
    {
        static_assert(U::False, "Current implementation should pass smart pointers by reference.");
    }
};

template <class U>
struct TMaybeLockHelper< true, TIntrusivePtr<U>& >
{
    typedef TIntrusivePtr<U>& T;
    T&& T_;
    inline TMaybeLockHelper(T&& x)
        : T_(static_cast<T&&>(x))
    { }
    inline T&& Lock() const
    {
        return static_cast<T&&>(T_);
    }
};

template <class U>
struct TMaybeLockHelper< true, const TIntrusivePtr<U>& >
{
    typedef const TIntrusivePtr<U>& T;
    T&& T_;
    inline TMaybeLockHelper(T&& x)
        : T_(static_cast<T&&>(x))
    { }
    inline T&& Lock() const
    {
        return static_cast<T&&>(T_);
    }
};

template <class U>
struct TMaybeLockHelper< true, TWeakPtr<U> >
{
    typedef TWeakPtr<U> T;
    inline TMaybeLockHelper(T&& x)
    {
        static_assert(U::False, "Current implementation should pass smart pointers by reference.");
    } 
};

template <class U>
struct TMaybeLockHelper< true, TWeakPtr<U>& >
{
    typedef TWeakPtr<U>& T;
    T&& T_;
    inline TMaybeLockHelper(T&& x)
        : T_(static_cast<T&&>(x))
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
    T&& T_;
    inline TMaybeLockHelper(T&& x) 
        : T_(static_cast<T&&>(x))
    { }
    inline TIntrusivePtr<U> Lock() const 
    {
        return T_.Lock();
    }
};

#ifdef _win_
#pragma warning(default:4413)
#endif

////////////////////////////////////////////////////////////////////////////////
//
// === TMaybeCopyHelper ===
//
// #TMaybeCopyHelper makes a scoped copy of the argument if the type coercion
// semantics require so.
//

template <class TExpected>
struct TMaybeCopyHelper
{
    typedef TExpected T;
    static inline T&& Do(T&& x)
    {
        return static_cast<T&&>(x);
    }
    static inline T Do(const T& x)
    {
        return x;
    }
};

template <class TExpected>
struct TMaybeCopyHelper<const TExpected&>
{
    typedef TExpected T;
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
        static_assert(T::False, "Current implementation should not make copies of non-const lvalue-references.");
    }
};

template <class T>
struct TMaybeCopyHelper<T&&>
{
    static inline void Do()
    {
        static_assert(T::False, "Current implementation should not make copies of rvalue-references.");
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

template <class T>
struct TRawPtrToRefCountedTypeHelper
{
#if defined(_win_)
    enum {
        Value = 0
    };
#else
    enum {
        Value = (NMpl::TIsPointer<T>::Value && (
            NMpl::TIsConvertible<T, TIntrinsicRefCounted*>::Value ||
            NMpl::TIsConvertible<T, TExtrinsicRefCounted*>::Value
        ))
    };
#endif
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail                                                        
} // namespace NY
