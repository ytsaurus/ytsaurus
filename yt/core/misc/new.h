#pragma once

#include "source_location.h"

#include <typeinfo>

#include <util/system/defaults.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * \defgroup yt_new New<T> safe smart pointer constructors
 * \ingroup yt_new
 *
 * This is collection of safe smart pointer constructors.
 *
 * \{
 *
 * \page yt_new_rationale Rationale
 * New<T> function family was designed to prevent the following problem.
 * Consider the following piece of code.
 *
 * \code
 *     class TFoo
 *         : public virtual TRefCounted
 *     {
 *     public:
 *         TFoo();
 *     };
 *     
 *     typedef TIntrusivePtr<TFoo> TFooPtr;
 *
 *     void RegisterObject(TFooPtr foo)
 *     {
 *         ...
 *     }
 *
 *     TFoo::TFoo()
 *     {
 *         // ... do something before
 *         RegisterObject(this);
 *         // ... do something after
 *     }
 * \endcode
 *
 * What will happen on <tt>new TFoo()</tt> construction? After memory allocation
 * the reference counter for newly created instance would be initialized to
     zero.
 * Afterwards, the control goes to TFoo constructor. To invoke
 * <tt>RegisterObject</tt> a new temporary smart pointer to the current instance
 * have to be created effectively incrementing the reference counter (now one).
 * After <tt>RegisterObject</tt> returns the control to the constructor
 * the temporary pointer is destroyed effectively decrementing the reference
 * counter to zero hence triggering object destruction during its initialization.
 *
 * To avoid this undefined behavior <tt>New<T></tt> was introduced.
 * <tt>New<T></tt> holds a fake
 * reference to the object during its construction effectively preventing
 * premature destruction.
 *
 * \note An initialization like <tt>TIntrusivePtr&lt;T&gt; p = new T()</tt>
 * would result in a dangling reference due to internals of #New<T> and
 * #TRefCountedBase.
 */

////////////////////////////////////////////////////////////////////////////////

template <class T>
TRefCountedTypeKey GetRefCountedTypeKey()
{
    return &typeid(T);
}

TRefCountedTypeCookie GetRefCountedTypeCookie(
    TRefCountedTypeKey typeKey,
    const TSourceLocation& location);

template <class T>
FORCED_INLINE TRefCountedTypeCookie GetRefCountedTypeCookie()
{
    static auto cookie = NullRefCountedTypeCookie;
    if (UNLIKELY(cookie == NullRefCountedTypeCookie)) {
        cookie = GetRefCountedTypeCookie(
            GetRefCountedTypeKey<T>(),
            NYT::TSourceLocation());
    }
    return cookie;
}

template <class T, class TTag, int Counter>
FORCED_INLINE TRefCountedTypeCookie GetRefCountedTypeCookieWithLocation(const TSourceLocation& location)
{
    static auto cookie = NullRefCountedTypeCookie;
    if (UNLIKELY(cookie == NullRefCountedTypeCookie)) {
        cookie = GetRefCountedTypeCookie(
            GetRefCountedTypeKey<T>(),
            location);
    }
    return cookie;
}

namespace {

//! A per-translation unit tag type.
struct TCurrentTranslationUnitTag
{ };

} // namespace

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

#define REF_COUNTED_NEW_EPILOGUE() \
    InitializeTracking(result.Get(), cookie, sizeof (T))

#else // !YT_ENABLE_REF_COUNTED_TRACKING

#define REF_COUNTED_NEW_EPILOGUE() \
    (void) 0

#endif // YT_ENABLE_REF_COUNTED_TRACKING

template <class T, class... As>
inline TIntrusivePtr<T> New(As&&... args)
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    auto cookie = GetRefCountedTypeCookie<T>();
#endif
    TIntrusivePtr<T> result(new T(std::forward<As>(args)...), false);
    REF_COUNTED_NEW_EPILOGUE();
    return result;
}

template <class T, class TTag, int Counter, class... As>
inline TIntrusivePtr<T> NewWithLocation(
    const TSourceLocation& location,
    As&&... args)
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    auto cookie = GetRefCountedTypeCookieWithLocation<T, TTag, Counter>(location);
#endif
    TIntrusivePtr<T> result(new T(std::forward<As>(args)...), false);
    REF_COUNTED_NEW_EPILOGUE();
    return result;    
}

#undef REF_COUNTED_NEW_EPILOGUE

#define DECLARE_NEW_FRIEND() \
    template <class T, class... As> \
    friend TIntrusivePtr<T> NYT::New(As&&... args);

/*! \} */

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
