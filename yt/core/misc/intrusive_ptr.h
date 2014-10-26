#pragma once

#include "assert.h"
#include "mpl.h"

// For DoSwap
#include <util/generic/utility.h>

// For std::move
#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Default (generic) implementation of Ref/Unref strategy.
// Assumes the existence of Ref/Unref members.
// Only works if |T| is fully defined.
template <class T>
void Ref(T* obj)
{
    obj->Ref();
}

template <class T>
void Unref(T* obj)
{
    obj->Unref();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TIntrusivePtr
{
public:
    typedef T TUnderlying;

    //! Empty constructor.
    TIntrusivePtr() // noexcept
        : T_(nullptr)
    { }

    //! Constructor from an unqualified reference.
    /*!
     * Note that this constructor could be racy due to unsynchronized operations
     * on the object and on the counter.
     *
     * Note that it notoriously hard to make this constructor explicit
     * given the current amount of code written.
     */
    TIntrusivePtr(T* obj) // noexcept
        : T_(obj)
    {
        if (T_) {
            Ref(T_);
        }
    }

    //! Constructor from an unqualified reference.
    TIntrusivePtr(T* obj, bool addReference) // noexcept
        : T_(obj)
    {
        if (T_ && addReference) {
            Ref(T_);
        }
    }

    //! Copy constructor.
    explicit TIntrusivePtr(const TIntrusivePtr& other) // noexcept
        : T_(other.Get())
    {
        if (T_) {
            Ref(T_);
        }
    }

    //! Copy constructor with an upcast.
    template <class U>
    TIntrusivePtr(
        const TIntrusivePtr<U>& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U*, T*>, int>::TType = 0) // noexcept
        : T_(other.Get())
    {
        if (T_) {
            Ref(T_);
        }
    }

    //! Move constructor.
    explicit TIntrusivePtr(TIntrusivePtr&& other) noexcept
        : T_(other.Get())
    {
        other.T_ = nullptr;
    }

    //! Move constructor with an upcast.
    template <class U>
    TIntrusivePtr(
        TIntrusivePtr<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U*, T*>, int>::TType = 0) // noexcept
        : T_(other.Get())
    {
        other.T_ = nullptr;
    }

    //! Destructor.
    ~TIntrusivePtr()
    {
        if (T_) {
            Unref(T_);
        }
    }

    //! Copy assignment operator.
    TIntrusivePtr& operator=(const TIntrusivePtr& other) // noexcept
    {
        TIntrusivePtr(other).Swap(*this);
        return *this;
    }

    //! Copy assignment operator with an upcast.
    template <class U>
    TIntrusivePtr& operator=(const TIntrusivePtr<U>& other) // noexcept
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        TIntrusivePtr(other).Swap(*this);
        return *this;
    }

    //! Move assignment operator.
    TIntrusivePtr& operator=(TIntrusivePtr&& other) noexcept
    {
        TIntrusivePtr(std::move(other)).Swap(*this);
        return *this;
    }

    //! Move assignment operator with an upcast.
    template <class U>
    TIntrusivePtr& operator=(TIntrusivePtr<U>&& other) // noexcept
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        TIntrusivePtr(std::move(other)).Swap(*this);
        return *this;
    }

    //! Drop the pointer.
    void Reset() // noexcept
    {
        TIntrusivePtr().Swap(*this);
    }

    //! Replace the pointer with a specified one.
    void Reset(T* p) // noexcept
    {
        TIntrusivePtr(p).Swap(*this);
    }

    //! Returns the pointer.
    T* Get() const // noexcept
    {
        return T_;
    }

    T& operator*() const // noexcept
    {
        YASSERT(T_);
        return *T_;
    }

    T* operator->() const // noexcept
    {
        YASSERT(T_);
        return  T_;
    }

    // Implicit conversion to bool.
    typedef T* TIntrusivePtr::*TUnspecifiedBoolType;
    operator TUnspecifiedBoolType() const // noexcept
    {
        return T_ ? &TIntrusivePtr::T_ : nullptr;
    }

    //! Swap the pointer with the other one.
    void Swap(TIntrusivePtr& r) // noexcept
    {
        DoSwap(T_, r.T_);
    }

private:
    template <class U>
    friend class TIntrusivePtr;

    T* T_;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates a strong pointer wrapper for a given raw pointer.
//! Compared to |TIntrusivePtr<T>::ctor|, type inference enables omitting |T|.
template <class T>
TIntrusivePtr<T> MakeStrong(T* p)
{
    return TIntrusivePtr<T>(p);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator<(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return lhs.Get() < rhs.Get();
}

template <class T>
bool operator>(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return lhs.Get() > rhs.Get();
}

template <class T, class U>
bool operator==(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<U>& rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs.Get() == rhs.Get();
}

template <class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<U>& rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs.Get() != rhs.Get();
}

template <class T, class U>
bool operator==(const TIntrusivePtr<T>& lhs, U* rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs.Get() == rhs;
}

template <class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, U* rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs.Get() != rhs;
}

template <class T, class U>
bool operator==(T* lhs, const TIntrusivePtr<U>& rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs == rhs.Get();
}

template <class T, class U>
bool operator!=(T* lhs, const TIntrusivePtr<U>& rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs != rhs.Get();
}

////////////////////////////////////////////////////////////////////////////////

// A bunch of helpful macros that enable working with intrusive pointers to incomplete types.
/*
 *  Typically when you have a forward-declared type |T| and an instance
 *  of |TIntrusivePtr<T>| you need the complete definition of |T| to work with
 *  the pointer even if you're not actually using the members of |T|.
 *  E.g. the dtor of |TIntrusivePtr<T>|, should you ever need it, must be able
 *  to unref an instance of |T| and eventually destroy it.
 *  This may force #inclusion of way more headers than really seems necessary.
 *
 *  |DECLARE_REFCOUNTED_STRUCT|, |DECLARE_REFCOUNTED_CLASS|, and |DEFINE_REFCOUNTED_TYPE|
 *  alleviate this issue by forcing TIntrusivePtr to work with the free-standing overloads
 *  of |Ref| and |Unref| instead of their template version.
 *  These overloads are declared together with the forward declaration of |T| and
 *  are subsequently defined afterwards.
 */

#ifdef __linux__
    // Prevent GCC from throwing out our precious Ref/Unref functions in
    // release builds.
    #define REF_UNREF_DECLARATION_ATTRIBUTES __attribute__((used))
#else
    #define REF_UNREF_DECLARATION_ATTRIBUTES
#endif

#define DECLARE_REFCOUNTED_TYPE(kind, type) \
    kind type; \
    typedef ::NYT::TIntrusivePtr<type> type ## Ptr; \
    \
    void Ref(type* obj) REF_UNREF_DECLARATION_ATTRIBUTES; \
    void Ref(const type* obj) REF_UNREF_DECLARATION_ATTRIBUTES; \
    void Unref(type* obj) REF_UNREF_DECLARATION_ATTRIBUTES; \
    void Unref(const type* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

//! Forward-declares a class type, defines an intrusive pointer for it, and finally
//! declares Ref/Unref overloads. Use this macro in |public.h|-like files.
#define DECLARE_REFCOUNTED_CLASS(type) \
    DECLARE_REFCOUNTED_TYPE(class, type)

//! Forward-declares a struct type, defines an intrusive pointer for it, and finally
//! declares Ref/Unref overloads. Use this macro in |public.h|-like files.
#define DECLARE_REFCOUNTED_STRUCT(type) \
    DECLARE_REFCOUNTED_TYPE(struct, type)

//! Provides implementations for Ref/Unref overloads. Use this macro right
//! after the type's full definition.
#define DEFINE_REFCOUNTED_TYPE(type) \
    inline void Ref(type* obj) \
    { \
        obj->Ref(); \
    } \
    \
    inline void Ref(const type* obj) \
    { \
        obj->Ref(); \
    } \
    \
    inline void Unref(type* obj) \
    { \
        obj->Unref(); \
    } \
    \
    inline void Unref(const type* obj) \
    { \
        obj->Unref(); \
    }

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
