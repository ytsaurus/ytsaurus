#pragma once

#include "assert.h"
#include "mpl.h"

// For DoSwap
#include <util/generic/utility.h>

// For std::move
#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DEFINE_MPL_MEMBER_DETECTOR(Ref);
DEFINE_MPL_MEMBER_DETECTOR(Unref);

//! An MPL functor which tests for existance of both Ref() and Unref() methods.
template <class T>
struct THasRefAndUnrefMethods
    : NMpl::TIntegralConstant<bool, NMpl::TAnd<
        THasRefMember<T>,
        THasUnrefMember<T>
    >::Value>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TIntrusivePtr
{
public:
    typedef T TUnderlying;

    //! Empty constructor.
    TIntrusivePtr() // noexcept
        : T_(NULL)
    { }

    //! Constructor from an unqualified reference.
    /*!
     * Note that this constructor could be racy due to unsynchronized operations
     * on the object and on the counter.
     *
     * Note that it notoriously hard to make this constructor explicit
     * given the current amount of code written.
     */
    TIntrusivePtr(T* p) // noexcept
        : T_(p)
    {
        static_assert(
            NYT::NDetail::THasRefAndUnrefMethods<T>::Value,
            "T must have Ref() and UnRef() methods");

        if (T_) {
            T_->Ref();
        }
    }

    //! Constructor from an unqualified reference.
    TIntrusivePtr(T* p, bool addReference) // noexcept
        : T_(p)
    {
        static_assert(
            NYT::NDetail::THasRefAndUnrefMethods<T>::Value,
            "T must have Ref() and UnRef() methods");

        if (T_ && addReference) {
            T_->Ref();
        }
    }

    //! Copy constructor.
    explicit TIntrusivePtr(const TIntrusivePtr& other) // noexcept
        : T_(other.Get())
    {
        if (T_) {
            T_->Ref();
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
            T_->Ref();
        }
    }

    //! Move constructor.
    explicit TIntrusivePtr(TIntrusivePtr&& other) // noexcept
        : T_(other.Get())
    {
        other.T_ = NULL;
    }

    //! Move constructor with an upcast.
    template <class U>
    TIntrusivePtr(
        TIntrusivePtr<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U*, T*>, int>::TType = 0) // noexcept
        : T_(other.Get())
    {
        other.T_ = NULL;
    }

    //! Destructor.
    ~TIntrusivePtr()
    {
        if (T_) {
            T_->Unref();
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
    TIntrusivePtr& operator=(TIntrusivePtr&& other) // noexcept
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
        return T_ ? &TIntrusivePtr::T_ : NULL;
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

} //namespace NYT
