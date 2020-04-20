#pragma once

#include "mpl.h"
#include "ref_counted.h"

#include <util/generic/hash.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRefCounted;

template <class T>
class TWeakPtr
{
public:
    typedef T TUnderlying;

    //! Empty constructor.
    TWeakPtr() = default;

    TWeakPtr(std::nullptr_t)
    { }

    //! Constructor from an unqualified reference.
    /*!
     * Note that this constructor could be racy due to unsynchronized operations
     * on the object and on the counter.
     */
    explicit TWeakPtr(T* p) noexcept
        : T_(p)
    {
        if (T_) {
            T_->WeakRef();
        }
    }

    //! Constructor from a strong reference.
    template <class U>
    TWeakPtr(
        const TIntrusivePtr<U>& p,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U*, T*>, int>::TType = 0) noexcept
        : T_(p.Get())
    {
        if (T_) {
            T_->WeakRef();
        }
    }

    //! Copy constructor.
    explicit TWeakPtr(const TWeakPtr& other) noexcept
        : T_(other.T_)
    {
        if (T_) {
            T_->WeakRef();
        }
    }

    //! Copy constructor with an upcast.
    template <class U>
    TWeakPtr(
        const TWeakPtr<U>& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U*, T*>, int>::TType = 0) noexcept
    {
        TIntrusivePtr<U> strongOther = other.Lock();
        if (strongOther) {
            T_ = strongOther.Get();
            T_->WeakRef();
        }
    }

    //! Move constructor.
    explicit TWeakPtr(TWeakPtr&& other) noexcept
        : T_(other.T_)
    {
        other.T_ = nullptr;
    }

    //! Move constructor with an upcast.
    template <class U>
    TWeakPtr(
        TWeakPtr<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U*, T*>, int>::TType = 0) noexcept
    {
        TIntrusivePtr<U> strongOther = other.Lock();
        if (strongOther) {
            T_ = other.T_;

            other.T_ = nullptr;
        }
    }

    //! Destructor.
    ~TWeakPtr()
    {
        if (T_) {
            // Support incomplete type.
            static_cast<const TRefCounted*>(GetRefCountedBase(T_))->WeakUnref();
        }
    }

    //! Assignment operator from a strong reference.
    template <class U>
    TWeakPtr& operator=(const TIntrusivePtr<U>& p) noexcept
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        TWeakPtr(p).Swap(*this);
        return *this;
    }

    //! Copy assignment operator.
    TWeakPtr& operator=(const TWeakPtr& other) noexcept
    {
        TWeakPtr(other).Swap(*this);
        return *this;
    }

    //! Copy assignment operator with an upcast.
    template <class U>
    TWeakPtr& operator=(const TWeakPtr<U>& other) noexcept
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        TWeakPtr(other).Swap(*this);
        return *this;
    }

    //! Move assignment operator.
    TWeakPtr& operator=(TWeakPtr&& other) noexcept
    {
        TWeakPtr(std::move(other)).Swap(*this);
        return *this;
    }

    //! Move assignment operator with an upcast.
    template <class U>
    TWeakPtr& operator=(TWeakPtr<U>&& other) noexcept
    {
        static_assert(NMpl::TIsConvertible<U*, T*>::Value, "U* have to be convertible to T*");
        TWeakPtr(std::move(other)).Swap(*this);
        return *this;
    }

    //! Drop the pointer.
    void Reset() // noexcept
    {
        TWeakPtr().Swap(*this);
    }

    //! Replace the pointer with a specified one.
    void Reset(T* p) // noexcept
    {
        TWeakPtr(p).Swap(*this);
    }

    //! Replace the pointer with a specified one.
    template <class U>
    void Reset(const TIntrusivePtr<U>& p) // noexcept
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        TWeakPtr(p).Swap(*this);
    }

    //! Acquire a strong reference to the pointee and return a strong pointer.
    TIntrusivePtr<T> Lock() const noexcept
    {
        return T_ && T_->TryRef()
            ? TIntrusivePtr<T>(T_, false)
            : TIntrusivePtr<T>();
    }

    //! Swap the pointer with the other one.
    void Swap(TWeakPtr& r) noexcept
    {
        DoSwap(T_, r.T_);
    }

    bool IsExpired() const noexcept
    {
        return !T_ || (T_->GetRefCount() == 0);
    }

private:
    template <class U>
    friend class TWeakPtr;
    template <class U>
    friend struct ::THash;

    T* T_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates a weak pointer wrapper for a given raw pointer.
//! Compared to |TWeakPtr<T>::ctor|, type inference enables omitting |T|.
template <class T>
TWeakPtr<T> MakeWeak(T* p)
{
    return TWeakPtr<T>(p);
}

//! Creates a weak pointer wrapper for a given intrusive pointer.
//! Compared to |TWeakPtr<T>::ctor|, type inference enables omitting |T|.
template <class T>
TWeakPtr<T> MakeWeak(const TIntrusivePtr<T>& p)
{
    return TWeakPtr<T>(p);
}

//! A helper for acquiring weak pointer for pointee, resetting intrusive pointer and then
//! returning the pointee reference count using the acquired weak pointer.
//! This helper is designed for best effort in checking that the object is not leaked after
//! destructing (what seems to be) the last pointer to it.
//! NB: it is possible to rewrite this helper making it working event with intrinsic refcounted objects,
//! but it requires much nastier integration with the intrusive pointer destruction routines.
template <typename T>
int ResetAndGetResidualRefCount(TIntrusivePtr<T>& pointer)
{
    auto weakPointer = MakeWeak(pointer);
    pointer.Reset();
    if (pointer = weakPointer.Lock()) {
        // This _may_ return 0 if we are again the only holder of the pointee.
        return pointer->GetRefCount() - 1;
    } else {
        return 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Kill comparsions.
template <class T>
bool operator<(const TWeakPtr<T>& lhs, const TWeakPtr<T>& rhs)
{
    return lhs.Lock().Get() < rhs.Lock().Get();
}

template <class T>
bool operator>(const TWeakPtr<T>& lhs, const TWeakPtr<T>& rhs)
{
    return lhs.Lock().Get() > rhs.Lock().Get();
}

template <class T, class U>
bool operator==(const TWeakPtr<T>& lhs, const TWeakPtr<U>& rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs.Lock().Get() == rhs.Lock().Get();
}

template <class T, class U>
bool operator!=(const TWeakPtr<T>& lhs, const TWeakPtr<U>& rhs)
{
    static_assert(
        NMpl::TIsConvertible<U*, T*>::Value,
        "U* have to be convertible to T*");
    return lhs.Lock().Get() != rhs.Lock().Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


//! A hasher for TWeakPtr.
template <class T>
struct THash<NYT::TWeakPtr<T>>
{
    size_t operator () (const NYT::TWeakPtr<T>& ptr) const
    {
        return THash<const NYT::TRefCounted*>()(ptr.T_);
    }
};
