#pragma once

#include "mpl.h"
#include "rvalue.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct TIntrusivePtrTraits
{
    static void Ref(T* p)
    {
        p->Ref();
    }

    static void UnRef(T* p)
    {
        p->UnRef();
    }
};

template<class T>
struct TIntrusivePtrTraits<const T>
{
    static void Ref(const T* p)
    {
        const_cast<T*>(p)->Ref();
    }

    static void UnRef(const T* p)
    {
        const_cast<T*>(p)->UnRef();
    }
};

////////////////////////////////////////////////////////////////////////////////

template<class T>
class TIntrusivePtr
{
public:
    typedef T TElementType;

    //! Empty constructor.
    TIntrusivePtr() // noexcept
        : T_(NULL)
    { }

    //! Constructor from an unqualified reference.
    /*!
     * Note that this constructor could be racy due to unsynchronized operations
     * on the object and on the counter.
     */
    TIntrusivePtr(T* p) // noexcept
        : T_(p)
    {
        if (T_) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }
    }

    //! Constructor from an unqualified reference.
    TIntrusivePtr(T* p, bool addReference) // noexcept
        : T_(p)
    {
        if (T_ && addReference) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }
    }

    //! Copy constructor.    
    TIntrusivePtr(const TIntrusivePtr& other) // noexcept
        : T_(other.Get())
    {
        if (T_) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }           
    }

    //! Copy constructor with an upcast.
    template<class U, class = typename NMpl::TEnableIf<
            NMpl::TIsConvertible<U*, T*>
        >::TType>
    TIntrusivePtr(const TIntrusivePtr<U>& other) // noexcept
        : T_(other.Get())
    {
        if (T_) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }
    }

    //! Move constructor.
    TIntrusivePtr(TIntrusivePtr&& other) // noexcept
        : T_(other.Get())
    {
        other.T_ = NULL;
    }

    //! Move constructor with an upcast.
    template<class U, class = typename NMpl::TEnableIf<
            NMpl::TIsConvertible<U*, T*>
        >::TType>
    TIntrusivePtr(TIntrusivePtr<U>&& other) // noexcept
        : T_(other.Get())
    {
        other.T_ = NULL;
    }

    //! Destructor.
    ~TIntrusivePtr()
    {
        if (T_) {
            TIntrusivePtrTraits<T>::UnRef(T_);
        }
    }

    //! Copy assignment operator.
    TIntrusivePtr& operator=(const TIntrusivePtr& other) // noexcept
    {
        TIntrusivePtr(other).Swap(*this);
        return *this;
    }

    //! Copy assignment operator with an upcast.
    template<class U, class = typename NMpl::TEnableIf<
            NMpl::TIsConvertible<U*, T*>
        >::TType>
    TIntrusivePtr& operator=(const TIntrusivePtr<U>& other) // noexcept
    {
        TIntrusivePtr(other).Swap(*this);
        return *this;
    }

    //! Move assignment operator.
    TIntrusivePtr& operator=(TIntrusivePtr&& other) // noexcept
    {
        TIntrusivePtr(MoveRV(other)).Swap(*this);
        return *this;
    }

    //! Move assignment operator with an upcast.
    template<class U, class = typename NMpl::TEnableIf<
            NMpl::TIsConvertible<U*, T*>
        >::TType>
    TIntrusivePtr& operator=(TIntrusivePtr<U>&& other) // noexcept
    {
        TIntrusivePtr(MoveRV(other)).Swap(*this);
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
    template<class U>
    friend class TIntrusivePtr;

    template<class U>
    friend class TActionTargetTraits;

    T* T_;
};

////////////////////////////////////////////////////////////////////////////////

template<class T>
bool operator<(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return lhs.Get() < rhs.Get();
}

template<class T>
bool operator>(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return lhs.Get() > rhs.Get();
}

template<class T, class U>
bool operator==(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs.Get() == rhs.Get();
}

template<class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs.Get() != rhs.Get();
}

template<class T, class U>
bool operator==(const TIntrusivePtr<T>& lhs, U* rhs)
{
    return lhs.Get() == rhs;
}

template<class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, U* rhs)
{
    return lhs.Get() != rhs;
}

template<class T, class U>
bool operator==(T* lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs == rhs.Get();
}

template<class T, class U>
bool operator!=(T* lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs != rhs.Get();
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
