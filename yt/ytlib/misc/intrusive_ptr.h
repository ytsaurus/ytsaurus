#pragma once

#include <util/system/yassert.h>
#include <util/system/defaults.h>

#include <util/generic/ptr.h>

// Implementation was forked from util/generic/ptr.h.

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! TIsConvertible<U, T>::Value is True iff #S is convertable to #T.
template<class U, class T>
struct TIsConvertible
{
    typedef char (&TYes)[1];
    typedef char (&TNo) [2];

    static TYes f(T*);
    static TNo  f(...);

    enum
    {
        Value = sizeof( (f)(static_cast<U*>(0)) ) == sizeof(TYes)
    };
};

struct TEmpty
{ };

template<bool>
struct TEnableIfConvertibleImpl;

template<>
struct TEnableIfConvertibleImpl<true>
{
    typedef TEmpty TType;
};

template<>
struct TEnableIfConvertibleImpl<false>
{ };

template<class U, class T>
struct TEnableIfConvertible
    : public TEnableIfConvertibleImpl< TIsConvertible<U, T>::Value >
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct TIntrusivePtrTraits
{
    static void Ref(T *p)
    {
        p->Ref();
    }

    static void UnRef(T *p)
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

template<class T>
class TIntrusivePtr
{
public:
    TIntrusivePtr() throw()
        : T_(NULL)
    { }

    TIntrusivePtr(T* p, bool addReference = true) throw()
        : T_(p)
    {
        if (T_ != 0 && addReference) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }
    }

    //! Copy constructor.
    TIntrusivePtr(const TIntrusivePtr& other) throw()
        : T_(other.T_)
    {
        if (T_ != 0) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }
    }

    //! Copy constructor with an implicit cast between Convertible classes.
    template<class U>
    TIntrusivePtr(
        const TIntrusivePtr<U>& other,
        typename NDetail::TEnableIfConvertible<U, T>::TType = NDetail::TEmpty())
        throw()
        : T_(other.Get())
    {
        if (T_ != 0) {
            TIntrusivePtrTraits<T>::Ref(T_);
        }
    }

    //! Move constructor.
    TIntrusivePtr(TIntrusivePtr&& other) throw()
        : T_(other.T_)
    {
        other.T_ = 0;
    }

    //! Move constructor with an implicit cast between Convertible classes.
    template<class U>
    TIntrusivePtr(
        TIntrusivePtr<U>&& other,
        typename NDetail::TEnableIfConvertible<U, T>::TType = NDetail::TEmpty())
        throw()
        : T_(other.Get())
    {
        other.T_ = NULL;
    }

    //! Destructor.
    ~TIntrusivePtr()
    {
        if (T_ != NULL) {
            TIntrusivePtrTraits<T>::UnRef(T_);
        }
    }

    //! Copy assignment operator.
    TIntrusivePtr& operator=(const TIntrusivePtr& other) throw()
    {
        TIntrusivePtr(other).Swap(*this);
        return *this;
    }

    //! Move assignment operator.
    TIntrusivePtr& operator=(TIntrusivePtr&& other) throw()
    {
        TIntrusivePtr(MoveRV(other)).Swap(*this);
        return *this;
    }

    //! Drop the pointer.
    void Reset() throw()
    {
        TIntrusivePtr().Swap(*this);
    }

    //! Replace the pointee with a specified.
    void Reset(T* p) throw()
    {
        TIntrusivePtr(p).Swap(*this);
    }

    //! Returns the pointer.
    T* Get() const throw()
    {
        return T_;
    }

    T& operator*() const throw()
    {
        YASSERT(T_ != 0);
        return *T_;
    }

    T* operator->() const throw()
    {
        YASSERT(T_ != 0);
        return  T_;
    }

    void Swap(TIntrusivePtr& r) throw()
    {
        DoSwap(T_, r.T_);
    }

private:
    template<class U>
    friend class TIntrusivePtr;

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
bool operator==(const TIntrusivePtr<T>& lhs, U * rhs)
{
    return lhs.Get() == rhs;
}

template<class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, U * rhs)
{
    return lhs.Get() != rhs;
}

template<class T, class U>
bool operator==(T * lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs == rhs.Get();
}

template<class T, class U>
bool operator!=(T * lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs != rhs.Get();
}

template<class T>
T* operator ~ (const TIntrusivePtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const TAutoPtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const THolder<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
