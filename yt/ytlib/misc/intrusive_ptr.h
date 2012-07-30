#pragma once

#include "assert.h"
#include "mpl.h"
#include "rvalue.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

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

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
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
        TIntrusivePtr(MoveRV(other)).Swap(*this);
        return *this;
    }

    //! Move assignment operator with an upcast.
    template <class U>
    TIntrusivePtr& operator=(TIntrusivePtr<U>&& other) // noexcept
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
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
