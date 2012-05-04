#pragma once

#include "rvalue.h"

#include <util/generic/utility.h>
#include <util/string/cast.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TNullHelper
{ };

} // namespace NDetail

typedef int NDetail::TNullHelper::* TNull;

const TNull Null = static_cast<TNull>(NULL);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNullable
{
public:
    typedef T TValueType;

    TNullable()
        : Initialized(false)
    { }

    TNullable(const T& value)
        : Initialized(true)
        , Value(value)
    { }

    TNullable(T&& value)
        : Initialized(true)
        , Value(MoveRV(value))
    { }

    TNullable(TNull)
        : Initialized(false)
    { }
    
    template <class U>
    TNullable(
        const TNullable<U>& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : Initialized(other.Initialized)
        , Value(other.Value)
    { }

    template <class U>
    TNullable(
        TNullable<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : Initialized(other.Initialized)
        , Value(MoveRV(other.Value))
    { }
    
    TNullable(bool condition, const T& value)
        : Initialized(false)
    {
        if (condition) {
            Assign(value);
        }
    }

    TNullable(bool condition, T&& value)
        : Initialized(false)
    {
        if (condition) {
            Assign(MoveRV(value));
        }
    }

    TNullable& operator=(const T& value)
    {
        Assign(value);
        return *this;
    }

    TNullable& operator=(T&& value)
    {
        Assign(MoveRV(value));
        return *this;
    }

    TNullable& operator=(TNull value)
    {
        Assign(value);
        return *this;
    }

    template <class U>
    TNullable& operator=(const TNullable<U>& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");
        Assign(other);
        return *this;
    }

    template <class U>
    TNullable& operator=(TNullable<U>&& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");
        Assign(other);
        return *this;
    }
    
    void Assign(const T& value)
    {
        Initialized = true;
        Value = value;
    }

    void Assign(T&& value)
    {
        Initialized = true;
        Value = MoveRV(value);
    }

    void Assign(TNull)
    {
        Reset();
    }

    template <class U>
    void Assign(const TNullable<U>& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");
        
        bool wasInitalized = Initialized;
        Initialized = other.Initialized;
        if (other.Initialized) {
            Value = other.Value;
        } else if (wasInitalized) {
            Value = T();
        }
    }

    template <class U>
    void Assign(TNullable<U>&& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");

        TNullable<T>(MoveRV(other)).Swap(*this);
    }

    void Reset()
    {
        Initialized = false;
        Value = T();
    }

    void Swap(TNullable& other)
    {
        if (!Initialized && !other.Initialized) {
            return;
        }
        
        DoSwap(Initialized, other.Initialized);
        DoSwap(Value, other.Value);
    }


    bool IsInitialized() const
    {
        return Initialized;
    }

    const T& Get() const
    {
        YASSERT(Initialized);
        return Value;
    }

    T& Get()
    {
        YASSERT(Initialized);
        return Value;
    }

    const T& Get(const T& defaultValue) const
    {
        return Initialized ? Value : defaultValue;
    }

    const T* GetPtr() const
    {
        return Initialized ? &Value : NULL;
    }

    T* GetPtr()
    {
        return Initialized ? &Value : NULL;
    }

    const T& operator*() const
    {
        return Get();
    }

    T& operator*()
    {
        return Get();
    }

    const T* operator->() const
    {
        return GetPtr();
    }

    T* operator->()
    {
        return GetPtr();
    }

    // Implicit conversion to bool.
    typedef T TNullable::*TUnspecifiedBoolType;
    operator TUnspecifiedBoolType() const
    {
        return Initialized ? &TNullable::Value : NULL;
    }

    Stroka ToString() const
    {
        return Initialized ? ::ToString(Value) : "<NULL>";
    }

private:
    template <class U>
    friend class TNullable;

    bool Initialized;
    T Value;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TNullableTraits
{
    typedef TNullable<T> TNullableType;
    typedef T TValueType;
};

template <class T>
struct TNullableTraits< TNullable<T> >
{
    typedef TNullable<T> TNullableType;
    typedef T TValueType;
};

template <class T>
struct TNullableTraits<T*>
{
    typedef T* TNullableType;
    typedef T* TValueType;
};

template <class T>
struct TNullableTraits< TIntrusivePtr<T> >
{
    typedef TIntrusivePtr<T> TNullableType;
    typedef TIntrusivePtr<T> TValueType;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TNullable< typename NMpl::TDecay<T>::TType > MakeNullable(T&& value)
{
    return TNullable< typename NMpl::TDecay<T>::TType >(ForwardRV<T>(value));
}

template <class T>
TNullable< typename NMpl::TDecay<T>::TType > MakeNullable(bool condition, T&& value)
{
    return TNullable< typename NMpl::TDecay<T>::TType >(condition, ForwardRV<T>(value));
}

template <class T>
bool operator==(const TNullable<T>& lhs, const TNullable<T>& rhs)
{
    if (!lhs.IsInitialized() && !rhs.IsInitialized()) {
        return true;
    }
    if (!lhs.IsInitialized() || !rhs.IsInitialized()) {
        return false;
    }
    return *lhs == *rhs;
}

template <class T>
bool operator!=(const TNullable<T>& rhs, const TNullable<T>& lhs)
{
    return !(lhs == rhs);
}

template <class T>
bool operator==(const TNullable<T>& lhs, const T& rhs)
{
    if (!lhs.IsInitialized()) {
        return false;
    }
    return *lhs == rhs;
}

template <class T>
bool operator!=(const TNullable<T>& rhs, const T& lhs)
{
    return !(lhs == rhs);
}

template <class T>
bool operator==(const T& lhs, const TNullable<T>& rhs)
{
    if (!rhs.IsInitialized()) {
        return false;
    }
    return lhs == *rhs;
}

template <class T>
bool operator!=(const T& rhs, const TNullable<T>& lhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

//template <class T>
//Stroka ToString(const TNullable<T>& nullable)
//{
//    return nullable ? ToString(*nullable) : "<NULL>";
//}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
