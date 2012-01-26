#pragma once

#include "rvalue.h"
#include <util/generic/utility.h>

namespace NYT {

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
    
    template<class U>
    TNullable(const TNullable<U>& other)
        : Initialized(other.Initialized)
        , Value(other.Value)
    { }

    template<class U>
    TNullable(TNullable<U>&& other)
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

    template <class U>
    TNullable& operator=(const TNullable<U>& other)
    {
        Assign(other);
        return *this;
    }

    template <class U>
    TNullable& operator=(TNullable<U>&& other)
    {
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

    template <class U>
    void Assign(const TNullable<U>& other)
    {
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

    const T* GetPtr() const
    {
        return Initialized ? &Value : NULL;
    }

    T* GetPtr()
    {
        return Initialized ? &Value : NULL;
    }

    const T& GetValueOrDefault(const T& defaultValue = T()) const
    {
        return Initialized ? Value : defaultValue;
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

private:
    template <class U>
    friend class TNullable;

    bool Initialized;
    T Value;
};


template <class T>
TNullable<T> MakeNullable(const T& value)
{
    return TNullable<T>(value);
}

template <class T>
TNullable<T> MakeNullable(T&& value)
{
    return TNullable<T>(MoveRV(value));
}

template <class T>
TNullable<T> MakeNullable(bool condition, const T& value)
{
    return TNullable<T>(condition, value);
}

template <class T>
TNullable<T> MakeNullable(bool condition, T&& value)
{
    return TNullable<T>(condition, MoveRV(value));
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


} //namespace NYT
