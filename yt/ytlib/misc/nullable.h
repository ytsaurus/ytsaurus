#pragma once

#include "rvalue.h"

#include <util/generic/utility.h>
#include <util/string/cast.h>
#include <util/ysaveload.h>

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
        : HasValue_(false)
    { }

    TNullable(const T& value)
        : HasValue_(true)
        , Value(value)
    { }

    TNullable(T&& value)
        : HasValue_(true)
        , Value(MoveRV(value))
    { }

    TNullable(TNull)
        : HasValue_(false)
    { }
    
    template <class U>
    TNullable(
        const TNullable<U>& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : HasValue_(other.HasValue_)
        , Value(other.Value)
    { }

    template <class U>
    TNullable(
        TNullable<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : HasValue_(other.HasValue_)
        , Value(MoveRV(other.Value))
    { }
    
    TNullable(bool condition, const T& value)
        : HasValue_(false)
    {
        if (condition) {
            Assign(value);
        }
    }

    TNullable(bool condition, T&& value)
        : HasValue_(false)
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
        HasValue_ = true;
        Value = value;
    }

    void Assign(T&& value)
    {
        HasValue_ = true;
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
        
        bool hadValue = HasValue_;
        HasValue_ = other.HasValue_;
        if (other.HasValue_) {
            Value = other.Value;
        } else if (hadValue) {
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
        HasValue_ = false;
        Value = T();
    }

    void Swap(TNullable& other)
    {
        if (!HasValue_ && !other.HasValue_) {
            return;
        }
        
        DoSwap(HasValue_, other.HasValue_);
        DoSwap(Value, other.Value);
    }


    bool HasValue() const
    {
        return HasValue_;
    }

    const T& Get() const
    {
        YASSERT(HasValue_);
        return Value;
    }

    T& Get()
    {
        YASSERT(HasValue_);
        return Value;
    }

    const T& Get(const T& defaultValue) const
    {
        return HasValue_ ? Value : defaultValue;
    }

    const T* GetPtr() const
    {
        return HasValue_ ? &Value : NULL;
    }

    T* GetPtr()
    {
        return HasValue_ ? &Value : NULL;
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
        return HasValue_ ? &TNullable::Value : NULL;
    }

private:
    template <class U>
    friend class TNullable;

    bool HasValue_;
    T Value;
};

template <class T>
Stroka ToString(const NYT::TNullable<T>& nullable)
{
    using ::ToString;
    return nullable ? ToString(*nullable) : "<Null>";
}

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
TNullable< typename NMpl::TDecay<T>::TType > MakeNullable(const T* ptr)
{
    return ptr
        ? TNullable< typename NMpl::TDecay<T>::TType >(*ptr)
        : TNullable< typename NMpl::TDecay<T>::TType >(Null);
}

template <class T>
bool operator==(const TNullable<T>& lhs, const TNullable<T>& rhs)
{
    if (!lhs.HasValue() && !rhs.HasValue()) {
        return true;
    }
    if (!lhs.HasValue() || !rhs.HasValue()) {
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
    if (!lhs.HasValue()) {
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
    if (!rhs.HasValue()) {
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

template <class T>
void Save(TOutputStream* output, const TNullable<T>& obj)
{
    using ::Save;
    Save(output, obj.HasValue());
    if (obj.HasValue()) {
        Save(output, obj.Get());
    }
}

template <class T>
void Load(TInputStream* input, TNullable<T>& obj)
{
    using ::Load;
    /*
    bool hasValue;
    Load(input, hasValue);
    if (hasValue) {
        T temp;
        Load(input, temp);
        obj = MoveRV(temp);
    }*/
    // XXX(babenko): oh Ignat!
    T temp;
    Load(input, temp);
    obj = MoveRV(temp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


