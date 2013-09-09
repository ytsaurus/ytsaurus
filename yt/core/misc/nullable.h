#pragma once

#include "mpl.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TNullHelper
{ };

} // namespace NDetail

typedef int NDetail::TNullHelper::* TNull;

const TNull Null = static_cast<TNull>(nullptr);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNullable
{
public:
    typedef T TValueType;

    TNullable()
        : HasValue_(false)
        , Value_()
    { }

    TNullable(const T& value)
        : HasValue_(true)
        , Value_(value)
    { }

    TNullable(T&& value)
        : HasValue_(true)
        , Value_(std::move(value))
    { }

    TNullable(TNull)
        : HasValue_(false)
    { }

    template <class U>
    TNullable(
        const TNullable<U>& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : HasValue_(other.HasValue_)
        , Value_(other.Value_)
    { }

    template <class U>
    TNullable(
        TNullable<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : HasValue_(other.HasValue_)
        , Value_(std::move(other.Value_))
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
            Assign(std::move(value));
        }
    }

    TNullable& operator=(const T& value)
    {
        Assign(value);
        return *this;
    }

    TNullable& operator=(T&& value)
    {
        Assign(std::move(value));
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
        Value_ = value;
    }

    void Assign(T&& value)
    {
        HasValue_ = true;
        Value_ = std::move(value);
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
            Value_ = other.Value_;
        } else if (hadValue) {
            Value_ = T();
        }
    }

    template <class U>
    void Assign(TNullable<U>&& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");

        TNullable<T>(std::move(other)).Swap(*this);
    }

    void Reset()
    {
        HasValue_ = false;
        Value_ = T();
    }

    void Swap(TNullable& other)
    {
        if (!HasValue_ && !other.HasValue_) {
            return;
        }

        DoSwap(HasValue_, other.HasValue_);
        DoSwap(Value_, other.Value_);
    }


    bool HasValue() const
    {
        return HasValue_;
    }

    const T& Get() const
    {
        YASSERT(HasValue_);
        return Value_;
    }

    T& Get()
    {
        YASSERT(HasValue_);
        return Value_;
    }

    const T& Get(const T& defaultValue) const
    {
        return HasValue_ ? Value_ : defaultValue;
    }

    const T* GetPtr() const
    {
        return HasValue_ ? &Value_ : nullptr;
    }

    T* GetPtr()
    {
        return HasValue_ ? &Value_ : nullptr;
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
        return HasValue_ ? &TNullable::Value_ : nullptr;
    }

private:
    template <class U>
    friend class TNullable;

    bool HasValue_;
    T Value_;

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
    return TNullable< typename NMpl::TDecay<T>::TType >(std::forward<T>(value));
}

template <class T>
TNullable< typename NMpl::TDecay<T>::TType > MakeNullable(bool condition, T&& value)
{
    return TNullable< typename NMpl::TDecay<T>::TType >(condition, std::forward<T>(value));
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

struct TNullableSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& nullable)
    {
        using NYT::Save;
        Save(context, nullable.HasValue());
        if (nullable) {
            Save(context, *nullable);
        }
    }

    template <class T, class C>
    static void Load(C& context, T& nullable)
    {
        using NYT::Load;
        bool hasValue = Load<bool>(context);
        if (hasValue) {
            typename T::TValueType temp;
            Load(context, temp);
            nullable.Assign(std::move(temp));
        } else {
            nullable.Reset();
        }
    }
};

template <class T, class C>
struct TSerializerTraits<TNullable<T>, C, void>
{
    typedef TNullableSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


