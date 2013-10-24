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
    static_assert(
        !NMpl::TIsReference<T>::Value,
        "Cannot use TNullable<T> with reference types");

    typedef T TValueType;

    TNullable()
        : HasValue_(false)
    { }

    TNullable(TNull)
        : HasValue_(false)
    { }

    TNullable(const T& value)
#ifdef DEBUG
        : HasValue_(false)
#endif
    {
        Construct(value);
    }

    TNullable(T&& value)
#ifdef DEBUG
        : HasValue_(false)
#endif
    {
        Construct(std::move(value));
    }

    TNullable(const TNullable& other)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(other.Get());
        }
    }

    template <class U>
    TNullable(
        const TNullable<U>& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(other.Get());
        }
    }

    TNullable(TNullable&& other)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(std::move(other.Get()));
            other.Reset();
        }
    }

    template <class U>
    TNullable(
        TNullable<U>&& other,
        typename NMpl::TEnableIf<NMpl::TIsConvertible<U, T>, int>::TType = 0)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(std::move(other.Get()));
            other.Reset();
        }
    }

    TNullable(bool condition, const T& value)
        : HasValue_(false)
    {
        if (condition) {
            Construct(value);
        }
    }

    TNullable(bool condition, T&& value)
        : HasValue_(false)
    {
        if (condition) {
            Construct(std::move(value));
        }
    }

    TNullable& operator=(TNull value)
    {
        Reset();
        return *this;
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

    TNullable& operator=(const TNullable& other)
    {
        Assign(other);
        return *this;
    }

    template <class U>
    TNullable& operator=(const TNullable<U>& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");
        Assign(other);
        return *this;
    }

    TNullable& operator=(TNullable&& other)
    {
        Assign(std::move(other));
        return *this;
    }

    template <class U>
    TNullable& operator=(TNullable<U>&& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");
        Assign(std::move(other));
        return *this;
    }

    void Assign(TNull)
    {
        Reset();
    }

    void Assign(const T& value)
    {
        if (HasValue_) {
            Get() = value;
        } else {
            Construct(value);
        }
    }

    void Assign(T&& value)
    {
        if (HasValue_) {
            Get() = std::move(value);
        } else {
            Construct(std::move(value));
        }
    }

    template <class U>
    void Assign(const TNullable<U>& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");

        if (other.HasValue_) {
            Assign(other.Get());
        } else {
            Reset();
        }
    }

    template <class U>
    void Assign(TNullable<U>&& other)
    {
        static_assert(NMpl::TIsConvertible<U, T>::Value, "U have to be convertible to T");

        if (other.HasValue_) {
            Assign(std::move(other.Get()));
            other.Reset();
        } else {
            Reset();
        }
    }

    template <class... As>
    void Emplace(As&&... as)
    {
        Reset();
        Construct(std::forward<As>(as)...);
    }

    void Reset()
    {
        if (HasValue_) {
            Destruct();
        }
    }

    void Swap(TNullable& other)
    {
        TNullable tmp = std::move(other);
        other = std::move(*this);
        *this = std::move(tmp);
    }

    bool HasValue() const
    {
        return HasValue_;
    }

    const T& Get() const
    {
        YASSERT(HasValue_);
        return reinterpret_cast<const T&>(Storage_);
    }

    T& Get()
    {
        YASSERT(HasValue_);
        return reinterpret_cast<T&>(Storage_);
    }

    const T& Get(const T& defaultValue) const
    {
        return HasValue_ ? Get() : defaultValue;
    }

    const T* GetPtr() const
    {
        return HasValue_ ? &Get() : nullptr;
    }

    T* GetPtr()
    {
        return HasValue_ ? &Get() : nullptr;
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
    typedef bool TNullable::*TUnspecifiedBoolType;
    operator TUnspecifiedBoolType() const
    {
        return HasValue_ ? &TNullable::HasValue_ : nullptr;
    }
    
    ~TNullable()
    {
        Reset();
    }

private:
    template <class U>
    friend class TNullable;

    bool HasValue_;
    typename std::aligned_storage<sizeof(T), alignof(T)>::type Storage_;

    template <class... As>
    void Construct(As&&... as)
    {
        YASSERT(!HasValue_);
#ifdef _MSC_VER
        new (&Storage_) TValueType(as...);
#else
        new (&Storage_) TValueType(std::forward<As>(as)...);
#endif
        HasValue_ = true;
    }

    void Destruct()
    {
        YASSERT(HasValue_);
        Get().~TValueType();
        HasValue_ = false;
    }
};

template <class T>
Stroka ToString(const TNullable<T>& nullable)
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

