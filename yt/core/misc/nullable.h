#pragma once

#include "intrusive_ptr.h"
#include "mpl.h"

#include <util/string/cast.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TNull
{
    struct TNullInitializer
    { };
    constexpr TNull(TNullInitializer)
    { };
};

constexpr TNull Null{TNull::TNullInitializer{}};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNullable
{
public:
    static_assert(
        !std::is_reference<T>::value,
        "Cannot use TNullable<T> with reference types");

    typedef T TValueType;

    TNullable() noexcept
        : HasValue_(false)
    { }

    TNullable(TNull) noexcept
        : HasValue_(false)
    { }

    TNullable(const T& value) noexcept(std::is_nothrow_copy_constructible<T>::value)
#ifndef NDEBUG
        : HasValue_(false)
#endif
    {
        Construct(value);
    }

    TNullable(T&& value) noexcept(std::is_nothrow_move_constructible<T>::value)
#ifndef NDEBUG
        : HasValue_(false)
#endif
    {
        Construct(std::move(value));
    }

    TNullable(const TNullable& other) noexcept(std::is_nothrow_copy_constructible<T>::value)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(other.Get());
        }
    }

    template <class U>
    TNullable(
        const TNullable<U>& other,
        typename std::enable_if<std::is_constructible<T, U>::value, int>::type = 0)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(other.Get());
        }
    }

    TNullable(TNullable&& other) noexcept(std::is_nothrow_move_constructible<T>::value)
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
        typename std::enable_if<std::is_constructible<T, U>::value, int>::type = 0)
        : HasValue_(false)
    {
        if (other.HasValue_) {
            Construct(std::move(other.Get()));
            other.Reset();
        }
    }

    TNullable(bool condition, const T& value) noexcept(std::is_nothrow_copy_constructible<T>::value)
        : HasValue_(false)
    {
        if (condition) {
            Construct(value);
        }
    }

    TNullable(bool condition, T&& value) noexcept(std::is_nothrow_move_constructible<T>::value)
        : HasValue_(false)
    {
        if (condition) {
            Construct(std::move(value));
        }
    }

    ~TNullable()
    {
        Reset();
    }

    TNullable& operator=(TNull)
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
        static_assert(
            std::is_assignable<
                typename std::add_lvalue_reference<T>::type,
                typename std::add_lvalue_reference<U>::type
            >::value,
            "U& have to be assignable to T");
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
        static_assert(
            std::is_assignable<
                typename std::add_lvalue_reference<T>::type,
                typename std::add_rvalue_reference<U>::type
            >::value,
            "U&& have to be assignable to T");
        Assign(std::move(other));
        return *this;
    }

    explicit operator bool() const
    {
        return HasValue_;
    }

    bool operator==(const TNullable& other)
    {
        return HasValue() && other.HasValue()
            ? Get() == other.Get()
            : HasValue() == other.HasValue();
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
        static_assert(
            std::is_assignable<
                typename std::add_lvalue_reference<T>::type,
                typename std::add_lvalue_reference<U>::type
            >::value,
            "U& have to be assignable to T");
        if (other.HasValue_) {
            Assign(other.Get());
        } else {
            Reset();
        }
    }

    template <class U>
    void Assign(TNullable<U>&& other)
    {
        static_assert(
            std::is_assignable<
                typename std::add_lvalue_reference<T>::type,
                typename std::add_rvalue_reference<U>::type
            >::value,
            "U&& have to be assignable to T");
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
        Y_ASSERT(HasValue_);
        return reinterpret_cast<const T&>(Storage_);
    }

    T& Get()
    {
        Y_ASSERT(HasValue_);
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
        return &Get();
    }

    T* operator->()
    {
        return &Get();
    }

private:
    template <class U>
    friend class TNullable;

    bool HasValue_;
    typename std::aligned_storage<sizeof(T), alignof(T)>::type Storage_;

    template <class... As>
    void Construct(As&&... as)
    {
        Y_ASSERT(!HasValue_);
        new (&Storage_) TValueType(std::forward<As>(as)...);
        HasValue_ = true;
    }

    void Destruct()
    {
        Y_ASSERT(HasValue_);
        Get().~TValueType();
        HasValue_ = false;
    }
};

template <class T>
TString ToString(const TNullable<T>& nullable)
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
TNullable< typename std::decay<T>::type > MakeNullable(T&& value)
{
    return TNullable< typename std::decay<T>::type >(std::forward<T>(value));
}

template <class T>
TNullable< typename std::decay<T>::type > MakeNullable(bool condition, T&& value)
{
    return TNullable< typename std::decay<T>::type >(condition, std::forward<T>(value));
}

template <class T>
TNullable< typename std::decay<T>::type > MakeNullable(const T* ptr)
{
    return ptr
        ? TNullable< typename std::decay<T>::type >(*ptr)
        : TNullable< typename std::decay<T>::type >();
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

} // namespace NYT

template <class T>
struct THash<NYT::TNullable<T>>
{
    size_t operator()(const NYT::TNullable<T>& nullable) const
    {
        return nullable ? hash<T>()(*nullable) : 0;
    }
};
