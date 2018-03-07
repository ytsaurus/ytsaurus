#pragma once
#ifndef ANY_INL_H_
#error "Direct inclusion of this file is not allowed, include any.h"
#endif

#include <type_traits>

#include <yt/core/misc/mpl.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TAny::TUntypedBox
{
    virtual ~TUntypedBox()
    { }

    virtual std::unique_ptr<TUntypedBox> Clone() = 0;
    virtual const void* GetRaw() const = 0;
};

template <class T>
struct TAny::TTypedBox
    : public TUntypedBox
{
    explicit TTypedBox(const T& value)
        : Value(value)
    { }

    explicit TTypedBox(T&& value)
        : Value(std::move(value))
    { }

    virtual std::unique_ptr<TUntypedBox> Clone() override
    {
        return std::unique_ptr<TUntypedBox>(new TTypedBox<T>(Value));
    }

    virtual const void* GetRaw() const override
    {
        return static_cast<const void*>(&Value);
    }

    T Value;
};

////////////////////////////////////////////////////////////////////////////////

inline TAny::TAny()
{ }

inline TAny::TAny(TNull)
{ }

inline TAny::TAny(const TAny& other)
{
    AssignAny(other);
}

inline TAny::TAny(TAny&& other)
{
    AssignAny(std::move(other));
}

TAny::operator bool() const
{
    return static_cast<bool>(Box_);
}

template <class T>
TAny::TAny(const T& value)
{
    AssignValue(value);
}

template <class T, class>
TAny::TAny(T&& value)
{
    AssignValue(std::move(value));
}

template <class T>
TAny& TAny::operator=(const T& value)
{
    if (!Box_ || &value != Box_->GetRaw()) {
        AssignValue(value);
    }
    return *this;
}

template <class T, class>
TAny& TAny::operator=(T&& value)
{
    if (!Box_ || &value != Box_->GetRaw()) {
        AssignValue(std::move(value));
    }
    return *this;
}

inline TAny& TAny::operator=(const TAny& other)
{
    if (&other != this) {
        AssignAny(other);
    }
    return *this;
}

inline TAny& TAny::operator=(TAny&& other)
{
    if (&other != this) {
        AssignAny(std::move(other));
    }
    return *this;
}

template <class T>
T& TAny::As()
{
#ifdef NDEBUG
    return dynamic_cast<TTypedBox<T>*>(Box_.get())->Value;
#else
    auto* result = TryAs<T>();
    Y_ASSERT(result);
    return *result;
#endif
}

template <class T>
const T& TAny::As() const
{
#ifdef NDEBUG
    return dynamic_cast<const TTypedBox<T>*>(Box_.get())->Value;
#else
    auto* result = TryAs<T>();
    Y_ASSERT(result);
    return *result;
#endif
}

template <class T>
T* TAny::TryAs()
{
    if (!Box_) {
        return nullptr;
    }
    auto* typedBox = dynamic_cast<TTypedBox<T>*>(Box_.get());
    if (!typedBox) {
        return nullptr;
    }
    return &typedBox->Value;
}

template <class T>
const T* TAny::TryAs() const
{
    if (!Box_) {
        return nullptr;
    }
    auto* typedBox = dynamic_cast<const TTypedBox<T>*>(Box_.get());
    if (!typedBox) {
        return nullptr;
    }
    return &typedBox->Value;
}

template <class T>
bool TAny::Is() const
{
    return TryAs<T>() != nullptr;
}

template <class T>
void TAny::AssignValue(const T& value)
{
    Box_.reset(new TTypedBox<T>(value));
}

template <class T>
void TAny::AssignValue(T&& value)
{
    Box_.reset(new TTypedBox<T>(std::move(value)));
}

inline void TAny::AssignAny(const TAny& other)
{
    if (other) {
        Box_ = other.Box_->Clone();
    } else {
        Box_.reset();
    }
}

inline void TAny::AssignAny(TAny&& other)
{
    Box_ = std::move(other.Box_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
