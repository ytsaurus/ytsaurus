#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
#endif
#undef ERROR_INL_H_

#include <core/misc/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TErrorCode::TErrorCode()
    : Value_(static_cast<int>(NYT::EErrorCode::OK))
{ }

inline TErrorCode::TErrorCode(int value)
    : Value_(value)
{ }

template <class E>
TErrorCode::TErrorCode(E value, typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type*)
    : Value_(static_cast<int>(value))
{ }

inline TErrorCode::operator int() const
{
    return Value_;
}

template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator == (E lhs, TErrorCode rhs)
{
    return static_cast<int>(lhs) == static_cast<int>(rhs);
}

template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator != (E lhs, TErrorCode rhs)
{
    return !(lhs == rhs);
}

template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator == (TErrorCode lhs, E rhs)
{
    return static_cast<int>(lhs) == static_cast<int>(rhs);
}

template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator != (TErrorCode lhs, E rhs)
{
    return !(lhs == rhs);
}

inline bool operator == (TErrorCode lhs, TErrorCode rhs)
{
    return static_cast<int>(lhs) == static_cast<int>(rhs);
}

inline bool operator != (TErrorCode lhs, TErrorCode rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
TError::TErrorOr(const char* format, const TArgs&... args)
    : Code_(NYT::EErrorCode::Generic)
    , Message_(Format(format, args...))
{
    CaptureOriginAttributes();
}

template <class... TArgs>
TError::TErrorOr(TErrorCode code, const char* format, const TArgs&... args)
    : Code_(code)
    , Message_(Format(format, args...))
{
    if (!IsOK()) {
        CaptureOriginAttributes();
    }
}

template <class... TArgs>
TError TError::Wrap(TArgs&&... args) const
{
    return TError(std::forward<TArgs>(args)...) << *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T>::TErrorOr()
{
    Value_.Emplace();
}

template <class T>
TErrorOr<T>::TErrorOr(T&& value) noexcept
    : Value_(std::move(value))
{ }

template <class T>
TErrorOr<T>::TErrorOr(const T& value)
    : Value_(value)
{ }

template <class T>
TErrorOr<T>::TErrorOr(const TError& other)
    : TError(other)
{
    YASSERT(!IsOK());
}

template <class T>
TErrorOr<T>::TErrorOr(TError&& other) noexcept
    : TError(std::move(other))
{
    YASSERT(!IsOK());
}

template <class T>
TErrorOr<T>::TErrorOr(const TErrorOr<T>& other)
    : TError(other)
{
    if (IsOK()) {
        Value_ = other.Value();
    }
}

template <class T>
TErrorOr<T>::TErrorOr(TErrorOr<T>&& other) noexcept
    : TError(std::move(other))
{
    if (IsOK()) {
        Value_ = std::move(other.Value());
    }
}

template <class T>
template <class U>
TErrorOr<T>::TErrorOr(const TErrorOr<U>& other)
    : TError(other)
{
    if (IsOK()) {
        Value_ = other.Value();
    }
}

template <class T>
template <class U>
TErrorOr<T>::TErrorOr(TErrorOr<U>&& other) noexcept
    : TError(other)
{
    if (IsOK()) {
        Value_ = std::move(other.Value());
    }
}

template <class T>
TErrorOr<T>::TErrorOr(const std::exception& ex)
    : TError(ex)
{ }

template <class T>
TErrorOr<T>& TErrorOr<T>::operator = (const TErrorOr<T>& other)
{
    static_cast<TError&>(*this) = static_cast<const TError&>(other);
    Value_ = other.Value_;
    return *this;
}

template <class T>
TErrorOr<T>& TErrorOr<T>::operator = (TErrorOr<T>&& other) noexcept
{
    static_cast<TError&>(*this) = std::move(other);
    Value_ = std::move(other.Value_);
    return *this;
}

template <class T>
T& TErrorOr<T>::ValueOrThrow()
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::ValueOrThrow() const
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
    return *Value_;
}

template <class T>
T& TErrorOr<T>::Value()
{
    YASSERT(IsOK());
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::Value() const
{
    YASSERT(IsOK());
    return *Value_;
}

template <class T>
Stroka ToString(const TErrorOr<T>& valueOrError)
{
    return ToString(TError(valueOrError));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
