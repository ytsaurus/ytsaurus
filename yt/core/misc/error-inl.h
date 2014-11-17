#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
#endif
#undef ERROR_INL_H_

#include <core/misc/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TInner, class... TArgs>
bool IsOK(const TInner& inner, TArgs&&... args)
{
    return inner.IsOK();
}

template <class TInner, class... TArgs>
TError WrapError(const TInner& inner, TArgs&&... args)
{
    return TError(std::forward<TArgs>(args)...) << inner;
}

template <class TInner>
TError WrapError(const TInner& inner)
{
    return inner;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
TError::TErrorOr(const char* format, const TArgs&... args)
    : Code_(NYT::EErrorCode::Generic)
    , Message_(Format(format, args...))
{
    CaptureOriginAttributes();
}

template <class... TArgs>
TError::TErrorOr(int code, const char* format, const TArgs&... args)
    : Code_(code)
    , Message_(Format(format, args...))
{
    if (!IsOK()) {
        CaptureOriginAttributes();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T>::TErrorOr()
    : Value_()
{ }

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
    , Value_()
{ }

template <class T>
TErrorOr<T>::TErrorOr(const TErrorOr<T>& other)
    : TError(other)
    , Value_(other.Value_)
{ }

template <class T>
template <class TOther>
TErrorOr<T>::TErrorOr(const TErrorOr<TOther>& other)
    : TError(other)
    , Value_(other.Value())
{ }

template <class T>
TErrorOr<T>::TErrorOr(const std::exception& ex)
    : TError(ex)
{ }

template <class T>
template <class U>
TErrorOr<U> TErrorOr<T>::As() const
{
    if (IsOK()) {
        return static_cast<U>(Value_);
    } else {
        return TError(*this);
    }
}

template <class T>
T& TErrorOr<T>::ValueOrThrow()
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
    return Value_;
}

template <class T>
const T& TErrorOr<T>::ValueOrThrow() const
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
    return Value_;
}

template <class T>
T& TErrorOr<T>::Value()
{
    YCHECK(IsOK());
    return Value_;
}

template <class T>
const T& TErrorOr<T>::Value() const
{
    YCHECK(IsOK());
    return Value_;
}

template <class T>
Stroka ToString(const TErrorOr<T>& valueOrError)
{
    return ToString(TError(valueOrError));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TSignature>
struct TGuardedHelper;

template <class U, class... TArgs>
struct TGuardedHelper<U(TArgs...)>
{
    static TCallback<TErrorOr<U>(TArgs...)> Do(TCallback<U(TArgs...)> callback)
    {
        return
            BIND([=] (TArgs... args) -> TErrorOr<U> {
                try {
                    return TErrorOr<U>(callback.Run(std::forward<TArgs>(args)...));
                } catch (const std::exception& ex) {
                    return ex;
                }
            });
    }
};

template <class... TArgs>
struct TGuardedHelper<void(TArgs...)>
{
    static TCallback<TErrorOr<void>(TArgs...)> Do(TCallback<void(TArgs...)> callback)
    {
        return
            BIND([=] (TArgs... args) -> TErrorOr<void> {
                try {
                    callback.Run(std::forward<TArgs>(args)...);
                    return TErrorOr<void>();
                } catch (const std::exception& ex) {
                    return ex;
                }
            });
    }
};

} // namespace NDetail

template <class U, class... TArgs>
TCallback<TErrorOr<U>(TArgs...)>
TCallback<U(TArgs...)>::Guarded()
{
    return NYT::NDetail::TGuardedHelper<U(TArgs...)>::Do(*this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
