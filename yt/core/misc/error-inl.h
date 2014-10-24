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
    : Code_(EErrorCode::Generic)
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
