#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
#endif
#undef ERROR_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TSignature>
struct TGuardedAsyncViaHelper;

template <class U, class... TArgs>
struct TGuardedAsyncViaHelper<U(TArgs...)>
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
struct TGuardedAsyncViaHelper<void(TArgs...)>
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
TCallback<TFuture<TErrorOr<U>>(TArgs...)>
TCallback<U(TArgs...)>::GuardedAsyncVia(TIntrusivePtr<IInvoker> invoker)
{
    return NYT::NDetail::TGuardedAsyncViaHelper<
        U(TArgs...)
    >::Do(*this).AsyncVia(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
