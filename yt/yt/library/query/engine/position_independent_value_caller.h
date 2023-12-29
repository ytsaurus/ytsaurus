#pragma once

#include <yt/yt/core/actions/callback.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TSignature, class TPISignature>
class TCGPICaller;

template <class TResult, class... TArgs, class TPIResult, class... TPIArgs>
class TCGPICaller<TResult(TArgs...), TPIResult(TPIArgs...)>
    : public NYT::NDetail::TBindStateBase
{
public:
    explicit TCGPICaller(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const NYT::TSourceLocation& location,
#endif
        TCallback<TPIResult(TPIArgs...)> callback)
        : NYT::NDetail::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
        )
        , Callback_(std::move(callback))
    { }

    TResult Run(TCallArg<TArgs>... args);

    static auto StaticInvoke(TCallArg<TArgs>... args, NYT::NDetail::TBindStateBase* stateBase)
    {
        auto* state = static_cast<TCGPICaller*>(stateBase);
        state->Run(std::forward<TArgs>(args)...);
    }

private:
    TCallback<TPIResult(TPIArgs...)> Callback_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
