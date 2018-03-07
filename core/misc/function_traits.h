#pragma once

#include <yt/core/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TFunctionTraits
    : public TFunctionTraits<decltype(&T::operator())>
{};

template <typename T, typename Result, typename Arg>
struct TFunctionTraits<Result(T::*)(Arg) const>
{
    typedef Result TResult;
    typedef Arg TArg;
};

////////////////////////////////////////////////////////////////////////////////

template <class TArg, class TResult>
struct TTrapExceptionHelper
{
    static std::function<TErrorOr<TResult>(const TArg&)> Trap(std::function<TResult(const TArg&)> func)
    {
        return [=] (const TArg& arg) {
            try {
                return TErrorOr<TResult>(func(arg));
            } catch (const std::exception& ex) {
                return TErrorOr<TResult>(ex);
            }
        };
    }
};

template <class TArg>
struct TTrapExceptionHelper<TArg, void>
{
    static std::function<TError(const TArg&)> Trap(std::function<void(const TArg&)> func)
    {
        return [=] (const TArg& arg) {
            try {
                func(arg);
                return TError();
            } catch (const std::exception& ex) {
                return TError(ex);
            }
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
