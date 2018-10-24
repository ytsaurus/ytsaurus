#pragma once
#ifndef REGISTRY_INL_H_
#error "Direct inclusion of this file is not allowed, include registry.h"
// For the sake of sane code completion
#include "registry.h"
#endif

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/function_traits.h>

namespace NYT {
namespace NTools {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TArg, class TResult>
TGenericTool Ysonize(std::function<TResult(const TArg&)> internal)
{
    return [internal] (const NYson::TYsonString& serializedArg) {
        auto func = TTrapExceptionHelper<TArg, TResult>::Trap(internal);

        TArg arg;
        try {
            arg = NYTree::ConvertTo<TArg>(serializedArg);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to parse argument %Qv", serializedArg.GetData())
                << ex;
            return NYTree::ConvertToYsonString(TErrorOr<TResult>(error), NYson::EYsonFormat::Text);
        }

        TErrorOr<TResult> result = func(arg);

        try {
            return NYTree::ConvertToYsonString(result, NYson::EYsonFormat::Text);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to convert result")
                << ex;
            return NYTree::ConvertToYsonString(TErrorOr<TResult>(error), NYson::EYsonFormat::Text);
        }
    };
}

template <
    typename TFunc,
    typename TArg = typename TFunctionTraits<TFunc>::TArg,
    typename TResult = typename TFunctionTraits<TFunc>::TResult
>
TGenericTool MakeGeneric(TFunc internal)
{
    return Ysonize<
        typename std::decay<TArg>::type,
        typename std::decay<TResult>::type>(internal);
}

template <class TTool>
struct TToolRegistrator
{
    explicit TToolRegistrator(const TString& toolName)
    {
        auto typeName = TString(typeid(TTool).name());
        auto tool = MakeGeneric(TTool());
        TToolRegistryEntry entry{toolName, tool};
        auto* registry = GetToolRegistry();
        YCHECK(registry->emplace(typeName, entry).second);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTools
} // namespace NDetail
} // namespace NYT
