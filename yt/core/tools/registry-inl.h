#ifndef REGISTRY_INL_H_
#error "Direct inclusion of this file is not allowed, include registry.h"
#endif

#include <core/ytree/convert.h>

#include <core/misc/function_traits.h>

namespace NYT {
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
            auto error = TError("Failed to parse argument %Qv", serializedArg.Data())
                << ex;
            return NYTree::ConvertToYsonString(TErrorOr<TResult>(error));
        }

        TErrorOr<TResult> result = func(arg);

        try {
            return NYTree::ConvertToYsonString(result);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to convert result")
                << ex;
            return NYTree::ConvertToYsonString(TErrorOr<TResult>(error));
        }
    };
}

template <typename T, typename TArg = typename TFunctionTraits<T>::TArg, typename TResult = typename TFunctionTraits<T>::TResult>
TGenericTool MakeGeneric(T internal)
{
    return Ysonize<
        typename std::decay<TArg>::type,
        typename std::decay<TResult>::type>(internal);
}

template <typename TTool, const char* Name>
const TToolRegistryEntry& RegisterTool()
{
    static const TToolRegistryEntry entry(typeid(TTool).name(), Stroka(Name), MakeGeneric(TTool()));
    return entry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
