#ifndef TOOLS_INL_H_
#error "Direct inclusion of this file is not allowed, include tools.h"
#endif
#undef TOOLS_INL_H_

#include <core/ytree/convert.h>

#include "registry.h"

#include <typeinfo>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename T>
struct TExtractValueHelper
{
    static const T& Extract(const TErrorOr<T>& errorOrValue)
    {
        return errorOrValue.Value();
    }
};

template <>
struct TExtractValueHelper<void>
{
    static void Extract(const TError& )
    { }
};

} // NDetail

////////////////////////////////////////////////////////////////////////////////

template <
  typename TTool,
  typename TArg,
  typename TResult
>
TResult RunTool(
    const TArg& arg,
    std::function<NYTree::TYsonString(const Stroka&, const NYTree::TYsonString&)> invoker)
{
    auto name = typeid(TTool).name();

    NYTree::TYsonString serializedArgument;
    try {
        serializedArgument = NYTree::ConvertToYsonString(arg);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to serialize argument for %v tool", name)
            << ex;
    }

    auto serializedResultOrError = invoker(name, serializedArgument);

    TErrorOr<TResult> resultOrError;

    try {
        resultOrError = NYTree::ConvertTo<TErrorOr<TResult>>(serializedResultOrError);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse result of %v tool (Result: %Qv)",
            name,
            serializedResultOrError.Data())
            << ex;
    }

    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error occured during tool run");

    return NDetail::TExtractValueHelper<TResult>::Extract(resultOrError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT