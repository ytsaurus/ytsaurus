#ifndef TOOLS_INL_H_
#error "Direct inclusion of this file is not allowed, include tools.h"
// For the sake of sane code completion.
#include "tools.h"
#endif

#include <yt/yt/core/ytree/convert.h>

#include "registry.h"

#include <typeinfo>

namespace NYT::NTools {

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
    static void Extract(const TError&)
    { }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <
  typename TTool,
  typename TArg,
  typename TResult
>
TResult RunTool(
    const TArg& arg,
    std::function<NYson::TYsonString(const TString&, const NYson::TYsonString&)> invoker)
{
    auto name = typeid(TTool).name();

    NYson::TYsonString serializedArgument;
    try {
        serializedArgument = NYson::ConvertToYsonString(arg, NYson::EYsonFormat::Text);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to serialize argument for tool %v", name)
            << ex;
    }

    auto serializedResultOrError = invoker(name, serializedArgument);

    TErrorOr<TResult> resultOrError;

    try {
        resultOrError = NYTree::ConvertTo<TErrorOr<TResult>>(serializedResultOrError);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse result of tool %v",
            name)
            << TErrorAttribute("result", serializedResultOrError.AsStringBuf())
            << ex;
    }

    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error occurred during tool run");

    return NDetail::TExtractValueHelper<TResult>::Extract(resultOrError);
}

////////////////////////////////////////////////////////////////////////////////

template <
    typename TTool,
    typename TArg
>
std::vector<TString> GenerateToolArguments(const TArg& arg)
{
    auto name = typeid(TTool).name();

    NYson::TYsonString serializedArgument;
    try {
        serializedArgument = NYson::ConvertToYsonString(arg, NYson::EYsonFormat::Text);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to serialize argument for tool %v", name)
            << ex;
    }

    return std::vector<TString>{
        "--tool-name",
        name,
        "--tool-spec",
        serializedArgument.ToString()
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
