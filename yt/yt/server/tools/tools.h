#pragma once

#include "public.h"
#include "registry.h"

#include <yt/yt/core/misc/function_traits.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString DoRunTool(const TString& toolName, const NYson::TYsonString& serializedArgument);
NYson::TYsonString DoRunToolInProcess(const TString& toolName, const NYson::TYsonString& serializedArgument);

////////////////////////////////////////////////////////////////////////////////

template <
    typename TTool,
    typename TArg = typename TFunctionTraits<TTool>::TArg,
    typename TResult = typename TFunctionTraits<TTool>::TResult
>
TResult RunTool(
    const TArg& arg,
    std::function<NYson::TYsonString(const TString&, const NYson::TYsonString&)> invoker = DoRunTool);

template <
    typename TTool,
    typename TArg = typename TFunctionTraits<TTool>::TArg
>
std::vector<TString> GenerateToolArguments(const TArg& arg);

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString ExecuteTool(const TString& toolName, const NYson::TYsonString& serializedArgument);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools

#define TOOLS_INL_H_
#include "tools-inl.h"
#undef TOOLS_INL_H_
