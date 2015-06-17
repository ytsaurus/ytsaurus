#pragma once

#include "public.h"

#include <core/ytree/public.h>

#include <core/misc/function_traits.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

NYTree::TYsonString DoRunTool(const Stroka& toolName, const NYTree::TYsonString& serializedArgument);
NYTree::TYsonString DoRunToolInProcess(const Stroka& toolName, const NYTree::TYsonString& serializedArgument);

////////////////////////////////////////////////////////////////////////////////

template <
    typename TTool,
    typename TArg = typename TFunctionTraits<TTool>::TArg,
    typename TResult = typename TFunctionTraits<TTool>::TResult>
TResult RunTool(
    const TArg& arg,
    std::function<NYTree::TYsonString(const Stroka&, const NYTree::TYsonString&)> invoker = DoRunTool);

////////////////////////////////////////////////////////////////////////////////

NYTree::TYsonString ExecuteTool(const Stroka& toolName, const NYTree::TYsonString& serializedArgument);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TOOLS_INL_H_
#include "tools-inl.h"
#undef TOOLS_INL_H_
