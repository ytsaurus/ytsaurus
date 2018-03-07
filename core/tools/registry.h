#pragma once

#include "public.h"

#include <yt/core/yson/public.h>
#include <yt/core/yson/string.h>
#include <yt/core/ytree/public.h>

#include <typeinfo>

namespace NYT {
namespace NTools {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<NYson::TYsonString(const NYson::TYsonString&)> TGenericTool;

struct TToolRegistryEntry
{
    TString Name;
    TGenericTool Tool;
};

typedef std::map<TString, TToolRegistryEntry> TToolRegistry;

TToolRegistry* GetToolRegistry();

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_TOOL(toolType) \
    static const ::NYT::NTools::NDetail::TToolRegistrator<toolType> toolType##_Registrator \
        (PP_STRINGIZE(toolType));

////////////////////////////////////////////////////////////////////////////////

} // namespace NTools
} // namespace NYT

#define REGISTRY_INL_H_
#include "registry-inl.h"
#undef REGISTRY_INL_H_
