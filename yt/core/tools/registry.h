#pragma once

#include "public.h"

#include <core/ytree/public.h>

#include <core/yson/public.h>

#include <typeinfo>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<NYson::TYsonString(const NYson::TYsonString&)> TGenericTool;

struct TToolDescription
{
    Stroka Name;
    TGenericTool Tool;
};

typedef std::map<Stroka, TToolDescription> TToolRegistry;

TToolRegistry* GetToolRegistry();

class TToolRegistryEntry
{
public:
    TToolRegistryEntry(Stroka&& typeName, Stroka&& toolName, TGenericTool tool);

};

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_TOOL(toolName) \
    extern const char toolName##Name[] = PP_STRINGIZE(toolName); \
    static const TToolRegistryEntry& toolName##Entry = ::NYT::NDetail::RegisterTool<toolName, toolName##Name>()

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define REGISTRY_INL_H_
#include "registry-inl.h"