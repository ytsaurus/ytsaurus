#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableManagerConfig
    : public NYTree::TYsonStruct
{
    i64 MaxSchemaMemoryUsageToLog;

    // COMPAT(cherepashka, aleksandra-zh).
    bool MakeSchemaAttributeOpaque;
    // COMPAT(babenko)
    std::vector<std::string> NonOpaqueSchemaAttributeUserWhitelist;

    REGISTER_YSON_STRUCT(TDynamicTableManagerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableManagerConfig)

struct TTableManagerConfig
    : public NYTree::TYsonStruct
{

    REGISTER_YSON_STRUCT(TTableManagerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
