#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableManagerConfig
    : public NYTree::TYsonStruct
{
public:
    i64 MaxSchemaMemoryUsageToLog;

    // COMPAT(cherepashka, aleksandra-zh).
    bool MakeSchemaAttributeOpaque;

    REGISTER_YSON_STRUCT(TDynamicTableManagerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableManagerConfig)

class TTableManagerConfig
    : public NYTree::TYsonStruct
{
public:

    REGISTER_YSON_STRUCT(TTableManagerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
