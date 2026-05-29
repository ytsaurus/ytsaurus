#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableManagerTestingConfig
    : public NYTree::TYsonStructLite
{
    std::optional<TDuration> GetMountInfoDelay;

    REGISTER_YSON_STRUCT_LITE(TDynamicTableManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableManagerConfig
    : public NYTree::TYsonStruct
{
    i64 MaxSchemaMemoryUsageToLog;
    TAsyncExpiringCacheConfigPtr TableSchemaCache;
    TAsyncExpiringCacheConfigPtr YsonTableSchemaCache;
    bool CacheHeavySchemaOnCreation;
    int ColumnToConstraintLogLimit;
    bool EnableColumnConstraintsForTables;

    // COMPAT(h0pless): Remove this in 26.2.
    bool ValidateNoDescendingSortOrder;

    TDynamicTableManagerTestingConfig Testing;

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
