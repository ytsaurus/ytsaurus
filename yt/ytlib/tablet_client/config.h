#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public TYsonSerializable
{
public:
    int MaxVersions;

    int ValueCountMemoryCompactionThreshold;
    i64 StringSpaceMemoryCompactionThreshold;

    TTableMountConfig()
    {
        RegisterParameter("max_versions", MaxVersions)
            .Default(16)
            .GreaterThan(0)
            .LessThanOrEqual(65535);

        RegisterParameter("value_count_memory_compaction_threshold", ValueCountMemoryCompactionThreshold)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("string_space_memory_compaction_threshold", StringSpaceMemoryCompactionThreshold)
            .GreaterThan(0)
            .Default((i64) 100 * 1024 * 1024);
    }
};

class TTableMountCacheConfig
    : public TYsonSerializable
{
public:
    TDuration SuccessExpirationTime;
    TDuration FailureExpirationTime;

    TTableMountCacheConfig()
    {
        RegisterParameter("success_expiration_time", SuccessExpirationTime)
            .Default(TDuration::Seconds(60));
        RegisterParameter("failure_expiration_time", FailureExpirationTime)
            .Default(TDuration::Seconds(5));
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
