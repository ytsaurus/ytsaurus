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
    int KeyCountRotationThreshold;
    int ValueCountRotationThreshold;
    i64 AlignedPoolSizeRotationThreshold;
    i64 UnalignedPoolSizeRotationThreshold;

    i64 MaxPartitionDataSize;
    i64 MinPartitionDataSize;

    int MaxPartitionCount;

    i64 EdenPartitioningDataSize;
    int EdenPartitioningStoreCount;

    TTableMountConfig()
    {
        RegisterParameter("key_count_rotation_threshold", KeyCountRotationThreshold)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("value_count_rotation_threshold", ValueCountRotationThreshold)
            .GreaterThan(0)
            .Default(10000000);
        RegisterParameter("aligned_pool_size_rotation_threshold", AlignedPoolSizeRotationThreshold)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);
        RegisterParameter("unaligned_pool_size_rotation_threshold", UnalignedPoolSizeRotationThreshold)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);

        RegisterParameter("max_partition_data_size", MaxPartitionDataSize)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("min_partition_data_size", MinPartitionDataSize)
            .Default((i64) 16 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_partition_count", MaxPartitionCount)
            .Default(64)
            .GreaterThan(0);

        RegisterParameter("eden_partitioning_data_size", EdenPartitioningDataSize)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("eden_partitioning_store_count", EdenPartitioningStoreCount)
            .Default(8)
            .GreaterThan(0);

        RegisterValidator([&] () {
            if (MinPartitionDataSize >= MaxPartitionDataSize) {
                THROW_ERROR_EXCEPTION("\"min_partition_data_size\" must be less than \"max_partition_data_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

///////////////////////////////////////////////////////////////////////////////

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

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
