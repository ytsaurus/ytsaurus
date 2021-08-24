#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAsyncExpiringCacheConfigPtr UserCache;

    TSecurityManagerConfig()
    {
        RegisterParameter("user_cache", UserCache)
            .DefaultNew();

        RegisterPreprocessor([&] {
            UserCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(60);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool VerboseLogging;
    bool EnableModifyRowsRequestReordering;
    bool ForceTracing;

    TSlruCacheConfigPtr ClientCache;

    i64 ReadBufferRowCount;
    i64 ReadBufferDataWeight;

    TSecurityManagerConfigPtr SecurityManager;

    TApiServiceConfig()
    {
        RegisterParameter("verbose_logging", VerboseLogging)
            .Default(false);
        RegisterParameter("enable_modify_rows_request_reordering", EnableModifyRowsRequestReordering)
            .Default(true);
        RegisterParameter("force_tracing", ForceTracing)
            .Default(false);
        RegisterParameter("client_cache", ClientCache)
            .Default(New<TSlruCacheConfig>(1000));
        RegisterParameter("read_buffer_row_count", ReadBufferRowCount)
            .Default(10000);
        RegisterParameter("read_buffer_data_weight", ReadBufferDataWeight)
            .Default(16_MB);
        RegisterParameter("security_manager", SecurityManager)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    TApiServiceDynamicConfig()
    {
        RegisterParameter("formats", Formats)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
