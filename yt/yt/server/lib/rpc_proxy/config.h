#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAsyncExpiringCacheConfigPtr UserCache;

    TSecurityManagerDynamicConfig()
    {
        RegisterParameter("user_cache", UserCache)
            .DefaultNew();

        RegisterPreprocessor([&] {
            UserCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(60);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStructuredLoggingTopicDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Global switch for enabling or disabling particular structured logging topic.
    bool Enable;

    //! List of methods for which structured logging is not emitted.
    THashSet<TString> SuppressedMethods;

    TStructuredLoggingTopicDynamicConfig(THashSet<TString> defaultSuppressedMethods = {})
    {
        RegisterParameter("enable", Enable)
            .Default(true);

        RegisterParameter("suppressed_methods", SuppressedMethods)
            .Default(std::move(defaultSuppressedMethods));
    }
};

DEFINE_REFCOUNTED_TYPE(TStructuredLoggingTopicDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr ClientCache;

    TSecurityManagerDynamicConfigPtr SecurityManager;

    static constexpr int DefaultClientCacheCapacity = 1000;

    TApiServiceConfig()
    {
        RegisterParameter("client_cache", ClientCache)
            .DefaultNew();
        RegisterParameter("security_manager", SecurityManager)
            .DefaultNew();

        RegisterPreprocessor([&] {
            ClientCache->Capacity = DefaultClientCacheCapacity;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TApiServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool VerboseLogging;
    bool EnableModifyRowsRequestReordering;
    bool ForceTracing;

    TSlruCacheDynamicConfigPtr ClientCache;
    i64 ReadBufferRowCount;
    i64 ReadBufferDataWeight;

    TSecurityManagerDynamicConfigPtr SecurityManager;

    TStructuredLoggingTopicDynamicConfigPtr StructuredLoggingMainTopic;
    TStructuredLoggingTopicDynamicConfigPtr StructuredLoggingErrorTopic;

    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    TApiServiceDynamicConfig()
    {
        RegisterParameter("client_cache", ClientCache)
            .DefaultNew();
        RegisterParameter("verbose_logging", VerboseLogging)
            .Default(false);
        RegisterParameter("enable_modify_rows_request_reordering", EnableModifyRowsRequestReordering)
            .Default(true);
        RegisterParameter("force_tracing", ForceTracing)
            .Default(false);
        RegisterParameter("read_buffer_row_count", ReadBufferRowCount)
            .Default(10000);
        RegisterParameter("read_buffer_data_weight", ReadBufferDataWeight)
            .Default(16_MB);
        RegisterParameter("security_manager", SecurityManager)
            .DefaultNew();
        RegisterParameter("structured_logging_main_topic", StructuredLoggingMainTopic)
            .DefaultNew(THashSet<TString>{"ModifyRows", "BatchModifyRows", "LookupRows", "VersionedLookupRows"});
        RegisterParameter("structured_logging_error_topic", StructuredLoggingErrorTopic)
            .DefaultNew();
        RegisterParameter("formats", Formats)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
