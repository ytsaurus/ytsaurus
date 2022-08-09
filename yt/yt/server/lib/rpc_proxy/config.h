#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TAsyncExpiringCacheConfigPtr UserCache;

    REGISTER_YSON_STRUCT(TSecurityManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStructuredLoggingTopicDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Global switch for enabling or disabling particular structured logging topic.
    bool Enable;

    //! List of methods for which structured logging is not emitted.
    THashSet<TString> SuppressedMethods;

    REGISTER_YSON_STRUCT(TStructuredLoggingTopicDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStructuredLoggingTopicDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TSlruCacheConfigPtr ClientCache;

    TSecurityManagerDynamicConfigPtr SecurityManager;

    static constexpr int DefaultClientCacheCapacity = 1000;

    REGISTER_YSON_STRUCT(TApiServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceDynamicConfig
    : public virtual NYTree::TYsonStruct
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
    //! If request byte size exceeds this value, it will be logged as # in main topic.
    i64 StructuredLoggingMaxRequestByteSize;

    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    REGISTER_YSON_STRUCT(TApiServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
