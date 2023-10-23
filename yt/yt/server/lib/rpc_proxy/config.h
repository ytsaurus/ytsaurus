#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

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

class TStructuredLoggingMethodDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Whether to log this particular method.
    bool Enable;

    //! Request size limit for particular method.
    //! If value is absent global limit StructuredLoggingMaxRequestByteSize is used.
    std::optional<i64> MaxRequestByteSize;

    REGISTER_YSON_STRUCT(TStructuredLoggingMethodDynamicConfig);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStructuredLoggingMethodDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStructuredLoggingTopicDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Global switch for enabling or disabling particular structured logging topic.
    bool Enable;

    //! [Deprecated] List of methods for which structured logging is not emitted.
    //! Prefer to use `Methods` config.
    THashSet<TString> SuppressedMethods;

    //! Configuration for particular methods.
    THashMap<TString, TStructuredLoggingMethodDynamicConfigPtr> Methods;

    REGISTER_YSON_STRUCT(TStructuredLoggingTopicDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStructuredLoggingTopicDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiTestingOptions
    : public NYTree::TYsonStruct
{
public:
    THeapProfilerTestingOptionsPtr HeapProfiler;

    REGISTER_YSON_STRUCT(TApiTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TSlruCacheConfigPtr ClientCache;

    TSecurityManagerDynamicConfigPtr SecurityManager;

    static constexpr int DefaultClientCacheCapacity = 1000;

    TApiTestingOptionsPtr TestingOptions;

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

    //! Maximum size of the query in SelectRows request.
    //! Queries exceeding this limit will be truncated.
    i64 StructuredLoggingQueryTruncationSize;

    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    bool EnableAllocationTags;

    REGISTER_YSON_STRUCT(TApiServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
