#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/security_server/config.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct TStructuredLoggingMethodDynamicConfig
    : public virtual NYTree::TYsonStruct
{
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

struct TStructuredLoggingTopicDynamicConfig
    : public virtual NYTree::TYsonStruct
{
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

struct TQueryCorpusReporterConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    int MaxBatchSize;
    TDuration Period;
    TDuration Splay;
    double Jitter;
    TDuration ReportBackoffTime;
    std::optional<NYPath::TYPath> TablePath;

    REGISTER_YSON_STRUCT(TQueryCorpusReporterConfig);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryCorpusReporterConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiTestingOptions
    : public NYTree::TYsonStruct
{
public:
    NServer::THeapProfilerTestingOptionsPtr HeapProfiler;

    REGISTER_YSON_STRUCT(TApiTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceConfig
    : public virtual NYTree::TYsonStruct
{
    TSlruCacheConfigPtr ClientCache;

    NSecurityServer::TUserAccessValidatorDynamicConfigPtr UserAccessValidator;

    TApiTestingOptionsPtr TestingOptions;

    bool EnableLargeColumnarStatistics = true;

    REGISTER_YSON_STRUCT(TApiServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    bool VerboseLogging;
    bool EnableModifyRowsRequestReordering;
    bool ForceTracing;

    TSlruCacheDynamicConfigPtr ClientCache;
    i64 ReadBufferRowCount;
    i64 ReadBufferDataWeight;

    NSecurityServer::TUserAccessValidatorDynamicConfigPtr UserAccessValidator;

    TStructuredLoggingTopicDynamicConfigPtr StructuredLoggingMainTopic;
    TStructuredLoggingTopicDynamicConfigPtr StructuredLoggingErrorTopic;
    //! If request byte size exceeds this value, it will be logged as # in main topic.
    i64 StructuredLoggingMaxRequestByteSize;

    //! Maximum size of the query in SelectRows request.
    //! Queries exceeding this limit will be truncated.
    i64 StructuredLoggingQueryTruncationSize;

    TQueryCorpusReporterConfigPtr QueryCorpusReporter;

    THashMap<NFormats::EFormatType, NServer::TFormatConfigPtr> Formats;

    bool EnableAllocationTags;

    REGISTER_YSON_STRUCT(TApiServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
