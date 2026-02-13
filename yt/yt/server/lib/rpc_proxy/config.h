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

struct TApiTestingOptions
    : public NYTree::TYsonStruct
{
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

// What kinds of requests we can send to specific cluster.
DEFINE_ENUM(EMultiproxyEnabledMethods,
    // Cannot send any request, all methods are disabled.
    ((DisableAll)         (0))
    // Can send requests that are explicitly enabled in configuration (EMultiproxyMethodKind::ExplicitlyEnabled).
    ((ExplicitlyEnabled)  (1))
    // Can send only read requests.
    ((Read)               (2))
    // Can send read and write requests.
    ((ReadAndWrite)              (3))
);

DEFINE_ENUM(EMultiproxyMethodKind,
    ((ExplicitlyEnabled) (1))
    ((Read)              (2))
    ((Write)             (3))
    ((ExplicitlyDisabled)(4))
);

// Preset configuration which defines what requests can be redirected to particular cluster.
struct TMultiproxyPresetDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    // What kind of requests might be send to cluster.
    EMultiproxyEnabledMethods EnabledMethods;

    // Mapping <method-name> -> <method-kind>
    // This mapping overrides default method markup hardcoded into RPC proxy and
    // allows to enable or disable particular method redirection in multiproxy mode.
    //
    // NB. Method names are in CamelCase (e.g. "WriteTable").
    // One can usually use "explicitly_enabled" or "explicitly_disabled" as value.
    THashMap<std::string, EMultiproxyMethodKind> MethodOverrides;

    REGISTER_YSON_STRUCT(TMultiproxyPresetDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiproxyPresetDynamicConfig)

// Configuration for multiproxy mode.
struct TMultiproxyDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    // Mapping <preset-name> -> <preset-configuration>
    // Preset defines which requests can be redirected to particular cluster.
    // One preset can be used for multiple clusters.
    THashMap<std::string, TMultiproxyPresetDynamicConfigPtr> Presets;

    // Mapping <cluster-name> -> <preset-name>
    // Mapping defines which preset should be used for particular cluster.
    // If client tries to redirect request to cluster that is missing in current map,
    // preset with name "default" is used. If this preset is missing proxy uses empty preset configuration
    // that disallows any request redirection.
    THashMap<std::string, std::string> ClusterPresets;

    REGISTER_YSON_STRUCT(TMultiproxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiproxyDynamicConfig)

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

    TMultiproxyDynamicConfigPtr Multiproxy;

    bool EnableAllocationTags;

    REGISTER_YSON_STRUCT(TApiServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
