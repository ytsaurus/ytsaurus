#pragma once

#include "private.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TUserConfig
    : public NYTree::TYsonStruct
{
    // This field is overridden by DefaultProfile in TEngineConfig.
    THashMap<TString, THashMap<TString, NYTree::INodePtr>> Profiles;
    NYTree::IMapNodePtr Quotas;
    NYTree::IMapNodePtr UserTemplate;
    NYTree::IMapNodePtr Users;

    REGISTER_YSON_STRUCT(TUserConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDictionarySourceYtConfig
    : public NYTree::TYsonStruct
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TDictionarySourceYtConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceYtConfig)

////////////////////////////////////////////////////////////////////////////////

//! Source configuration.
//! Extra supported configuration type is "yt".
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/
struct TDictionarySourceConfig
    : public NYTree::TYsonStruct
{
    // TODO(max42): proper value omission.
    TDictionarySourceYtConfigPtr Yt;

    REGISTER_YSON_STRUCT(TDictionarySourceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceConfig)

////////////////////////////////////////////////////////////////////////////////

//! External dictionary configuration.
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict/
struct TDictionaryConfig
    : public NYTree::TYsonStruct
{
    TString Name;

    //! Source configuration.
    TDictionarySourceConfigPtr Source;

    //! Layout configuration.
    //! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_layout/
    NYTree::IMapNodePtr Layout;

    //! Structure configuration.
    //! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_structure/
    NYTree::IMapNodePtr Structure;

    //! Lifetime configuration.
    //! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_lifetime/
    NYTree::INodePtr Lifetime;

    REGISTER_YSON_STRUCT(TDictionaryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionaryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSystemLogConfig
    : public NYTree::TYsonStruct
{
    TString Engine;
    int FlushIntervalMilliseconds;

    REGISTER_YSON_STRUCT(TSystemLogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSystemLogConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPocoInvalidCertificateHandlerConfig
    : public NYTree::TYsonStruct
{
    TString Name;

    REGISTER_YSON_STRUCT(TPocoInvalidCertificateHandlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPocoInvalidCertificateHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPocoOpenSslConfigEntry
    : public NYTree::TYsonStruct
{
public:
    TPocoInvalidCertificateHandlerConfigPtr InvalidCertificateHandler;
    TString CAConfig;

    REGISTER_YSON_STRUCT(TPocoOpenSslConfigEntry);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPocoOpenSslConfigEntry)

////////////////////////////////////////////////////////////////////////////////

struct TPocoOpenSslConfig
    : public NYTree::TYsonStruct
{
    TPocoOpenSslConfigEntryPtr Server;
    TPocoOpenSslConfigEntryPtr Client;

    REGISTER_YSON_STRUCT(TPocoOpenSslConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPocoOpenSslConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryCacheConfig
    : public NYTree::TYsonStruct
{
    size_t MaxSizeInBytes;
    size_t MaxEntries;
    size_t MaxEntrySizeInBytes;
    size_t MaxEntrySizeInRows;

    REGISTER_YSON_STRUCT(TQueryCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryCacheConfig)

////////////////////////////////////////////////////////////////////////////////

//! Config containing native clickhouse settings. Do not add our own settings here.
struct TClickHouseConfig
    : public NYTree::TYsonStruct
{
    //! A map with users.
    TUserConfigPtr Users;

    //! Path in filesystem to the internal state.
    TString DataPath;

    //! Log level for internal CH logging.
    TString LogLevel;

    //! External dictionaries.
    std::vector<TDictionaryConfigPtr> Dictionaries;

    //! Paths to geodata stuff.
    std::optional<TString> PathToRegionsHierarchyFile;
    std::optional<TString> PathToRegionsNameFiles;

    std::optional<TString> Timezone;

    TSystemLogConfigPtr QueryLog;
    TSystemLogConfigPtr QueryThreadLog;
    TSystemLogConfigPtr TraceLog;

    i64 MaxConcurrentQueries;

    int MaxConnections;

    int MaxThreadPoolSize;
    int MaxThreadPoolFreeSize;
    int ThreadPoolQueueSize;

    int MaxIOThreadPoolSize;
    int MaxIOThreadPoolFreeSize;
    int IOThreadPoolQueueSize;

    int KeepAliveTimeout;

    int TcpPort;
    int HttpPort;

    std::optional<i64> MaxServerMemoryUsage;

    i64 MaxTemporaryDataOnDiskSize;

    //! Settings for default user profile, this field is introduced for convenience.
    //! Refer to https://clickhouse.yandex/docs/en/operations/settings/settings/ for a complete list.
    //! This map is merged into `users/profiles/default`.
    THashMap<TString, NYTree::INodePtr> Settings;

    TPocoOpenSslConfigPtr OpenSsl;

    NYTree::IMapNodePtr QueryMaskingRules;

    TQueryCacheConfigPtr QueryCache;

    TString DefaultDatabase;

    REGISTER_YSON_STRUCT(TClickHouseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
