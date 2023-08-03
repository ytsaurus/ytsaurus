#pragma once

#include "private.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TUserConfig
    : public NYTree::TYsonStruct
{
public:
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

class TDictionarySourceYtConfig
    : public NYTree::TYsonStruct
{
public:
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TDictionarySourceYtConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceYtConfig)

////////////////////////////////////////////////////////////////////////////////

//! Source configuration.
//! Extra supported configuration type is "yt".
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/
class TDictionarySourceConfig
    : public NYTree::TYsonStruct
{
public:
    // TODO(max42): proper value omission.
    TDictionarySourceYtConfigPtr Yt;

    REGISTER_YSON_STRUCT(TDictionarySourceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceConfig)

////////////////////////////////////////////////////////////////////////////////

//! External dictionary configuration.
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict/
class TDictionaryConfig
    : public NYTree::TYsonStruct
{
public:
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

class TSystemLogConfig
    : public NYTree::TYsonStruct
{
public:
    TString Engine;
    int FlushIntervalMilliseconds;

    REGISTER_YSON_STRUCT(TSystemLogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSystemLogConfig)

////////////////////////////////////////////////////////////////////////////////

class TPocoInvalidCertificateHandlerConfig
    : public NYTree::TYsonStruct
{
public:
    TString Name;

    REGISTER_YSON_STRUCT(TPocoInvalidCertificateHandlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPocoInvalidCertificateHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPocoOpenSSLConfigEntry
    : public NYTree::TYsonStruct
{
public:
    TPocoInvalidCertificateHandlerConfigPtr InvalidCertificateHandler;
    TString CAConfig;

    REGISTER_YSON_STRUCT(TPocoOpenSSLConfigEntry);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPocoOpenSSLConfigEntry)

////////////////////////////////////////////////////////////////////////////////

class TPocoOpenSSLConfig
    : public NYTree::TYsonStruct
{
public:
    TPocoOpenSSLConfigEntryPtr Server;
    TPocoOpenSSLConfigEntryPtr Client;

    REGISTER_YSON_STRUCT(TPocoOpenSSLConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPocoOpenSSLConfig)

////////////////////////////////////////////////////////////////////////////////

//! Config containing native clickhouse settings. Do not add our own settings here.
class TClickHouseConfig
    : public NYTree::TYsonStruct
{
public:
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

    int KeepAliveTimeout;

    int TcpPort;
    int HttpPort;

    std::optional<i64> MaxServerMemoryUsage;

    //! Settings for default user profile, this field is introduced for convenience.
    //! Refer to https://clickhouse.yandex/docs/en/operations/settings/settings/ for a complete list.
    //! This map is merged into `users/profiles/default`.
    THashMap<TString, NYTree::INodePtr> Settings;

    TPocoOpenSSLConfigPtr OpenSSL;

    NYTree::IMapNodePtr QueryMaskingRules;

    REGISTER_YSON_STRUCT(TClickHouseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
