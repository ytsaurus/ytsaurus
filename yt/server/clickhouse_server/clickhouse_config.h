#pragma once

#include "private.h"

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TUserConfig
    : public NYTree::TYsonSerializable
{
public:
    // This field is overridden by DefaultProfile in TEngineConfig.
    THashMap<TString, THashMap<TString, NYTree::INodePtr>> Profiles;
    NYTree::IMapNodePtr Quotas;
    NYTree::IMapNodePtr UserTemplate;
    NYTree::IMapNodePtr Users;

    TUserConfig();
};

DEFINE_REFCOUNTED_TYPE(TUserConfig);

////////////////////////////////////////////////////////////////////////////////

class TDictionarySourceYtConfig
    : public NYTree::TYsonSerializable
{
public:
    NYPath::TRichYPath Path;

    TDictionarySourceYtConfig();
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceYtConfig);

////////////////////////////////////////////////////////////////////////////////

//! Source configuration.
//! Extra supported configuration type is "yt".
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/
class TDictionarySourceConfig
    : public NYTree::TYsonSerializable
{
public:
    // TODO(max42): proper value omission.
    TDictionarySourceYtConfigPtr Yt;

    TDictionarySourceConfig();
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceConfig);

////////////////////////////////////////////////////////////////////////////////

//! External dictionary configuration.
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict/
class TDictionaryConfig
    : public NYTree::TYsonSerializable
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

    TDictionaryConfig();
};

DEFINE_REFCOUNTED_TYPE(TDictionaryConfig)

////////////////////////////////////////////////////////////////////////////////

class TSystemLogConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Engine;
    int FlushIntervalMilliseconds;

    TSystemLogConfig();
};

DEFINE_REFCOUNTED_TYPE(TSystemLogConfig);

////////////////////////////////////////////////////////////////////////////////

//! Config containing native clickhouse settings. Do not add our own settings here.
class TClickHouseConfig
    : public NYTree::TYsonSerializable
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

    TSystemLogConfigPtr QueryLog;
    TSystemLogConfigPtr QueryThreadLog;
    TSystemLogConfigPtr TraceLog;

    i64 MaxConcurrentQueries;

    int MaxConnections;

    int KeepAliveTimeout;

    int TcpPort;
    int HttpPort;

    //! Settings for default user profile, this field is introduced for convenience.
    //! Refer to https://clickhouse.yandex/docs/en/operations/settings/settings/ for a complete list.
    //! This map is merged into `users/profiles/default`.
    THashMap<TString, NYTree::INodePtr> Settings;

    TClickHouseConfig();
};

DEFINE_REFCOUNTED_TYPE(TClickHouseConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
