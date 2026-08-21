#pragma once

#include "public.h"

#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/logger/log.h>

#include <library/cpp/yt/string/guid.h>

#include <library/cpp/yt/yson_string/string.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

using TQueryId = TGuid;

//! Applicable for native and process plugins.
struct TYqlNativePluginOptions
{
    NYson::TYsonString SingletonsConfig;
    NYson::TYsonString GatewayConfig;
    NYson::TYsonString DqGatewayConfig;
    NYson::TYsonString YtflowGatewayConfig;
    NYson::TYsonString PqGatewayConfig;
    NYson::TYsonString SolomonGatewayConfig;
    NYson::TYsonString DqManagerConfig;
    NYson::TYsonString FileStorageConfig;
    NYson::TYsonString TvmConfig;
    NYson::TYsonString YtAccessProviderConfig;
    NYson::TYsonString OperationAttributes;
    NYson::TYsonString Libraries;

    NYson::TYsonString InitialDynamicConfig;

    TString YTTokenPath;

    THolder<TLogBackend> LogBackend;

    bool StartDqManager;
};

//! Applicable only for qtworker plugin.
struct TYqlQTWorkerPluginOptions
    : public TYqlNativePluginOptions
{
    THolder<TLogBackend> QtWorkerLogBackend;
    int QtWorkerInspectorPort = 32391;
    TString GatewaysConfigPath;
};

struct TQueryResult
{
    std::optional<TString> YsonResult;
    std::optional<TString> Plan;
    std::optional<TString> Statistics;
    std::optional<TString> Progress;
    std::optional<TString> TaskInfo;
    std::optional<TString> Ast;

    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

struct TClustersResult
{
    std::vector<std::pair<TString, TString>> Clusters;

    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

enum EQueryFileContentType
{
    RawInlineData,
    Url,
};

struct TQueryFile
{
    TStringBuf Name;
    TStringBuf Content;
    EQueryFileContentType Type;
};

struct TAbortResult
{
    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

struct TGetDeclaredParametersInfoResult
{
    std::optional<TString> YsonParameters;
};


//! This interface encapsulates YT <-> YQL integration.
/*!
*  \note Thread affinity: any
*/
struct IYqlPlugin
{
    virtual void Start() = 0;

    virtual TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files) = 0;

    virtual TQueryResult Run(
        TQueryId queryId,
        TString user,
        NYson::TYsonString credentials,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode,
        NYqlClient::EQueryType queryType) = 0;

    virtual TQueryResult GetProgress(TQueryId queryId) = 0;

    virtual TAbortResult Abort(TQueryId queryId) = 0;

    virtual void OnDynamicConfigChanged(TYqlPluginDynamicConfigPtr config) = 0;

    virtual void OnUdfMetaChanged(TUdfMetaPtr udfMeta) = 0;

    virtual TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId queryId,
        TString user,
        TString queryText,
        NYson::TYsonString settings,
        NYson::TYsonString credentials) = 0;

    virtual NYTree::IMapNodePtr GetOrchidNode() const;

    virtual void RegisterQuery(TQueryId queryId) = 0;
    virtual void UnregisterQuery(TQueryId queryId) = 0;

    virtual ~IYqlPlugin() = default;
};

////////////////////////////////////////////////////////////////////////////////

TYqlNativePluginOptions ConvertToNativePluginOptions(
    TYqlPluginConfigPtr config,
    TYqlPluginDynamicConfigPtr initialDynamicConfig,
    NYson::TYsonString singletonsConfigString,
    THolder<TLogBackend> logBackend,
    bool startDqManager = false);

TYqlQTWorkerPluginOptions ConvertToQtWorkerPluginOptions(
    TYqlNativePluginOptions nativeOptions,
    THolder<TLogBackend> qtWorkerLogBackend,
    int qtWorkerInspectorPort,
    TString gatewaysConfigPath);

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf DefaultFlavor = "default";

TString DetectFlavorFromSettings(const NYson::TYsonString& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
