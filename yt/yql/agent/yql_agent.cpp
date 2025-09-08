#include "yql_agent.h"

#include "config.h"
#include "interop.h"

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/net/config.h>

#include <yt/yql/plugin/bridge/plugin.h>
#include <yt/yql/plugin/process/plugin.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NYTree;
using namespace NHiveClient;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;
using namespace NHiveClient;
using namespace NSecurityClient;
using namespace NLogging;

constinit const auto Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TActiveQueriesGuard
{
public:
    TActiveQueriesGuard() = delete;

    TActiveQueriesGuard(int maxSimultaneousQueries, std::atomic<int>* activeQueries)
        : ActiveQueries_(activeQueries)
    {
        IsTaken_ = true;
        auto queries = ActiveQueries_->load();
        do {
            if (queries >= maxSimultaneousQueries) {
                IsTaken_ = false;
                return;
            }
        } while (!ActiveQueries_->compare_exchange_weak(queries, queries + 1));
    }

    ~TActiveQueriesGuard()
    {
        if (IsTaken_) {
            ActiveQueries_->fetch_add(-1);
        }
    }

    bool IsTaken()
    {
        return IsTaken_;
    }

private:
    std::atomic<int>* const ActiveQueries_;
    bool IsTaken_;
};

class TActiveQueriesGuardFactory
{
public:
    explicit TActiveQueriesGuardFactory(int maxSimultaneousQueries)
        : MaxSimultaneousQueries_(maxSimultaneousQueries)
    { }

    void Update(int maxSimultaneousQueries)
    {
        MaxSimultaneousQueries_ = maxSimultaneousQueries;
    }

    TActiveQueriesGuard CreateGuard()
    {
        return TActiveQueriesGuard(MaxSimultaneousQueries_, &ActiveQueries_);
    }

    int GetGuardedValue() const
    {
        return ActiveQueries_.load();
    }

private:
    int MaxSimultaneousQueries_;
    std::atomic<int> ActiveQueries_;
};

////////////////////////////////////////////////////////////////////////////////

static std::optional<TString> TryIssueToken(const TQueryId queryId, const TString& user, const std::vector<std::pair<TString, TString>>& clusters, THashMap<TString, NApi::NNative::IClientPtr>& queryClients, const TDuration& expirationTimeout)
{
    TString token;
    if (clusters.empty()) {
        return token;
    }

    auto options = NApi::TIssueTemporaryTokenOptions{ .ExpirationTimeout = expirationTimeout };
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("query_id", queryId);
    attributes->Set("responsible", "query_tracker");

    for (auto& cluster : clusters) {
        YT_LOG_DEBUG("Requesting token (User: %v, Cluster: %v)", user, cluster);
        auto rspOrError = token.empty()
            ? WaitFor(queryClients[cluster.first]->IssueTemporaryToken(user, attributes, options))
            : WaitFor(queryClients[cluster.first]->IssueSpecificTemporaryToken(user, token, attributes, options));

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING("Token request failed (User: %v, Cluster: %v)", user, cluster.first);
            if (rspOrError.FindMatching(NYTree::EErrorCode::AlreadyExists)) {
                YT_LOG_WARNING("Requested token already exists in the cluster (User: %v, Cluster: %v)", user, cluster.first);
                return std::nullopt;
            }
            rspOrError.ThrowOnError();
        }

        if (token.empty()) {
            token = rspOrError.ValueOrThrow().Token;
        }
        YT_LOG_DEBUG("Token received (User: %v, Cluster: %v)", user, cluster);
    }

    return token;
}

static TString IssueToken(const TQueryId queryId, const TString& user, const std::vector<std::pair<TString, TString>>& clusters, THashMap<TString, NApi::NNative::IClientPtr>& queryClients, const TDuration& expirationTimeout, const int attempts)
{
    for (int attempt = 0; attempt < attempts; attempt++) {
        auto tokenOrErr = TryIssueToken(queryId, user, clusters, queryClients, expirationTimeout);
        if (!tokenOrErr) {
            // The selected token already exists on one of the clusters. We need to try to issue token again.
            continue;
        }

        return *tokenOrErr;
    }

    THROW_ERROR_EXCEPTION("Token cannot be issued, all attempts failed");
}

static void RefreshToken(const TString& user, const TString& token, const THashMap<TString, NApi::NNative::IClientPtr>& queryClients)
{
    for (auto& [cluster, client] : queryClients) {
        YT_LOG_DEBUG("Refreshing token (User: %v, Cluster: %v)", user, cluster);
        auto rspOrError = WaitFor(client->RefreshTemporaryToken(user, token, {}));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING("Token refreshing failed (User: %v, Cluster: %v)", user, cluster);
        } else {
            YT_LOG_DEBUG("Token refreshed (User: %v, Cluster: %v)", user, cluster);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYqlAgent
    : public IYqlAgent
{
public:
    TYqlAgent(
        TBootstrap* bootstrap,
        TSingletonsConfigPtr singletonsConfig,
        TYqlAgentConfigPtr yqlAgentConfig,
        TYqlAgentDynamicConfigPtr dynamicConfig,
        TClusterDirectoryPtr clusterDirectory,
        TClientDirectoryPtr clientDirectory,
        IInvokerPtr controlInvoker,
        TString agentId)
        : SingletonsConfig_(std::move(singletonsConfig))
        , Config_(std::move(yqlAgentConfig))
        , ClusterDirectory_(std::move(clusterDirectory))
        , ClientDirectory_(std::move(clientDirectory))
        , ControlInvoker_(std::move(controlInvoker))
        , AgentId_(std::move(agentId))
        , DynamicConfig_(std::move(dynamicConfig))
        , ThreadPool_(CreateThreadPool(Config_->YqlThreadCount, "Yql"))
        , ActiveQueriesGuardFactory_(TActiveQueriesGuardFactory(DynamicConfig_->MaxSimultaneousQueries))
    {
        YqlAgentProfiler().AddFuncGauge("/active_queries", MakeStrong(this), [this] {
            return ActiveQueriesGuardFactory_.GetGuardedValue();
        });

        static const TYsonString EmptyMap = TYsonString(TString("{}"));

        auto clustersConfig = Config_->GatewayConfig->AsMap()->GetChildOrThrow("cluster_mapping")->AsList();

        NYTree::IListNodePtr ytflowClustersConfig;
        if (auto clusterMapping = Config_->YtflowGatewayConfig->AsMap()->FindChild("cluster_mapping")) {
            ytflowClustersConfig = clusterMapping->AsList();
        } else {
            ytflowClustersConfig = GetEphemeralNodeFactory()->CreateList();
            Config_->YtflowGatewayConfig->AsMap()->AddChild("cluster_mapping", ytflowClustersConfig);
        }

        auto singletonsConfigDefaultLogging = CloneYsonStruct(SingletonsConfig_);
        // Compressed logs are broken if plugin tries to open and write to them.
        singletonsConfigDefaultLogging->SetSingletonConfig(TLogManagerConfig::CreateDefault());

        auto singletonsConfigString = singletonsConfigDefaultLogging
            ? ConvertToYsonString(singletonsConfigDefaultLogging)
            : EmptyMap;

        if (!Config_->DQManagerConfig->AddressResolver) {
            Config_->DQManagerConfig->AddressResolver = SingletonsConfig_->GetSingletonConfig<NNet::TAddressResolverConfig>();
        }

        THashSet<TString> presentClusters;
        for (const auto& cluster : clustersConfig->GetChildren()) {
            presentClusters.insert(cluster->AsMap()->GetChildOrThrow("name")->GetValue<TString>());
        }

        THashSet<TString> ytflowPresentClusters;
        for (const auto& cluster : ytflowClustersConfig->GetChildren()) {
            ytflowPresentClusters.insert(cluster->AsMap()->GetChildOrThrow("name")->GetValue<TString>());
        }

        for (const auto& clusterName : ClusterDirectory_->GetClusterNames()) {
            if (!presentClusters.contains(clusterName)) {
                auto cluster = NYTree::BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("name").Value(clusterName)
                        .Item("cluster").Value(clusterName)
                    .EndMap();
                auto settings = TYqlPluginConfig::MergeClusterDefaultSettings(GetEphemeralNodeFactory()->CreateList());
                cluster->AsMap()->AddChild("settings", std::move(settings));
                clustersConfig->AddChild(std::move(cluster));
            }

            if (!ytflowPresentClusters.contains(clusterName)) {
                auto cluster = NYTree::BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("name").Value(clusterName)
                        .Item("real_name").Value(clusterName)
                        .Item("proxy_url").Value(clusterName)
                    .EndMap();
                auto settings = TYqlPluginConfig::MergeClusterDefaultSettings(GetEphemeralNodeFactory()->CreateList());
                cluster->AsMap()->AddChild("settings", std::move(settings));
                ytflowClustersConfig->AddChild(std::move(cluster));
            }
        }

        NYql::TLangVersionBuffer buffer;
        TStringBuf maxVersionStringBuf;
        NYql::TLangVersion maxYqlLangVersion;
        if (!Config_->MaxSupportedYqlVersion || !NYql::ParseLangVersion(Config_->MaxSupportedYqlVersion.value(), maxYqlLangVersion) || !NYql::IsValidLangVersion(maxYqlLangVersion)) {
            maxYqlLangVersion = NYql::GetMaxLangVersion();
            NYql::FormatLangVersion(maxYqlLangVersion, buffer, maxVersionStringBuf);
            if (Config_->MaxSupportedYqlVersion) {
                YT_LOG_ERROR("Max YQL version set via config or flag is not valid. Setting default version as maximum available (VersionFromConfig: %v)", Config_->MaxSupportedYqlVersion);
            }
        } else {
            NYql::FormatLangVersion(maxYqlLangVersion, buffer, maxVersionStringBuf);
        }

        MaxSupportedYqlVersion_ = maxVersionStringBuf;
        YT_LOG_INFO("Maximum supported YQL version is set (Version: %v)", MaxSupportedYqlVersion_);

        TStringBuf defaultVersionStringBuf;
        NYql::FormatLangVersion(std::min(NYql::GetMaxReleasedLangVersion(), maxYqlLangVersion), buffer, defaultVersionStringBuf);
        DefaultYqlUILangVersion_ = defaultVersionStringBuf;
        YT_LOG_INFO("Deafult YQL version for UI is set (Version: %v)", DefaultYqlUILangVersion_);
        auto options = NYqlPlugin::ConvertToOptions(
            Config_,
            singletonsConfigString,
            NYT::NLogging::CreateArcadiaLogBackend(TLogger("YqlPlugin")),
            MaxSupportedYqlVersion_,
            Config_->EnableDQ);

        // NB: under debug build this method does not fit in regular fiber stack
        // due to python udf loading
        using TSignature = void(NYqlPlugin::TYqlPluginOptions);
        auto coroutine = TCoroutine<TSignature>(
            BIND([this, bootstrap, maxVersionStringBuf, singletonsConfigDefaultLogging](
                TCoroutine<TSignature>& /*self*/,
                NYqlPlugin::TYqlPluginOptions options
            ) {
                YqlPlugin_ = Config_->ProcessPluginConfig->Enabled
                    ? NYqlPlugin::NProcess::CreateProcessYqlPlugin(
                        Config_,
                        singletonsConfigDefaultLogging,
                        bootstrap->GetClusterConnectionConfig(),
                        TString(maxVersionStringBuf),
                        YqlAgentProfiler().WithPrefix("/process_yql_plugin"))
                    : NYqlPlugin::CreateBridgeYqlPlugin(std::move(options));
            }),
            EExecutionStackKind::Large);

        coroutine.Run(std::move(options));
        YT_VERIFY(coroutine.IsCompleted());
    }

    void Start() override
    {
        YqlPlugin_->Start();
    }

    void Stop() override
    { }

    NYTree::IYPathServicePtr CreateOrchidService() const override
    {
        auto producer = BIND_NO_PROPAGATE(&TYqlAgent::BuildOrchid, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    void OnDynamicConfigChanged(
        const TYqlAgentDynamicConfigPtr& /*oldConfig*/,
        const TYqlAgentDynamicConfigPtr& newConfig) override
    {
        DynamicConfig_ = newConfig;
        if (DynamicConfig_->MaxSimultaneousQueries >= Config_->YqlThreadCount) {
            YT_LOG_ERROR("Decreased \"max_simultaneous_queries\"; it should be less than \"yql_thread_count\" (MaxSimultaneousQueries: %v, YqlThreadCount: %v)",
                DynamicConfig_->MaxSimultaneousQueries,
                Config_->YqlThreadCount);

            DynamicConfig_->MaxSimultaneousQueries = Config_->YqlThreadCount - 1;
        }
        ActiveQueriesGuardFactory_.Update(DynamicConfig_->MaxSimultaneousQueries);

        if (DynamicConfig_->GatewaysConfig) {
            NYqlPlugin::TYqlPluginDynamicConfig pluginDynamicConfig{
                .GatewaysConfig = ConvertToYsonString(DynamicConfig_->GatewaysConfig),
            };
            YT_LOG_DEBUG("Call YqlPlugin_->OnDynamicConfigChanged with GatewaysConfig: %v", pluginDynamicConfig.GatewaysConfig.AsStringBuf());
            YqlPlugin_->OnDynamicConfigChanged(std::move(pluginDynamicConfig));
        }
    }

    TFuture<std::pair<TRspStartQuery, std::vector<TSharedRef>>> StartQuery(TQueryId queryId, const TString& user, const TReqStartQuery& request) override
    {
        YT_LOG_INFO("Starting query (QueryId: %v, User: %v)", queryId, user);

        return BIND(&TYqlAgent::DoStartQuery, MakeStrong(this), queryId, user, request)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TFuture<void> AbortQuery(TQueryId queryId) override
    {
        return BIND(&TYqlAgent::DoAbortQuery, MakeStrong(this), queryId)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TFuture<TRspGetDeclaredParametersInfo> GetDeclaredParametersInfo(const TString& user, const TString& query, const NYson::TYsonString& settings) override
    {
        YT_LOG_INFO("Getting query declared parameters types");

        return BIND(&TYqlAgent::DoGetDeclaredParametersInfo, MakeStrong(this), user, query, settings)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TRspGetQueryProgress GetQueryProgress(TQueryId queryId) override
    {
        YT_LOG_DEBUG("Getting query progress (QueryId: %v)", queryId);

        TRspGetQueryProgress response;

        YT_LOG_DEBUG("Getting progress from YQL plugin");

        try {
            auto result = YqlPlugin_->GetProgress(queryId);
            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }
            YT_LOG_DEBUG("YQL plugin progress call completed");

            if (result.Plan || result.Progress) {
                TYqlResponse yqlResponse;
                ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
                ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
                ValidateAndFillYqlResponseField(yqlResponse, result.Ast, &TYqlResponse::mutable_ast);
                response.mutable_yql_response()->Swap(&yqlResponse);
            }
            return response;
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

    TRspGetYqlAgentInfo GetYqlAgentInfo() override
    {
        YT_LOG_DEBUG("Getting yql agent info");

        TRspGetYqlAgentInfo response;

        std::vector<std::pair<ui32, ui32>> versions {
            #include "yql/essentials/public/langver/yql_langver_list.inc"
        };

        NYql::TLangVersionBuffer buffer;
        TStringBuf formattedVersion;
        for (const auto& version : versions) {
            NYql::FormatLangVersion(NYql::MakeLangVersion(version.first, version.second), buffer, formattedVersion);
            if (formattedVersion <= MaxSupportedYqlVersion_) {
                response.add_available_yql_versions(formattedVersion);
            }
        }
        response.set_default_ui_yql_version(DefaultYqlUILangVersion_);

        auto supportedFeatures = BuildYsonStringFluently()
            .BeginMap()
                .Item("declare_params").Value(true)
            .EndMap();
        response.set_supported_features(supportedFeatures.ToString());


        return response;
    }

private:
    const TSingletonsConfigPtr SingletonsConfig_;
    const TYqlAgentConfigPtr Config_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr ControlInvoker_;
    const TString AgentId_;

    std::string MaxSupportedYqlVersion_;
    std::string DefaultYqlUILangVersion_;

    TYqlAgentDynamicConfigPtr DynamicConfig_;

    std::unique_ptr<NYqlPlugin::IYqlPlugin> YqlPlugin_;

    IThreadPoolPtr ThreadPool_;
    TActiveQueriesGuardFactory ActiveQueriesGuardFactory_;

    std::pair<TRspStartQuery, std::vector<TSharedRef>> DoStartQuery(TQueryId queryId, const TString& user, const TReqStartQuery& request)
    {
        auto guard = ActiveQueriesGuardFactory_.CreateGuard();

        if (!guard.IsTaken()) {
            YT_LOG_INFO(
                "Query was throttled (QueryId: %v, User: %v)",
                queryId,
                user);
            THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::RequestThrottled, "Query was throttled");
        }

        static const auto EmptyMap = TYsonString(TString("{}"));

        const auto& Logger = YqlAgentLogger().WithTag("QueryId: %v", queryId);

        const auto& yqlRequest = request.yql_request();

        TRspStartQuery response;

        YT_LOG_INFO("Invoking YQL embedded");

        std::vector<TSharedRef> wireRowsets;
        try {
            auto query = TString(yqlRequest.query());
            auto settings = yqlRequest.has_settings() ? TYsonString(yqlRequest.settings()) : EmptyMap;

            std::vector<NYqlPlugin::TQueryFile> files;
            files.reserve(yqlRequest.files_size());
            for (const auto& file : yqlRequest.files()) {
                files.push_back(NYqlPlugin::TQueryFile{
                    .Name = file.name(),
                    .Content = file.content(),
                    .Type = static_cast<EQueryFileContentType>(file.type()),
                });
            }

            auto clustersResult = YqlPlugin_->GetUsedClusters(queryId, query, settings, files);
            if (clustersResult.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*clustersResult.YsonError));
                THROW_ERROR error;
            }

            THashMap<TString, NApi::NNative::IClientPtr> queryClients;
            for (const auto& clusterName : clustersResult.Clusters) {
                queryClients[clusterName.first] = ClusterDirectory_->GetConnectionOrThrow(clusterName.first)->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(user));
            }

            auto token = IssueToken(queryId, user, clustersResult.Clusters, queryClients, Config_->TokenExpirationTimeout, Config_->IssueTokenAttempts);

            auto refreshTokenExecutor = New<TPeriodicExecutor>(ControlInvoker_, BIND(&RefreshToken, user, token, queryClients), Config_->RefreshTokenPeriod);
            refreshTokenExecutor->Start();

            const auto defaultCluster = clustersResult.Clusters.front().first;
            // TODO(ngc224): revise after proper auth support in UI
            THashMap<TString, THashMap<TString, TString>> credentials = {
                {"default_yt", {{"category", "yt"}, {"content", token}}},
                {"default_ytflow", {{"category", "ytflow"}, {"content", token}}}
            };

            for (const auto& src : yqlRequest.secrets()) {
                auto& dst = credentials[src.id()];
                auto ypath = NYPath::TRichYPath(src.ypath());
                auto cluster = ypath.GetCluster().value_or(defaultCluster);
                if (!queryClients.contains(cluster)) {
                    queryClients[cluster] = ClusterDirectory_->GetConnectionOrThrow(cluster)->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(user));
                }

                auto& queryClient = queryClients[cluster];
                if (!Config_->InsecureSecretPathSubjects.empty()) {
                    auto allowedReadSubjects = FetchAllowedReadSubjects(queryClient, ypath.GetPath());
                    std::vector<std::string> insecureSubjects;

                    for (const auto& subject : Config_->InsecureSecretPathSubjects) {
                        if (allowedReadSubjects.contains(subject)) {
                            insecureSubjects.push_back(subject);
                        }
                    }

                    if (!insecureSubjects.empty()) {
                        THROW_ERROR_EXCEPTION("Found insecure subjects for provided secret")
                            << TErrorAttribute("secret_id", src.id())
                            << TErrorAttribute("secret_path", src.ypath())
                            << TErrorAttribute("insecure_subjects", insecureSubjects);
                    }
                }

                dst["content"] = ConvertTo<TString>(WaitFor(queryClient->GetNode(ypath.GetPath())).ValueOrThrow());
                if (src.has_category()) {
                    dst["category"] = src.category();
                }
                if (src.has_subcategory()) {
                    dst["subcategory"] = src.subcategory();
                }
            }

            // This is a long blocking call.
            const auto result = YqlPlugin_->Run(queryId, user, ConvertToYsonString(credentials), query, settings, files, yqlRequest.mode());
            auto finally = Finally([&] {
                try {
                    WaitUntilSet(refreshTokenExecutor->Stop());
                } catch (...) {
                    YT_LOG_ERROR("Caught exception while stopping token refresh: %v", CurrentExceptionMessage());
                }
            });

            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }

            YT_LOG_INFO("YQL plugin 'StartQuery' call completed");

            auto clientOptions = NApi::TClientOptions();
            clientOptions.User = user;
            clientOptions.Token = token;

            TYqlResponse yqlResponse;
            ValidateAndFillYqlResponseField(yqlResponse, result.YsonResult, &TYqlResponse::mutable_result);
            ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
            ValidateAndFillYqlResponseField(yqlResponse, result.Statistics, &TYqlResponse::mutable_statistics);
            ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
            ValidateAndFillYqlResponseField(yqlResponse, result.TaskInfo, &TYqlResponse::mutable_task_info);
            ValidateAndFillYqlResponseField(yqlResponse, result.Ast, &TYqlResponse::mutable_ast);
            if (request.build_rowsets() && result.YsonResult) {
                auto rowsets = BuildRowsets(clustersResult.Clusters, clientOptions, *result.YsonResult, request.row_count_limit());

                for (const auto& rowset : rowsets) {
                    if (rowset.Error.IsOK()) {
                        wireRowsets.push_back(rowset.WireRowset);
                        response.add_rowset_errors();
                        response.add_incomplete(rowset.Incomplete);

                        if (!rowset.References || rowset.References->Reference.size() != 3 || rowset.References->Reference[0] != "yt") {
                            response.add_full_result();
                        } else {
                            const auto& cluster = rowset.References->Reference[1];
                            const auto& table = rowset.References->Reference[2];
                            const auto fullResult = NYTree::BuildYsonStringFluently()
                                .BeginMap()
                                    .Item("cluster").Value(cluster)
                                    .Item("table_path").Value(table)
                                .EndMap();
                            ToProto(response.add_full_result(), fullResult.AsStringBuf());
                        }
                    } else {
                        wireRowsets.push_back(TSharedRef());
                        ToProto(response.add_rowset_errors(), rowset.Error);
                        response.add_incomplete(false);
                        response.add_full_result();
                    }
                }
            }
            response.mutable_yql_response()->Swap(&yqlResponse);
            return {response, wireRowsets};
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin 'StartQuery' call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin 'StartQuery' call failed");
            THROW_ERROR error;
        }
    }

    TRspGetDeclaredParametersInfo DoGetDeclaredParametersInfo(const TString& user, const TString& query, const NYson::TYsonString& settings)
    {
        const auto& Logger = YqlAgentLogger();

        TRspGetDeclaredParametersInfo response;

        YT_LOG_INFO("Invoking YQL plugin GetDeclaredParametersInfo");

        try {
            auto clustersResult = YqlPlugin_->GetUsedClusters(query, settings, {});
            if (clustersResult.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*clustersResult.YsonError));
                THROW_ERROR error;
            }

            THashMap<TString, NApi::NNative::IClientPtr> queryClients;
            for (const auto& clusterName : clustersResult.Clusters) {
                queryClients[clusterName.first] = ClusterDirectory_->GetConnectionOrThrow(clusterName.first)->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(user));
            }

            auto token = IssueToken(TGuid::Create(), user, clustersResult.Clusters, queryClients, Config_->TokenExpirationTimeout, Config_->IssueTokenAttempts);

            auto refreshTokenExecutor = New<TPeriodicExecutor>(ControlInvoker_, BIND(&RefreshToken, user, token, queryClients), Config_->RefreshTokenPeriod);
            refreshTokenExecutor->Start();

            const auto defaultCluster = clustersResult.Clusters.front();
            // TODO(ngc224): revise after proper auth support in UI
            THashMap<TString, THashMap<TString, TString>> credentials = {
                {"default_yt", {{"category", "yt"}, {"content", token}}},
                {"default_ytflow", {{"category", "ytflow"}, {"content", token}}}
            };

            const auto result = YqlPlugin_->GetDeclaredParametersInfo(user, query, settings, ConvertToYsonString(credentials));
            WaitFor(refreshTokenExecutor->Stop()).ThrowOnError();

            ToProto(response.mutable_declared_parameters_info(), result.YsonParameters.value_or("{}"));

            YT_LOG_INFO("YQL plugin 'GetDeclaredParametersInfo' call completed");

            return response;
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin 'GetDeclaredParametersInfo' call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin 'GetDeclaredParametersInfo' call failed");
            THROW_ERROR error;
        }
    }

    void DoAbortQuery(TQueryId queryId)
    {
        YT_LOG_INFO("Aborting query (QueryId: %v)", queryId);

        TError error;

        try {
            auto abortResult = YqlPlugin_->Abort(queryId);
            YT_LOG_DEBUG("Plugin abortion is finished (QueryId: %v)", queryId);
            if (auto ysonError = abortResult.YsonError) {
                error = ConvertTo<TError>(TYsonString(*ysonError));
            }
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }

        error.ThrowOnError();
    }

    void ValidateAndFillYqlResponseField(TYqlResponse& yqlResponse, const std::optional<TString>& rawField, TString* (TYqlResponse::*mutableProtoFieldAccessor)())
    {
        if (!rawField) {
            return;
        }
        // TODO(max42): original YSON tends to unnecessary pretty.
        *((&yqlResponse)->*mutableProtoFieldAccessor)() = *rawField;
    }

    THashSet<std::string> FetchAllowedReadSubjects(
        NApi::NNative::IClientPtr client, const TString& path)
    {
        auto aclAttributePath = ::TStringBuilder() << path << "/@effective_acl";

        auto acl = ConvertTo<std::vector<NSecurityClient::TSerializableAccessControlEntry>>(
            WaitFor(client->GetNode(aclAttributePath)).ValueOrThrow());

        THashSet<std::string> allowedReadSubjects;

        for (const auto& ace : acl) {
            if (ace.Action == NSecurityClient::ESecurityAction::Allow
                && Any(ace.Permissions & NYTree::EPermission::Read)
            ) {
                for (auto& subject : ace.Subjects) {
                    allowedReadSubjects.insert(std::move(subject));
                }
            }
        }

        return allowedReadSubjects;
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer).BeginMap()
            .Item("yql_plugin").Value(YqlPlugin_->GetOrchidNode())
        .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IYqlAgentPtr CreateYqlAgent(
    TBootstrap* bootstrap,
    TSingletonsConfigPtr singletonsConfig,
    TYqlAgentConfigPtr config,
    TYqlAgentDynamicConfigPtr dynamicConfig,
    TClusterDirectoryPtr clusterDirectory,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr controlInvoker,
    TString agentId)
{
    return New<TYqlAgent>(
        bootstrap,
        std::move(singletonsConfig),
        std::move(config),
        std::move(dynamicConfig),
        std::move(clusterDirectory),
        std::move(clientDirectory),
        std::move(controlInvoker),
        std::move(agentId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
