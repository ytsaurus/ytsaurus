#include "yql_agent.h"

#include "config.h"
#include "interop.h"

#include <yt/yql/plugin/bridge/plugin.h>
#include <yt/yql/plugin/process/plugin.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/net/config.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <string>
#include <vector>

namespace NYT::NYqlAgent {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NLogging;
using namespace NSecurityClient;
using namespace NThreading;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYqlPlugin;
using namespace NYqlPlugin::NProcess;
using namespace NYson;
using namespace NYTree;

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

struct TDiscoveredSecret
{
    TYsonString Content;
    std::optional<TYsonString> Category;
    std::optional<TYsonString> Subcategory;
};

////////////////////////////////////////////////////////////////////////////////

static std::optional<TString> TryIssueToken(
    const TQueryId queryId,
    const TString& user,
    const std::vector<std::pair<TString, TString>>& clusters,
    THashMap<TString, IClientPtr>& queryClients,
    TDuration expirationTimeout)
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
                YT_LOG_WARNING(
                    "Requested token already exists in the cluster (User: %v, Cluster: %v)",
                    user,
                    cluster.first);
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

static TString IssueToken(
    const TQueryId queryId,
    const TString& user,
    const std::vector<std::pair<TString, TString>>& clusters,
    THashMap<TString, IClientPtr>& queryClients,
    TDuration expirationTimeout,
    int attempts)
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

static void RefreshToken(const TString& user, const TString& token, const THashMap<TString, IClientPtr>& queryClients)
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

namespace {

std::optional<NYql::TLangVersion> ParseYQLVersion(const std::string& version)
{
    NYql::TLangVersion result;
    if (NYql::ParseLangVersion(version, result) && NYql::IsValidLangVersion(result)) {
        return result;
    }
    return {};
}

} // namespace

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

        InitYqlVersions();

        auto options = ConvertToOptions(
            Config_,
            singletonsConfigString,
            CreateArcadiaLogBackend(TLogger("YqlPlugin")),
            MaxSupportedYqlVersionStr_,
            Config_->EnableDQ);

        // NB: under debug build this method does not fit in regular fiber stack
        // due to python udf loading
        using TSignature = void(TYqlPluginOptions);
        auto coroutine = TCoroutine<TSignature>(
            BIND([this, bootstrap, singletonsConfigDefaultLogging](
                TCoroutine<TSignature>& /*self*/,
                TYqlPluginOptions options
            ) {
                YqlPlugin_ = Config_->ProcessPluginConfig->Enabled
                    ? CreateProcessYqlPlugin(
                        Config_,
                        singletonsConfigDefaultLogging,
                        bootstrap->GetClusterConnectionConfig(),
                        TString(MaxSupportedYqlVersionStr_),
                        YqlAgentProfiler().WithPrefix("/process_yql_plugin"))
                    : CreateBridgeYqlPlugin(std::move(options));
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

        InitYqlVersions();

        if (DynamicConfig_->GatewaysConfig) {
            TYqlPluginDynamicConfig pluginDynamicConfig{
                .GatewaysConfig = ConvertToYsonString(DynamicConfig_->GatewaysConfig),
                .MaxSupportedYqlVersion = TYsonString(MaxSupportedYqlVersionStr_),
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

    TFuture<TRspGetDeclaredParametersInfo> GetDeclaredParametersInfo(const TString& user, const TString& query, const TYsonString& settings) override
    {
        YT_LOG_INFO("Getting query declared parameters types");

        return BIND(&TYqlAgent::DoGetDeclaredParametersInfo, MakeStrong(this), user, query, settings)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TRspGetQueryProgress GetQueryProgress(TQueryId queryId) override
    {
        YT_LOG_DEBUG("Getting query progress from YQL plugin (QueryId: %v)", queryId);

        TRspGetQueryProgress response;

        try {
            auto result = YqlPlugin_->GetProgress(queryId);
            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }
            YT_LOG_DEBUG("Successfully got query progress from YQL plugin");

            if (result.Plan || result.Progress) {
                TYqlResponse yqlResponse;
                ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
                ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
                ValidateAndFillYqlResponseField(yqlResponse, result.Ast, &TYqlResponse::mutable_ast);
                response.mutable_yql_response()->Swap(&yqlResponse);
            }
            return response;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to get query progress")
                << TErrorAttribute("query_id", queryId)
                << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

    TRspGetYqlAgentInfo GetYqlAgentInfo() override
    {
        YT_LOG_DEBUG("Getting YQL agent info");

        TRspGetYqlAgentInfo response;

        std::vector<std::pair<ui32, ui32>> versions {
            #include "yql/essentials/public/langver/yql_langver_list.inc"
        };

        NYql::TLangVersion maxVersion;
        {
            auto guard = ReaderGuard(YqlVersionLock_);
            response.set_default_ui_yql_version(DefaultYqlUILangVersionStr_);
            maxVersion = MaxSupportedYqlVersion_;
        }

        NYql::TLangVersionBuffer buffer;
        for (const auto& [year, minor] : versions) {
            const auto version = NYql::MakeLangVersion(year, minor);
            if (version <= maxVersion) {
                TStringBuf formattedVersion;
                YT_VERIFY(NYql::FormatLangVersion(version, buffer, formattedVersion));
                response.add_available_yql_versions(formattedVersion);
            }
        }

        auto supportedFeatures = BuildYsonStringFluently()
            .BeginMap()
                .Item("declare_params").Value(true)
                .Item("yql_runner").Value(true)
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

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, YqlVersionLock_);
    NYql::TLangVersion MaxSupportedYqlVersion_;
    std::string MaxSupportedYqlVersionStr_;
    std::string DefaultYqlUILangVersionStr_;

    TYqlAgentDynamicConfigPtr DynamicConfig_;

    std::unique_ptr<IYqlPlugin> YqlPlugin_;

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

        YT_LOG_INFO("Running query via YQL plugin");

        std::vector<TSharedRef> wireRowsets;

        TPeriodicExecutorPtr refreshTokenExecutor;
        auto stopTokenRefresh = [&] {
            if (refreshTokenExecutor) {
                WaitUntilSet(refreshTokenExecutor->Stop());
            }
        };

        try {
            auto query = TString(yqlRequest.query());
            auto settings = yqlRequest.has_settings() ? TYsonString(yqlRequest.settings()) : EmptyMap;

            std::vector<TQueryFile> files;
            files.reserve(yqlRequest.files_size());
            for (const auto& file : yqlRequest.files()) {
                files.push_back(TQueryFile{
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

            EraseNonYtClusters(clustersResult.Clusters);

            THashMap<TString, IClientPtr> queryClients;
            for (const auto& clusterName : clustersResult.Clusters) {
                queryClients[clusterName.first] = ClusterDirectory_->GetConnectionOrThrow(clusterName.first)->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(user));
            }

            auto token = IssueToken(queryId, user, clustersResult.Clusters, queryClients, Config_->TokenExpirationTimeout, Config_->IssueTokenAttempts);

            refreshTokenExecutor = New<TPeriodicExecutor>(ControlInvoker_, BIND(&RefreshToken, user, token, queryClients), Config_->RefreshTokenPeriod);
            refreshTokenExecutor->Start();

            const auto defaultCluster = clustersResult.Clusters.front().first;
            // TODO(ngc224): revise after proper auth support in UI
            THashMap<TString, THashMap<TString, TString>> credentials = {
                {"default_yt", {{"category", "yt"}, {"content", token}}},
                {"default_ytflow", {{"category", "ytflow"}, {"content", token}}}
            };

            FillCredentials(
                credentials,
                yqlRequest.secrets(),
                defaultCluster,
                user,
                queryClients);

            // This is a long blocking call.
            const auto result = YqlPlugin_->Run(queryId, user, ConvertToYsonString(credentials), query, settings, files, yqlRequest.mode());

            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }

            YT_LOG_INFO("YQL plugin query run completed");

            auto clientOptions = NApi::TClientOptions::FromUserAndToken(user, token);

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

            stopTokenRefresh();
            response.mutable_yql_response()->Swap(&yqlResponse);
            return {response, wireRowsets};
        } catch (const std::exception& ex) {
            auto error = TError("Failed to run query")
                << TErrorAttribute("query_id", queryId)
                << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            stopTokenRefresh();
            THROW_ERROR error;
        }
    }

    TRspGetDeclaredParametersInfo DoGetDeclaredParametersInfo(const TString& user, const TString& query, const TYsonString& settings)
    {
        const auto& Logger = YqlAgentLogger();

        TRspGetDeclaredParametersInfo response;

        YT_LOG_INFO("Getting declared parameters via YQL plugin");

        try {
            TQueryId fictionalQueryId = TQueryId::Create();
            auto clustersResult = YqlPlugin_->GetUsedClusters(fictionalQueryId, query, settings, {});
            if (clustersResult.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*clustersResult.YsonError));
                THROW_ERROR error;
            }

            EraseNonYtClusters(clustersResult.Clusters);

            THashMap<TString, IClientPtr> queryClients;
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

            const auto result = YqlPlugin_->GetDeclaredParametersInfo(fictionalQueryId, user, query, settings, ConvertToYsonString(credentials));
            WaitFor(refreshTokenExecutor->Stop()).ThrowOnError();

            ToProto(response.mutable_declared_parameters_info(), result.YsonParameters.value_or("{}"));

            YT_LOG_INFO("Successfully got declared parameters via YQL plugin");

            return response;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to get declared parameters for query")
                << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

    void DoAbortQuery(TQueryId queryId)
    {
        YT_LOG_INFO("Aborting query (QueryId: %v)", queryId);

        try {
            auto abortResult = YqlPlugin_->Abort(queryId);
            YT_LOG_DEBUG("YQL plugin query abort finished (QueryId: %v)", queryId);
            if (auto ysonError = abortResult.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*ysonError));
                error.ThrowOnError();
            }
        } catch (const std::exception& ex) {
            auto error = TError("Failed to abort query")
                << TErrorAttribute("query_id", queryId)
                << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
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
        const IClientPtr& client,
        const TString& path)
    {
        auto aclAttributePath = Format("%v/@effective_acl", path);

        auto acl = ConvertTo<std::vector<TSerializableAccessControlEntry>>(
            WaitFor(client->GetNode(aclAttributePath)).ValueOrThrow());

        THashSet<std::string> allowedReadSubjects;

        for (const auto& ace : acl) {
            if (ace.Action == ESecurityAction::Allow &&
                Any(ace.Permissions & EPermission::Read))
            {
                for (auto& subject : ace.Subjects) {
                    allowedReadSubjects.insert(std::move(subject));
                }
            }
        }

        return allowedReadSubjects;
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("yql_plugin").Value(YqlPlugin_->GetOrchidNode())
            .EndMap();
    }

    void EraseNonYtClusters(std::vector<std::pair<TString, TString>>& clusters) const
    {
        auto ytClusterNames = ClusterDirectory_->GetClusterNames();
        THashSet<TString> presentYtClusters(ytClusterNames.begin(), ytClusterNames.end());
        EraseIf(clusters, [&presentYtClusters](const std::pair<TString, TString>& cluster) {
            if (!presentYtClusters.contains(cluster.first)) {
                return true;
            }
            return false;
        });
    }

    void FillCredentials(
        THashMap<TString, THashMap<TString, TString>>& credentials,
        const ::google::protobuf::RepeatedPtrField<TYqlSecret>& secrets,
        const TString& defaultCluster,
        const TString& user,
        THashMap<TString, IClientPtr>& queryClients)
    {
        std::vector<TFuture<void>> fillCredentialFutures;

        for (const auto& src : secrets) {
            auto& dst = credentials[src.id()];
            auto ypath = NYPath::TRichYPath(src.ypath());
            auto cluster = ypath.GetCluster().value_or(defaultCluster);
            if (!queryClients.contains(cluster)) {
                queryClients[cluster] = ClusterDirectory_
                    ->GetConnectionOrThrow(cluster)
                    ->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(user));
            }

            auto commonErrorAttributes = std::vector{
                TErrorAttribute("secret_id", src.id()),
                TErrorAttribute("secret_path", src.ypath())
            };

            auto& queryClient = queryClients[cluster];
            if (!Config_->InsecureSecretPathSubjects.empty()) {
                auto allowedReadSubjects = FetchAllowedReadSubjects(
                    queryClient,
                    ypath.GetPath());

                std::vector<std::string> insecureSubjects;

                for (const auto& subject : Config_->InsecureSecretPathSubjects) {
                    if (allowedReadSubjects.contains(subject)) {
                        insecureSubjects.push_back(subject);
                    }
                }

                if (!insecureSubjects.empty()) {
                    THROW_ERROR_EXCEPTION("Found insecure subjects for provided secret")
                        << commonErrorAttributes
                        << TErrorAttribute("insecure_subjects", insecureSubjects);
                }
            }

            auto getNodeOptions = NApi::TGetNodeOptions();
            getNodeOptions.Attributes = TAttributeFilter({
                "type",
                "_yqla_secret_category",
                "_yqla_secret_subcategory",
            });

            auto future = queryClient->GetNode(ypath.GetPath(), getNodeOptions)
                .Apply(BIND([
                    queryClient,
                    ypath,
                    commonErrorAttributes
                ](const TErrorOr<TYsonString>& valueOrError) -> TFuture<TDiscoveredSecret> {
                    if (!valueOrError.IsOK()) {
                        return MakeFuture<TDiscoveredSecret>(TError("Cannot get provided secret")
                            << commonErrorAttributes
                            << std::vector<TError>{valueOrError});
                    }

                    const auto& ysonString = valueOrError.Value();
                    auto node = ConvertTo<INodePtr>(ysonString);

                    TDiscoveredSecret discoveredSecret;

                    if (auto category = node->Attributes().FindYson("_yqla_secret_category")) {
                        discoveredSecret.Category = std::move(category);
                    }

                    if (auto subcategory = node->Attributes().FindYson("_yqla_secret_subcategory")) {
                        discoveredSecret.Subcategory = std::move(subcategory);
                    }

                    // document nodes don't return type attribute via GetNode call
                    if (auto type = node->Attributes().Find<NObjectClient::EObjectType>("type")) {
                        switch (*type) {
                        // just safety precaution in case GetNode behavior
                        // regarding document nodes changes in future
                        case NObjectClient::EObjectType::Document:
                        case NObjectClient::EObjectType::StringNode:
                            discoveredSecret.Content = ysonString;
                            return MakeFuture(std::move(discoveredSecret));

                        case NObjectClient::EObjectType::File:
                            break;

                        default:
                            return MakeFuture(TErrorOr<TDiscoveredSecret>(
                                TError("Unexpected secret node type")
                                    << commonErrorAttributes
                                    << TErrorAttribute("node_type", *type)));
                        }

                        return queryClient->CreateFileReader(ypath.GetPath())
                            .Apply(BIND([
                                discoveredSecret = std::move(discoveredSecret)
                            ](const NApi::IFileReaderPtr& fileReader) mutable {
                                auto contentRef = fileReader->ReadAll();
                                auto contentBuf = contentRef.ToStringBuf();
                                contentBuf.ChopSuffix("\n");

                                discoveredSecret.Content = ConvertToYsonString(contentBuf);

                                return std::move(discoveredSecret);
                            }));
                    }

                    discoveredSecret.Content = ysonString;
                    return MakeFuture(std::move(discoveredSecret));
                })).Apply(BIND([
                    &src,
                    &dst,
                    commonErrorAttributes = std::move(commonErrorAttributes)
                ] (const TDiscoveredSecret& discoveredSecret) -> TFuture<void> {
                    auto safeConvertToString = [](
                        const TYsonString& ysonString,
                        const std::vector<TErrorAttribute>& errorAttributes,
                        auto errorMessage
                    ) -> TErrorOr<TString> {
                        TErrorOr<TString> valueOrError;

                        try {
                            valueOrError = ConvertTo<TString>(ysonString);
                        } catch (const std::exception& exception) {
                            valueOrError = TError(errorMessage, TError::DisableFormat)
                                << errorAttributes
                                << std::vector{TError(exception)};
                        }

                        return valueOrError;
                    };

                    auto contentOrError = safeConvertToString(
                        discoveredSecret.Content,
                        commonErrorAttributes,
                        "Cannot convert secret value to string");

                    if (!contentOrError.IsOK()) {
                        return MakeFuture(TError(std::move(contentOrError)));
                    }

                    dst["content"] = contentOrError.Value();

                    std::optional<TString> discoveredCategory;
                    if (discoveredSecret.Category) {
                        auto categoryOrError = safeConvertToString(
                            *discoveredSecret.Category,
                            commonErrorAttributes,
                            "Cannot convert secret category to string");

                        if (!categoryOrError.IsOK()) {
                            return MakeFuture(TError(std::move(categoryOrError)));
                        }

                        discoveredCategory = categoryOrError.Value();
                    }

                    std::optional<TString> discoveredSubcategory;
                    if (discoveredSecret.Subcategory) {
                        auto subcategoryOrError = safeConvertToString(
                            *discoveredSecret.Subcategory,
                            commonErrorAttributes,
                            "Cannot convert secret subcategory to string");

                        if (!subcategoryOrError.IsOK()) {
                            return MakeFuture(TError(std::move(subcategoryOrError)));
                        }

                        discoveredSubcategory = subcategoryOrError.Value();
                    }

                    // TODO: switch back to has_category when api will provide empty category
                    if (src.category() || discoveredCategory) {
                        if (src.category() &&
                            discoveredCategory &&
                            src.category() != *discoveredCategory
                        ) {
                            auto error = TError("Found mismatch between provided and discovered secret categories")
                                << commonErrorAttributes
                                << TErrorAttribute("provided_category", src.category())
                                << TErrorAttribute("discovered_category", *discoveredCategory);

                            return MakeFuture(std::move(error));
                        }

                        dst["category"] = src.category()
                            ? src.category()
                            : *discoveredCategory;
                    } else if (
                        auto secretIdBuf = TStringBuf(src.id());
                        secretIdBuf.SkipPrefix("default_")
                    ) {
                        dst["category"] = TString(secretIdBuf);
                    }

                    // TODO: switch back to has_subcategory when api will provide empty subcategory
                    if (src.subcategory() || discoveredSubcategory) {
                        if (src.subcategory() &&
                            discoveredSubcategory &&
                            src.subcategory() != *discoveredSubcategory
                        ) {
                            auto error = TError("Found mismatch between provided and discovered secret subcategories")
                                << commonErrorAttributes
                                << TErrorAttribute("provided_subcategory", src.subcategory())
                                << TErrorAttribute("discovered_subcategory", *discoveredSubcategory);

                            return MakeFuture(std::move(error));
                        }

                        dst["subcategory"] = src.subcategory()
                            ? src.subcategory()
                            : *discoveredSubcategory;
                    }

                    return OKFuture;
                }));

            fillCredentialFutures.push_back(std::move(future));
        }

        auto resultFutures = WaitFor(AllSet(std::move(fillCredentialFutures)))
            .ValueOrThrow();

        for (const auto& resultFuture : resultFutures) {
            resultFuture.ThrowOnError();
        }
    }

    void InitYqlVersions()
    {
        std::optional<NYql::TLangVersion> maxVersion;
        if (Config_->MaxSupportedYqlVersion.has_value()) {
            maxVersion = ParseYQLVersion(Config_->MaxSupportedYqlVersion.value());
            if (!maxVersion.has_value()) {
                YT_LOG_ERROR("Max YQL version set via config or flag is not valid. Setting default version as maximum available (VersionFromConfig: %v)", Config_->MaxSupportedYqlVersion.value());
            }
        }

        const bool allowNotReleased = DynamicConfig_->AllowNotReleasedYqlVersions.value_or(Config_->AllowNotReleasedYqlVersions.value_or(false));
        if (maxVersion.has_value()) {
            maxVersion = std::min(maxVersion.value(), allowNotReleased ? NYql::GetMaxLangVersion() : NYql::GetMaxReleasedLangVersion());
        } else {
            maxVersion = allowNotReleased ? NYql::GetMaxLangVersion() : NYql::GetMaxReleasedLangVersion();
        }


        std::optional<NYql::TLangVersion> defaultVersion;
        if (DynamicConfig_->DefaultYqlUIVersion.has_value()) {
            defaultVersion = ParseYQLVersion(DynamicConfig_->DefaultYqlUIVersion.value());
            if (!defaultVersion.has_value()) {
                YT_LOG_ERROR("Default UI YQL version set via dynamic config is not valid (VersionFromConfig: %v)", DynamicConfig_->DefaultYqlUIVersion.value());
            }
        } else if (Config_->DefaultYqlUIVersion.has_value()) {
            defaultVersion = ParseYQLVersion(Config_->DefaultYqlUIVersion.value());
            if (!defaultVersion.has_value()) {
                YT_LOG_ERROR("Default UI YQL version set via config or flag is not valid (VersionFromConfig: %v)", Config_->DefaultYqlUIVersion.value());
            }
        }
        if (defaultVersion.has_value()) {
            defaultVersion = std::min(defaultVersion.value(), maxVersion.value());
        } else {
            defaultVersion = NYql::MinLangVersion;
        }

        {
            auto guard = WriterGuard(YqlVersionLock_);
            MaxSupportedYqlVersion_ = maxVersion.value();
            NYql::TLangVersionBuffer buffer;
            TStringBuf maxVersionStr;
            YT_VERIFY(NYql::FormatLangVersion(maxVersion.value(), buffer, maxVersionStr));
            MaxSupportedYqlVersionStr_ = maxVersionStr;
            TStringBuf defaultVersionStr;
            YT_VERIFY(NYql::FormatLangVersion(defaultVersion.value(), buffer, defaultVersionStr));
            DefaultYqlUILangVersionStr_ = defaultVersionStr;
        }
        YT_LOG_INFO("Maximum supported YQL language version is set (Version: %v)", maxVersion.value());
        YT_LOG_INFO("Default YQL language version for UI is set (Version: %v)", defaultVersion.value());
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
