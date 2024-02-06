#include "spyt_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/escape.h>
#include <util/string/split.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

class TSpytSettings
    : public TYsonStruct
{
public:
    std::optional<TString> Cluster;

    std::optional<TYPath> DiscoveryPath;

    std::optional<TString> SparkConf;

    REGISTER_YSON_STRUCT(TSpytSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster", &TThis::Cluster)
            .Default();
        registrar.Parameter("discovery_path", &TThis::DiscoveryPath)
            .Default();
        registrar.Parameter("spark_conf", &TThis::SparkConf)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSpytSettings)
DECLARE_REFCOUNTED_CLASS(TSpytSettings)

///////////////////////////////////////////////////////////////////////////////

class TSpytVersion
{
public:
    TSpytVersion(TString rawVersion)
    {
        auto dashPos = rawVersion.find_first_of('-');
        // Strip SNAPSHOT version suffix.
        if (dashPos != TString::npos) {
            rawVersion = rawVersion.substr(0, dashPos);
        }
        if (!StringSplitter(rawVersion).Split('.').TryCollectInto(&Major_, &Minor_, &Patch_)) {
            THROW_ERROR_EXCEPTION(
                "Version %Qv cannot be parsed",
                rawVersion);
        }
    }

    bool operator<(const TSpytVersion& rhs) const
    {
        return Major_ < rhs.Major_ || Major_ == rhs.Major_ && (Minor_ < rhs.Minor_ || Minor_ == rhs.Minor_ && Patch_ < rhs.Patch_);
    }

private:
    int Major_;
    int Minor_;
    int Patch_;
};

///////////////////////////////////////////////////////////////////////////////

class TSpytDiscovery
    : public TRefCounted
{
public:
    TSpytDiscovery(NApi::IClientPtr queryClient, TYPath discoveryPath)
        : QueryClient_(std::move(queryClient))
        , DiscoveryPath_(std::move(discoveryPath))
    { }

    std::vector<TString> GetModuleValues(const TString& moduleName) const
    {
        auto modulePath = Format("%v/discovery/%v", DiscoveryPath_, ToYPathLiteral(moduleName));
        auto rawResult = WaitFor(QueryClient_->ListNode(modulePath))
            .ValueOrThrow();
        return ConvertTo<std::vector<TString>>(rawResult);
    }

    std::optional<TString> GetModuleValue(const TString& moduleName) const
    {
        auto listResult = GetModuleValues(moduleName);
        if (listResult.size() > 1) {
            THROW_ERROR_EXCEPTION(
                "Invalid discovery directory for %v: at most 1 value expected, found %v",
                moduleName,
                listResult.size());
        }
        return listResult.size() == 1 ? std::optional(listResult[0]) : std::nullopt;
    }

    TString GetModuleValueOrThrow(const TString& moduleName) const
    {
        auto listResult = GetModuleValue(moduleName);
        if (!listResult) {
            THROW_ERROR_EXCEPTION(
                "Invalid discovery directory for %v: 1 value expected, found 0",
                moduleName);
        }
        return *listResult;
    }

private:
    const NApi::IClientPtr QueryClient_;
    const TYPath DiscoveryPath_;
};

DEFINE_REFCOUNTED_TYPE(TSpytDiscovery)
DECLARE_REFCOUNTED_CLASS(TSpytDiscovery)

///////////////////////////////////////////////////////////////////////////////

class TSpytQueryHandler
    : public TQueryHandlerBase
{
public:
    TSpytQueryHandler(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TSpytEngineConfigPtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery,
        const NHiveClient::TClusterDirectoryPtr& clusterDirectory,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Settings_(ConvertTo<TSpytSettingsPtr>(SettingsNode_))
        , Config_(config)
        , Cluster_(Settings_->Cluster.value_or(Config_->DefaultCluster))
        , NativeConnection_(clusterDirectory->GetConnectionOrThrow(Cluster_))
        , QueryClient_(NativeConnection_->CreateNativeClient(TClientOptions{.User = activeQuery.User}))
        , HttpClient_(CreateClient(Config_->HttpClient, NBus::TTcpDispatcher::Get()->GetXferPoller()))
        , Headers_(New<NHttp::THeaders>())
        , RefreshTokenExecutor_(New<TPeriodicExecutor>(GetCurrentInvoker(), BIND(&TSpytQueryHandler::RefreshToken, MakeWeak(this)), Config_->RefreshTokenPeriod))
    {
        if (Cluster_.Empty()) {
            THROW_ERROR_EXCEPTION("'cluster' setting is not specified");
        }
        auto discoveryPath = Settings_->DiscoveryPath.value_or(Config_->DefaultDiscoveryPath);
        if (discoveryPath.Empty()) {
            THROW_ERROR_EXCEPTION("'discovery_path' setting is not specified");
        }
        Headers_->Add("Content-Type", "application/json");
        Discovery_ = New<TSpytDiscovery>(QueryClient_, discoveryPath);
        ClusterVersion_ = Discovery_->GetModuleValueOrThrow("version");
    }

    void Start() override
    {
        YT_LOG_DEBUG("Starting SPYT query");
        Token_ = IssueToken();
        if (Token_) {
            RefreshTokenExecutor_->Start();
        }
        AsyncQueryResult_ = BIND(&TSpytQueryHandler::Execute, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
        AsyncQueryResult_.Subscribe(BIND(&TSpytQueryHandler::OnSpytResponse, MakeWeak(this)).Via(GetCurrentInvoker()));
    }

    void Abort() override
    {
        YT_LOG_DEBUG("Aborting SPYT query (SessionUrl: %v)", SessionUrl_);
        AsyncQueryResult_.Cancel(TError("Query aborted"));
        // After Abort() call there is Detach() call always. But double closing request is not the error.
        if (!SessionUrl_.Empty()) {
            CloseSession();
        }
    }

    void Detach() override
    {
        YT_LOG_DEBUG("Detaching SPYT query (SessionUrl: %v)", SessionUrl_);
        AsyncQueryResult_.Cancel(TError("Query detached"));
        if (!SessionUrl_.Empty()) {
            CloseSession();
        }
    }

private:
    const TSpytSettingsPtr Settings_;
    const TSpytEngineConfigPtr Config_;
    const TString Cluster_;
    const NApi::NNative::IConnectionPtr NativeConnection_;
    const NApi::NNative::IClientPtr QueryClient_;
    const NHttp::IClientPtr HttpClient_;
    const NHttp::THeadersPtr Headers_;
    const TPeriodicExecutorPtr RefreshTokenExecutor_;
    TSpytDiscoveryPtr Discovery_;
    TString ClusterVersion_;
    TFuture<TSharedRef> AsyncQueryResult_;
    TString SessionUrl_;
    std::optional<TString> Token_;
    // Not exists for YT scheduled jobs
    std::optional<TString> MasterWebUI_;

    void SetProgress(double progress_value)
    {
        YT_LOG_DEBUG("Reporting progress (Progress: %v)", progress_value);
        auto progress = BuildYsonStringFluently()
            .BeginMap()
                .OptionalItem("webui", MasterWebUI_)
                .Item("spyt_progress").Value(progress_value)
            .EndMap();
        OnProgress(std::move(progress));
    }

    TString GetLocation(const NHttp::IResponsePtr& response) const
    {
        return response->GetHeaders()->GetOrThrow("Location");
    }

    void ValidateStatusCode(const NHttp::IResponsePtr& response, const NHttp::EStatusCode& expected) const
    {
        if (response->GetStatusCode() != expected) {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy status code: expected %Qv, actual %Qv",
                expected,
                response->GetStatusCode());
        }
    }

    INodePtr ParseJson(const TSharedRef& data) const
    {
        TMemoryInput stream(data.Begin(), data.Size());
        auto factory = NYTree::CreateEphemeralNodeFactory();
        auto builder = NYTree::CreateBuilderFromFactory(factory.get());
        auto config = New<NJson::TJsonFormatConfig>();
        NJson::ParseJson(&stream, builder.get(), config);
        return builder->EndTree();
    }

    INodePtr ExecuteGetQuery(const TString& url) const
    {
        YT_LOG_DEBUG("Executing HTTP GET request (Url: %v)", url);
        auto rsp = WaitFor(HttpClient_->Get(url))
            .ValueOrThrow();
        YT_LOG_DEBUG("HTTP GET request executed (StatusCode: %v)", rsp->GetStatusCode());
        ValidateStatusCode(rsp, NHttp::EStatusCode::OK);
        auto jsonRoot = ParseJson(rsp->ReadAll());
        return jsonRoot;
    }

    TString WaitStatusChange(const TString& url, const TString& defaultState, const bool& reportProgress)
    {
        auto state = defaultState;
        while (state == defaultState) {
            TDelayedExecutor::WaitForDuration(Config_->StatusPollPeriod);
            auto jsonRoot = ExecuteGetQuery(url)->AsMap();
            state = jsonRoot->GetChildOrThrow("state")->AsString()->GetValue();
            auto rawProgress = jsonRoot->FindChildValue<double>("progress");
            if (reportProgress && rawProgress.has_value()) {
                SetProgress(rawProgress.value());
            }
        }
        return state;
    }

    TString GetReleaseMode() const
    {
        if (ClusterVersion_.Contains("SNAPSHOT")) {
            return "snapshots";
        } else {
            return "releases";
        }
    }

    TString GetSpytFile(const TString& filename) const
    {
        return Format("yt:/%v/spyt/%v/%v/%v", Config_->SpytHome, GetReleaseMode(), ClusterVersion_, filename);
    }

    TString SerializeYsonToJson(const INodePtr& ysonNode) const
    {
        TString result;
        TStringOutput resultOutput(result);
        auto jsonWriter = NJson::CreateJsonConsumer(&resultOutput);
        Serialize(ysonNode, jsonWriter.get());
        jsonWriter->Flush();
        return result;
    }

    THashMap<TString, TString> GetSparkConf() const
    {
        THashMap<TString, TString> sparkConf;
        if (Settings_->SparkConf) {
            sparkConf = ConvertTo<THashMap<TString, TString>>(TYsonString(Settings_->SparkConf.value()));
            if (sparkConf.contains("spark.hadoop.yt.user") || sparkConf.contains("spark.hadoop.yt.token")) {
                THROW_ERROR_EXCEPTION("Providing credentials is forbidden");
            }
        }
        auto versionInsert = sparkConf.emplace("spark.yt.version", ClusterVersion_).second;
        if (!versionInsert) {
            THROW_ERROR_EXCEPTION("Don't use 'spark.yt.version'. Use 'client_version' setting instead");
        }
        // COMPAT(alex-shishkin): Cluster >= 1.76.0 doesn't require client jars.
        if (TSpytVersion(ClusterVersion_) < TSpytVersion("1.76.0")) {
            auto jarsInsert = sparkConf.emplace("spark.yt.jars", GetSpytFile("spark-yt-data-source.jar")).second;
            if (!jarsInsert) {
                THROW_ERROR_EXCEPTION("Configuration of 'spark.yt.jars' is forbidden");
            }
        }
        auto pyFilesInsert = sparkConf.emplace("spark.yt.pyFiles", GetSpytFile("spyt.zip")).second;
        if (!pyFilesInsert) {
            THROW_ERROR_EXCEPTION("Configuration of 'spark.yt.pyFiles' is forbidden");
        }
        YT_LOG_DEBUG("Session spark conf prepared (Data: %v)", sparkConf);
        // Token insertion after data logging.
        if (Token_) {
            sparkConf.emplace("spark.hadoop.yt.user", User_);
            sparkConf.emplace("spark.hadoop.yt.token", *Token_);
        }
        return sparkConf;
    }

    TString MakeSessionStartQueryData() const
    {
        auto dataNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("kind").Value("spark")
                .Item("conf").Value(GetSparkConf())
            .EndMap();
        return SerializeYsonToJson(dataNode);
    }

    void StartSession(const TString& rootUrl)
    {
        auto data = MakeSessionStartQueryData();
        auto body = TSharedRef::FromString(data);
        auto rsp = WaitFor(HttpClient_->Post(rootUrl + "/sessions", body, Headers_))
            .ValueOrThrow();
        ValidateStatusCode(rsp, NHttp::EStatusCode::Created);
        YT_LOG_DEBUG("Session creation response received (Headers: %v)", rsp->GetHeaders()->Dump());
        SessionUrl_ = rootUrl + GetLocation(rsp);
        YT_LOG_DEBUG("Session creation response parsed (Url: %v)", SessionUrl_);
    }

    void WaitSessionReady()
    {
        auto state = WaitStatusChange(SessionUrl_, "starting", /* reportProgress */ false);
        if (state != "idle") {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy session state: expected \"idle\", found %Qv",
                state);
        }
    }

    void CloseSession() const
    {
        YT_LOG_DEBUG("Closing session");
        auto rsp = WaitFor(HttpClient_->Delete(SessionUrl_, Headers_))
            .ValueOrThrow();
        YT_LOG_DEBUG("Session closing response received (Code: %v)", rsp->GetStatusCode());
    }

    TString ConcatChunks(const std::vector<TString>& chunks) const
    {
        size_t summarySize = 0;
        for (const auto& chunk : chunks) {
            summarySize += chunk.Size();
        }
        TString result;
        result.reserve(summarySize);
        for (const auto& chunk : chunks) {
            result.append(chunk);
        }
        return result;
    }

    TSharedRef ExtractTableBytes(const TString& queryResult) const
    {
        auto encodedChunks = StringSplitter(queryResult).Split('\n').ToList<TString>();
        YT_LOG_DEBUG("Raw result received (LineCount: %v)", encodedChunks.size());

        std::vector<TString> tableChunks;
        for (size_t i = 0; i + 4 < encodedChunks.size(); i++) { // We must ignore last 4 lines, they don't contain result info
            tableChunks.push_back(Base64StrictDecode(encodedChunks[i]));
        }

        return TSharedRef::FromString(ConcatChunks(tableChunks));
    }

    TString ParseQueryOutput(const IMapNodePtr& outputNode) const
    {
        auto status = outputNode->GetChildOrThrow("status")->AsString()->GetValue();
        if (status == "ok") {
            auto dataNode = outputNode->GetChildOrThrow("data")->AsMap();
            return dataNode->GetChildValueOrThrow<TString>("text/plain");
        } else if (status == "error") {
            auto error = outputNode->GetChildValueOrThrow<TString>("evalue");
            auto traceback = outputNode->GetChildValueOrThrow<std::vector<TString>>("traceback");
            THROW_ERROR_EXCEPTION("Livy error: %v", error)
                << TErrorAttribute("traceback", traceback);
        } else {
            THROW_ERROR_EXCEPTION("Unknown Livy query status: %v", status);
        }
    }

    TString MakeStatementSubmitQueryData(const TString& sqlQuery) const
    {
        auto code = Format(
            "import tech.ytsaurus.spyt.serializers.GenericRowSerializer;"
            "val df = spark.sql(\"%v\").limit(%v);"
            "val rows = GenericRowSerializer.dfToYTFormatWithBase64(df);"
            "println(rows.mkString(\"\\n\"))",
            EscapeC(sqlQuery),
            Config_->RowCountLimit);
        auto dataNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("code").Value(code)
            .EndMap();
        return SerializeYsonToJson(dataNode);
    }

    TString SubmitStatement(const TString& rootUrl, const TString& sqlQuery) const
    {
        auto data = MakeStatementSubmitQueryData(sqlQuery);
        YT_LOG_DEBUG("Statement data prepared (Data: %v)", data);
        auto body = TSharedRef::FromString(data);
        auto rsp = WaitFor(HttpClient_->Post(SessionUrl_ + "/statements", body, Headers_))
            .ValueOrThrow();
        ValidateStatusCode(rsp, NHttp::EStatusCode::Created);
        YT_LOG_DEBUG("Statement submission response received (Headers: %v)", rsp->GetHeaders()->Dump());
        auto statementUrl = rootUrl + GetLocation(rsp);
        YT_LOG_DEBUG("Statement submission response parsed (Url: %v)", statementUrl);
        return statementUrl;
    }

    TString WaitStatementFinished(const TString& statementUrl)
    {
        auto state = WaitStatusChange(statementUrl, "running", /* reportProgress */ true);
        if (state != "available") {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy result state: expected \"available\", found %Qv",
                state);
        }
        auto queryResult = ExecuteGetQuery(statementUrl);
        auto outputNode = queryResult->AsMap()->GetChildOrThrow("output")->AsMap();
        return ParseQueryOutput(outputNode);
    }

    TString GetLivyServerUrl() const
    {
        YT_LOG_DEBUG("Listing livy discovery path");
        auto url = "http://" + Discovery_->GetModuleValueOrThrow("livy");
        YT_LOG_DEBUG("Livy server url received (Url: %v)", url);
        return url;
    }

    std::optional<TString> IssueToken() const
    {
        auto options = TIssueTemporaryTokenOptions{ .ExpirationTimeout = Config_->TokenExpirationTimeout };
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("query_id", QueryId_);
        attributes->Set("responsible", "query_tracker");
        YT_LOG_DEBUG("Requesting token (User: %v)", User_);
        auto rspOrError = WaitFor(QueryClient_->IssueTemporaryToken(User_, attributes, options));
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG("Token request failed (User: %v)", User_);
            return std::nullopt;
        }
        auto token = rspOrError.Value().Token;
        YT_LOG_DEBUG("Token received (User: %v)", User_);
        return token;
    }

    void RefreshToken() const
    {
        YT_VERIFY(Token_);
        YT_LOG_DEBUG("Refreshing token (User: %v)", User_);
        auto rspOrError = WaitFor(QueryClient_->RefreshTemporaryToken(User_, *Token_, {}));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING("Token refreshing failed (User: %v)", User_);
        } else {
            YT_LOG_DEBUG("Token refreshed (User: %v)", User_);
        }
    }

    void UpdateMasterWebUIUrl()
    {
        YT_LOG_DEBUG("Listing webui discovery path");
        auto url = Discovery_->GetModuleValue("webui");
        YT_LOG_DEBUG("Master webui url received (Url: %v)", url);
        if (url) {
            MasterWebUI_ = "http://" + *url;
        }
    }

    TSharedRef Execute()
    {
        SetProgress(0.0);
        UpdateMasterWebUIUrl();
        auto rootUrl = GetLivyServerUrl();
        YT_LOG_DEBUG("Starting session");
        StartSession(rootUrl);  // Session URL stores to the class field
        try {
            WaitSessionReady();
            YT_LOG_DEBUG("Sesison is ready, submitting statement (SessionUrl: %v)", SessionUrl_);
            auto statementUrl = SubmitStatement(rootUrl, Query_);
            auto queryResult = WaitStatementFinished(statementUrl);
            YT_LOG_DEBUG("Query finished, closing session");
            CloseSession();
            return ExtractTableBytes(queryResult);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Caught error while executing query; closing session");
            CloseSession();
            throw;
        }
    }

    void OnSpytResponse(const TErrorOr<TSharedRef>& queryResultOrError)
    {
        if (queryResultOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }
        SetProgress(1.0);
        if (!queryResultOrError.IsOK()) {
            OnQueryFailed(queryResultOrError);
            return;
        }
        OnQueryCompletedWire({TWireRowset{.Rowset = queryResultOrError.Value()}});
    }
};

class TSpytEngine
    : public IQueryEngine
{
public:
    TSpytEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("SpytEngineControl"))
        , ClusterDirectory_(DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection())->GetClusterDirectory())
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        return New<TSpytQueryHandler>(
            StateClient_,
            StateRoot_,
            Config_,
            activeQuery,
            ClusterDirectory_,
            ControlQueue_->GetInvoker());
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        Config_ = DynamicPointerCast<TSpytEngineConfig>(config);
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    TSpytEngineConfigPtr Config_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
};

IQueryEnginePtr CreateSpytEngine(IClientPtr stateClient, TYPath stateRoot)
{
    return New<TSpytEngine>(
        std::move(stateClient),
        std::move(stateRoot));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
