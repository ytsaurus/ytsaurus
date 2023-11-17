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

    std::optional<TString> ClientVersion;

    std::optional<TString> SparkConf;

    REGISTER_YSON_STRUCT(TSpytSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster", &TThis::Cluster)
            .Default();
        registrar.Parameter("discovery_path", &TThis::DiscoveryPath)
            .Default();
        registrar.Parameter("client_version", &TThis::ClientVersion)
            .Default();
        registrar.Parameter("spark_conf", &TThis::SparkConf)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSpytSettings)
DECLARE_REFCOUNTED_CLASS(TSpytSettings)

///////////////////////////////////////////////////////////////////////////////

class TSpytDiscovery
    : public TRefCounted
{
public:
    TSpytDiscovery(NApi::IClientPtr queryClient, TYPath discoveryPath)
        : QueryClient_(std::move(queryClient))
        , DiscoveryPath_(std::move(discoveryPath))
    { }

    std::vector<TString> GetModuleValues(const TString& moduleName)
    {
        auto modulePath = Format("%v/discovery/%v", DiscoveryPath_, ToYPathLiteral(moduleName));
        auto rawResult = WaitFor(QueryClient_->ListNode(modulePath))
            .ValueOrThrow();
        return ConvertTo<std::vector<TString>>(rawResult);
    }

    TString GetModuleValue(const TString& moduleName)
    {
        auto listResult = GetModuleValues(moduleName);
        if (listResult.size() != 1) {
            THROW_ERROR_EXCEPTION(
                "Invalid discovery directory for %v: 1 value expected, found %v",
                moduleName,
                listResult.size());
        }
        return listResult[0];
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
        , QueryClient_(NativeConnection_->CreateClient(TClientOptions{.User = activeQuery.User}))
        , HttpClient_(CreateClient(Config_->HttpClient, NBus::TTcpDispatcher::Get()->GetXferPoller()))
        , Headers_(New<NHttp::THeaders>())
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
        ClusterVersion_ = Discovery_->GetModuleValue("version");
    }

    void Start() override
    {
        YT_LOG_DEBUG("Starting SPYT query");
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
    const NApi::IClientPtr QueryClient_;
    const NHttp::IClientPtr HttpClient_;
    const NHttp::THeadersPtr Headers_;
    TSpytDiscoveryPtr Discovery_;
    TString ClusterVersion_;
    TFuture<TSharedRef> AsyncQueryResult_;
    TString SessionUrl_;

    TString GetLocation(const NHttp::IResponsePtr& response)
    {
        return response->GetHeaders()->GetOrThrow("Location");
    }

    void ValidateStatusCode(const NHttp::IResponsePtr& response, const NHttp::EStatusCode& expected)
    {
        if (response->GetStatusCode() != expected) {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy status code: expected %Qv, actual %Qv",
                expected,
                response->GetStatusCode());
        }
    }

    INodePtr ParseJson(const TSharedRef& data)
    {
        TMemoryInput stream(data.Begin(), data.Size());
        auto factory = NYTree::CreateEphemeralNodeFactory();
        auto builder = NYTree::CreateBuilderFromFactory(factory.get());
        auto config = New<NJson::TJsonFormatConfig>();
        NJson::ParseJson(&stream, builder.get(), config);
        return builder->EndTree();
    }

    INodePtr ExecuteGetQuery(const TString& url)
    {
        YT_LOG_DEBUG("Executing HTTP GET request (Url: %v)", url);
        auto rsp = WaitFor(HttpClient_->Get(url))
            .ValueOrThrow();
        YT_LOG_DEBUG("HTTP GET request executed (StatusCode: %v)", rsp->GetStatusCode());
        ValidateStatusCode(rsp, NHttp::EStatusCode::OK);
        auto jsonRoot = ParseJson(rsp->ReadAll());
        return jsonRoot;
    }

    TString WaitStatusChange(const TString& url, const TString& defaultState)
    {
        auto state = defaultState;
        while (state == defaultState) {
            TDelayedExecutor::WaitForDuration(Config_->StatusPollPeriod);
            auto jsonRoot = ExecuteGetQuery(url)->AsMap();
            state = jsonRoot->GetChildOrThrow("state")->AsString()->GetValue();
        }
        return state;
    }

    TString GetClientVersion()
    {
        return Settings_->ClientVersion.value_or(ClusterVersion_);
    }

    TString GetReleaseMode()
    {
        if (GetClientVersion().Contains("SNAPSHOT")) {
            return "snapshots";
        } else {
            return "releases";
        }
    }

    TString GetSpytFile(const TString& Filename)
    {
        return Format("yt:/%v/spyt/%v/%v/%v", Config_->SpytHome, GetReleaseMode(), GetClientVersion(), Filename);
    }

    TString SerializeYsonToJson(const INodePtr& ysonNode)
    {
        TString result;
        TStringOutput resultOutput(result);
        auto jsonWriter = NJson::CreateJsonConsumer(&resultOutput);
        Serialize(ysonNode, jsonWriter.get());
        jsonWriter->Flush();
        return result;
    }

    THashMap<TString, TString> GetSparkConf()
    {
        THashMap<TString, TString> sparkConf;
        if (Settings_->SparkConf) {
            sparkConf = ConvertTo<THashMap<TString, TString>>(TYsonString(Settings_->SparkConf.value()));
        }
        auto versionInsert = sparkConf.emplace("spark.yt.version", GetClientVersion()).second;
        if (!versionInsert) {
            THROW_ERROR_EXCEPTION("Don't use 'spark.yt.version'. Use 'client_version' setting instead");
        }
        auto jarsInsert = sparkConf.emplace("spark.yt.jars", GetSpytFile("spark-yt-data-source.jar")).second;
        auto pyFilesInsert = sparkConf.emplace("spark.yt.pyFiles", GetSpytFile("spyt.zip")).second;
        if (!jarsInsert || !pyFilesInsert) {
            THROW_ERROR_EXCEPTION("Configuration of 'spark.yt.jars' and 'spark.yt.pyFiles' is forbidden");
        }
        return sparkConf;
    }

    TString MakeSessionStartQueryData()
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
        YT_LOG_DEBUG("Session data prepared (Data: %v)", data);
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
        auto state = WaitStatusChange(SessionUrl_, "starting");
        if (state != "idle") {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy session state: expected \"idle\", found %Qv",
                state);
        }
    }

    void CloseSession()
    {
        YT_LOG_DEBUG("Closing session");
        auto rsp = WaitFor(HttpClient_->Delete(SessionUrl_, Headers_))
            .ValueOrThrow();
        YT_LOG_DEBUG("Session closing response received (Code: %v)", rsp->GetStatusCode());
    }

    TString ConcatChunks(const std::vector<TString>& chunks)
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

    TSharedRef ExtractTableBytes(const TString& queryResult)
    {
        auto encodedChunks = StringSplitter(queryResult).Split('\n').ToList<TString>();
        YT_LOG_DEBUG("Raw result received (LineCount: %v)", encodedChunks.size());

        std::vector<TString> tableChunks;
        for (size_t i = 0; i + 4 < encodedChunks.size(); i++) { // We must ignore last 4 lines, they don't contain result info
            tableChunks.push_back(Base64StrictDecode(encodedChunks[i]));
        }

        return TSharedRef::FromString(ConcatChunks(tableChunks));
    }

    TString ParseQueryOutput(const IMapNodePtr& outputNode)
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

    TString MakeStatementSubmitQueryData(const TString& sqlQuery)
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

    TString SubmitStatement(const TString& rootUrl, const TString& sqlQuery)
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
        auto state = WaitStatusChange(statementUrl, "running");
        if (state != "available") {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy result state: expected \"available\", found %Qv",
                state);
        }
        auto queryResult = ExecuteGetQuery(statementUrl);
        auto outputNode = queryResult->AsMap()->GetChildOrThrow("output")->AsMap();
        return ParseQueryOutput(outputNode);
    }

    TString GetLivyServerUrl()
    {
        YT_LOG_DEBUG("Listing livy discovery path");
        auto url = "http://" + Discovery_->GetModuleValue("livy");
        YT_LOG_DEBUG("Livy server url received (Url: %v)", url);
        return url;
    }

    TSharedRef Execute()
    {
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
        if (!queryResultOrError.IsOK()) {
            OnQueryFailed(queryResultOrError);
            return;
        }
        OnQueryCompletedWire({queryResultOrError.Value()});
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

    void OnDynamicConfigChanged(const TEngineConfigBasePtr& config) override
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
