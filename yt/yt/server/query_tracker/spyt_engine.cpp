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

    std::optional<TString> DiscoveryPath;

    std::optional<TString> ClientVersion;

    REGISTER_YSON_STRUCT(TSpytSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster", &TThis::Cluster)
            .Default();
        registrar.Parameter("discovery_path", &TThis::DiscoveryPath)
            .Default();
        registrar.Parameter("client_version", &TThis::ClientVersion)
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
    TSpytDiscovery(const NApi::IClientPtr& queryClient, const TString& discoveryPath)
        : QueryClient_(queryClient)
        , DiscoveryPath_(discoveryPath)
    { }

    std::vector<TString> GetModuleValues(const TString& moduleName)
    {
        auto modulePath = DiscoveryPath_ + "/discovery/" + moduleName;
        auto rawResult = WaitFor(QueryClient_->ListNode(modulePath)).ValueOrThrow();
        auto result = ConvertTo<std::vector<TString>>(rawResult);
        return result;
    }

    TString GetModuleValue(const TString& moduleName)
    {
        auto listResult = GetModuleValues(moduleName);
        if (listResult.size() != 1) {
            THROW_ERROR_EXCEPTION("Invalid discovery directory for %v; 1 value expected, found %v", moduleName, listResult.size());
        }
        return listResult[0];
    }

private:
    NApi::IClientPtr QueryClient_;
    TString DiscoveryPath_;
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
        , SpytHome_(config->SpytHome)
        , Cluster_(Settings_->Cluster.value_or(config->DefaultCluster))
        , NativeConnection_(clusterDirectory->GetConnectionOrThrow(Cluster_))
        , QueryClient_(NativeConnection_->CreateClient(TClientOptions{.User = activeQuery.User}))
        , Discovery_(New<TSpytDiscovery>(QueryClient_, Settings_->DiscoveryPath.value()))
        , HttpClient_(CreateClient(config->HttpClient, NBus::TTcpDispatcher::Get()->GetXferPoller()))
        , ClusterVersion_(Discovery_->GetModuleValue("version"))
        , ClientVersion_(Settings_->ClientVersion.value_or(ClusterVersion_))
        , StatusPollPeriod_(config->StatusPollPeriod)
    { }

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
        // Nothing smarter than that for now.
        AsyncQueryResult_.Cancel(TError("Query aborted"));
    }

    void Detach() override
    {
        // Nothing smarter than that for now.
        AsyncQueryResult_.Cancel(TError("Query detached"));
    }

private:
    const TSpytSettingsPtr Settings_;
    TString SpytHome_;
    TString Cluster_;
    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::IClientPtr QueryClient_;
    TSpytDiscoveryPtr Discovery_;
    NHttp::IClientPtr HttpClient_;
    TString ClusterVersion_;
    TString ClientVersion_;
    TDuration StatusPollPeriod_;
    TFuture<TSharedRef> AsyncQueryResult_;

    TString GetLocation(const NHttp::IResponsePtr& response)
    {
        return response->GetHeaders()->GetOrThrow("Location");
    }

    void ValidateStatusCode(const NHttp::IResponsePtr& response, const NHttp::EStatusCode& expected)
    {
        if (response->GetStatusCode() != expected) {
            THROW_ERROR_EXCEPTION("Unexpected Livy status code: expected %Qv, actual %Qv", expected, response->GetStatusCode());
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

    INodePtr ExecuteGetQuery(const TString& url, const NHttp::THeadersPtr& headers)
    {
        YT_LOG_DEBUG("Executing HTTP GET request (Url: %v)", url);
        auto rsp = WaitFor(HttpClient_->Get(url, headers)).ValueOrThrow();
        YT_LOG_DEBUG("HTTP GET request executed (StatusCode: %v)", rsp->GetStatusCode());
        ValidateStatusCode(rsp, NHttp::EStatusCode::OK);
        auto jsonRoot = ParseJson(rsp->ReadAll());
        return jsonRoot;
    }

    TString WaitStatusChange(const TString& url, const NHttp::THeadersPtr& headers, const TString& defaultState)
    {
        auto state = defaultState;
        while (state == defaultState) {
            TDelayedExecutor::WaitForDuration(StatusPollPeriod_);
            auto jsonRoot = ExecuteGetQuery(url, headers)->AsMap();
            state = jsonRoot->GetChildOrThrow("state")->AsString()->GetValue();
        }
        return state;
    }

    TString GetReleaseMode()
    {
        if (ClientVersion_.Contains("SNAPSHOT")) {
            return "snapshots";
        } else {
            return "releases";
        }
    }

    TString GetDataSourcePath()
    {
        return Format("yt:/%v/spyt/%v/%v/spark-yt-data-source.jar", SpytHome_, GetReleaseMode(), ClientVersion_);
    }

    TString GetPythonPackagePath()
    {
        return Format("yt:/%v/spyt/%v/%v/spyt.zip", SpytHome_, GetReleaseMode(), ClientVersion_);
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

    TString MakeSessionStartQueryData()
    {
        auto dataNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("kind").Value("spark")
                .Item("conf")
                    .BeginMap()
                        .Item("spark.yt.version").Value(ClientVersion_)
                        .Item("spark.yt.jars").Value(GetDataSourcePath())
                        .Item("spark.yt.pyFiles").Value(GetPythonPackagePath())
                    .EndMap()
            .EndMap();
        return SerializeYsonToJson(dataNode);
    }

    TString StartSession(const TString& rootUrl, const NHttp::THeadersPtr& headers)
    {
        auto data = MakeSessionStartQueryData();
        YT_LOG_DEBUG("Session data prepared (Data: %v)", data);
        auto body = TSharedRef::FromString(data);
        auto rsp = WaitFor(HttpClient_->Post(rootUrl + "/sessions", body, headers)).ValueOrThrow();
        ValidateStatusCode(rsp, NHttp::EStatusCode::Created);
        YT_LOG_DEBUG("Session creation response received (Headers: %v)", rsp->GetHeaders()->Dump());
        auto sessionUrl = rootUrl + GetLocation(rsp);
        YT_LOG_DEBUG("Session creation response parsed (Url: %v)", sessionUrl);
        return sessionUrl;
    }

    void WaitSessionReady(const TString& sessionUrl, const NHttp::THeadersPtr& headers)
    {
        auto state = WaitStatusChange(sessionUrl, headers, "starting");
        if (state != "idle") {
            THROW_ERROR_EXCEPTION("Unexpected Livy session state: expected \"idle\", found %Qv", state);
        }
    }

    void CloseSession(const TString& sessionUrl, const NHttp::THeadersPtr& headers)
    {
        YT_LOG_DEBUG("Closing session");
        auto rsp = WaitFor(HttpClient_->Delete(sessionUrl, headers)).ValueOrThrow();
        YT_LOG_DEBUG("Session closing response received (Code: %v)", rsp->GetStatusCode());
    }

    TString ConcatChunks(const std::vector<TString>& chunks)
    {
        size_t summary_size = 0;
        for (const auto& chunk : chunks) {
            summary_size += chunk.Size();
        }
        TString result;
        result.reserve(summary_size);
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
            auto textNode = dataNode->GetChildOrThrow("text/plain")->AsString();
            return textNode->GetValue();
        } else if (status == "error") {
            auto error = outputNode->GetChildOrThrow("evalue")->AsString()->GetValue();
            THROW_ERROR_EXCEPTION("Livy error: %v", error);
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
            sqlQuery,
            Config_->RowCountLimit);
        auto dataNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("code").Value(code)
            .EndMap();
        return SerializeYsonToJson(dataNode);
    }

    TString SubmitStatement(const TString& rootUrl, const TString& sessionUrl, const NHttp::THeadersPtr& headers, const TString& sqlQuery)
    {
        auto data = MakeStatementSubmitQueryData(sqlQuery);
        YT_LOG_DEBUG("Statement data prepared (Data: %v)", data);
        auto body = TSharedRef::FromString(data);
        auto rsp = WaitFor(HttpClient_->Post(sessionUrl + "/statements", body, headers)).ValueOrThrow();
        ValidateStatusCode(rsp, NHttp::EStatusCode::Created);
        YT_LOG_DEBUG("Statement submission response received (Headers: %v)", rsp->GetHeaders()->Dump());
        auto statementUrl = rootUrl + GetLocation(rsp);
        YT_LOG_DEBUG("Statement submission response parsed (Url: %v)", statementUrl);
        return statementUrl;
    }

    TString WaitStatementFinished(const TString& statementUrl, const NHttp::THeadersPtr& headers)
    {
        auto state = WaitStatusChange(statementUrl, headers, "running");
        if (state != "available") {
            THROW_ERROR_EXCEPTION("Unexpected Livy result state: expected \"available\", found %Qv", state);
        }
        auto queryResult = ExecuteGetQuery(statementUrl, headers);
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
        auto headers = New<NHttp::THeaders>();
        headers->Add("Content-Type", "application/json");
        YT_LOG_DEBUG("Starting session");
        auto sessionUrl = StartSession(rootUrl, headers);
        try {
            WaitSessionReady(sessionUrl, headers);
            YT_LOG_DEBUG("Sesison is ready, submitting statement (SessionUrl: %v)", sessionUrl);
            auto statementUrl = SubmitStatement(rootUrl, sessionUrl, headers, Query_);
            auto queryResult = WaitStatementFinished(statementUrl, headers);
            YT_LOG_DEBUG("Query finished, closing session");
            CloseSession(sessionUrl, headers);
            return ExtractTableBytes(queryResult);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Caught error while executing query; closing session");
            CloseSession(sessionUrl, headers);
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
        return New<TSpytQueryHandler>(StateClient_, StateRoot_, Config_, activeQuery, ClusterDirectory_, ControlQueue_->GetInvoker());
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
    NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
};

IQueryEnginePtr CreateSpytEngine(const IClientPtr& stateClient, const TYPath& stateRoot)
{
    return New<TSpytEngine>(stateClient, stateRoot);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
