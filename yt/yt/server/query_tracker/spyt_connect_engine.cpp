#include "spyt_connect_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/formats/arrow_parser.h>
#include <yt/yt/library/arrow_adapter/public.h>

#include <pyspark/sql/connect/proto/base.grpc.pb.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>

#include <grpcpp/grpcpp.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NApi;
using namespace NArrow;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NFormats;
using namespace NHiveClient;
using namespace NRpc;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace spark::connect;

////////////////////////////////////////////////////////////////////////////////

struct TSpytConnectSettings
    : public TYsonStruct
{
    std::optional<std::string> Cluster;

    std::string Proxy;

    THashMap<TString, TString> SparkConf;

    THashMap<TString, TString> Params;

    int NumExecutors;

    int ExecutorCores;

    std::string ExecutorMemory;

    int DriverCores;

    ui64 DriverMemory;

    bool DriverReuse;

    REGISTER_YSON_STRUCT(TSpytConnectSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster", &TThis::Cluster)
            .Default();
        registrar.Parameter("proxy", &TThis::Proxy)
            .Default();
        registrar.Parameter("spark_conf", &TThis::SparkConf)
            .Default();
        registrar.Parameter("params", &TThis::Params)
            .Default();
        registrar.Parameter("num_executors", &TThis::NumExecutors)
            .Default(2);
        registrar.Parameter("executor_cores", &TThis::ExecutorCores)
            .Default(1);
        registrar.Parameter("executor_memory", &TThis::ExecutorMemory)
            .Default("4G");
        registrar.Parameter("driver_cores", &TThis::DriverCores)
            .Default(1);
        registrar.Parameter("driver_memory", &TThis::DriverMemory)
            .Default(1536_MB);
        registrar.Parameter("driver_reuse", &TThis::DriverReuse)
            .Default(true);

        registrar.Postprocessor([&] (TThis* config) {
            if (config->Proxy.empty() && config->Cluster) {
                config->Proxy = *config->Cluster;
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSpytConnectSettings);
DECLARE_REFCOUNTED_STRUCT(TSpytConnectSettings);

////////////////////////////////////////////////////////////////////////////////

struct TSpytConnectQueryResult
{
    const bool IsTruncated;
    const TArrowSchemaPtr ArrowSchema;
    const std::vector<TString> ArrowData;
};

////////////////////////////////////////////////////////////////////////////////

class TSpytConnectQueryHandler
    : public TQueryHandlerBase
{
public:
    TSpytConnectQueryHandler(
        const IClientPtr& stateClient,
        const TYPath& stateRoot,
        const IInvokerPtr& controlInvoker,
        const TSpytConnectEngineConfigPtr& config,
        const TActiveQuery& activeQuery,
        const TClusterDirectoryPtr& clusterDirectory,
        const TDuration notIndexedQueriesTTL)
    : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery, notIndexedQueriesTTL)
    , Settings_(ConvertTo<TSpytConnectSettingsPtr>(SettingsNode_))
    , SettingsHash_(ComputeSettingsHash())
    , Config_(config)
    , Cluster_(Settings_->Cluster.value_or(Config_->DefaultCluster))
    , NativeConnection_(clusterDirectory->GetConnectionOrThrow(Cluster_))
    , TargetClusterClient_(NativeConnection_->CreateNativeClient(NNative::TClientOptions::FromUser(activeQuery.User)))
    , GrpcContext_(std::make_unique<grpc::ClientContext>())
    , DriverOperationAnnotation_(Format("QT Spark connect driver for %v", User_))
    , DriverOperationId_(std::nullopt)
    { };

    void Start() override
    {
        YT_LOG_DEBUG("Starting SPYT Connect query (Query: %v)", Query_);
        OnQueryStarted();
        AsyncQueryResult_ = BIND(&TSpytConnectQueryHandler::Execute, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
        AsyncQueryResult_.Subscribe(BIND(&TSpytConnectQueryHandler::OnSpytConnectResponse, MakeWeak(this)).Via(GetCurrentInvoker()));
    };

    void Abort() override
    {
        YT_LOG_DEBUG("Aborting SPYT Connect query (User: %v, DriverOperationId: %v)", User_, DriverOperationId_);
        AsyncQueryResult_.Cancel(TError("Query aborted"));
        // After Abort() call there is Detach() call always.
    }

    void Detach() override
    {
        YT_LOG_DEBUG("Detaching SPYT Connect query (User: %v, DriverOperationId: %v)", User_, DriverOperationId_);
        AsyncQueryResult_.Cancel(TError("Query detached"));
        CancelQuery();
        if (!Settings_->DriverReuse && DriverOperationId_) {
            StopDriver(*DriverOperationId_);
        }
    }


private:
    const TSpytConnectSettingsPtr Settings_;
    const TString SettingsHash_;
    const TSpytConnectEngineConfigPtr Config_;
    const std::string Cluster_;
    const NNative::IConnectionPtr NativeConnection_;
    const NNative::IClientPtr TargetClusterClient_;
    const std::unique_ptr<grpc::ClientContext> GrpcContext_;
    const TString DriverOperationAnnotation_;

    std::optional<TOperationId> DriverOperationId_;
    TFuture<TSpytConnectQueryResult> AsyncQueryResult_;

    TString ComputeSettingsHash()
    {
        TSha256Hasher hasher{};
        std::vector<TString> confKeys;
        confKeys.reserve(Settings_->SparkConf.size());
        for(auto& kv : Settings_->SparkConf) {
            confKeys.push_back(kv.first);
        }
        std::sort(confKeys.begin(), confKeys.end());
        for(TString& key : confKeys) {
            hasher.Append(key);
            hasher.Append(Settings_->SparkConf[key]);
        }

        hasher.Append(Settings_->ExecutorMemory);
        hasher.Append(Format("%v;%v;%v:%v",
            Settings_->NumExecutors,
            Settings_->ExecutorCores,
            Settings_->DriverCores,
            Settings_->DriverMemory
        ));
        return hasher.GetHexDigestLowerCase();
    }

    std::string IssueToken() const
    {
        auto options = TIssueTemporaryTokenOptions{ .ExpirationTimeout = Config_->TokenExpirationTimeout };
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("query_id", QueryId_);
        attributes->Set("responsible", "query_tracker");
        YT_LOG_DEBUG("Requesting token (User: %v)", User_);
        auto rspOrError = WaitFor(TargetClusterClient_->IssueTemporaryToken(User_, attributes, options));
        auto token = rspOrError.ValueOrThrow().Token;
        YT_LOG_DEBUG("Token received (User: %v)", User_);
        return token;
    }

    std::string CreateCommand(std::string& sparkHome, std::string& sparkDistr, std::string& sparkConnectJar) const
    {
        TStringBuilder command{};
        if (!Config_->UseSquashfs) {
            command.AppendFormat("./setup-spyt-env.sh --spark-home . --spark-distributive %v.tgz && ", sparkDistr);
        }
        command.AppendFormat("%v/bin/spark-submit", sparkHome);
        auto master = Settings_->Proxy.empty() ? Config_->DefaultCluster : Settings_->Proxy;
        command.AppendFormat(" --master ytsaurus://%v", master);
        command.AppendString(" --deploy-mode client");
        command.AppendString(" --class org.apache.spark.sql.connect.ytsaurus.SpytConnectServer");
        command.AppendFormat(" --num-executors %v", Settings_->NumExecutors);
        command.AppendFormat(" --executor-cores %v", Settings_->ExecutorCores);
        command.AppendFormat(" --executor-memory %v", Settings_->ExecutorMemory);
        command.AppendFormat(" --queue %v", User_);
        command.AppendFormat(" --name \"Spark connect driver for %v\"", User_);
        command.AppendFormat(" --jars %v", sparkConnectJar);
        command.AppendFormat(" --conf spark.ytsaurus.squashfs.enabled=%v", Config_->UseSquashfs);
        command.AppendFormat(" --conf spark.driver.extraJavaOptions='-Djava.net.preferIPv6Addresses=%v'", Config_->PreferIpv6);
        command.AppendFormat(" --conf spark.connect.grpc.binding.port=%v", Config_->GrpcPort);
        command.AppendFormat(" --conf spark.ytsaurus.connect.token.refresh.period=%vs", Config_->RefreshTokenPeriod.Seconds());
        command.AppendString(" --conf spark.ytsaurus.arrow.stringToBinary=true");
        for (auto confEntry : Settings_->SparkConf) {
            command.AppendFormat(" --conf %v=%v", confEntry.first, confEntry.second);
        }
        command.AppendString(" spark-internal");

        return command.Flush();
    }

    std::string ReleaseType(std::string& SpytVersion) const
    {
        if (SpytVersion.contains("alpha") || SpytVersion.contains("beta") || SpytVersion.contains("rc")) {
            return "pre-releases";
        } else {
            return "releases";
        }
    }

    TYsonString CreateDriverSpec(const std::string& token) const
    {
        std::string sparkHome = Config_->UseSquashfs ? "/usr/lib/spark" : "$HOME/spark";
        std::string sparkDistr = Format("spark-%v-bin-hadoop3", Config_->SparkVersion);
        std::string sparkConnectJar = Format("spark-connect_2.12-%v.jar", Config_->SparkVersion);
        std::string command = CreateCommand(sparkHome, sparkDistr, sparkConnectJar);

        auto layerPaths = BuildYsonStringFluently()
            .BeginList();

        auto filePaths = BuildYsonStringFluently()
            .BeginList();

        std::string versionPath{Config_->SparkVersion};
        std::replace(versionPath.begin(), versionPath.end(), '.', '/');
        YT_LOG_DEBUG(
            "Creating Spark connect driver specification (SparkVersion: %v, SpytVersion: %v, LaunchCommand: %v)",
            Config_->SparkVersion,
            Config_->SpytVersion,
            command);

        auto releaseType = ReleaseType(Config_->SpytVersion);
        TYPath releaseConfigPath = Format("%v/%v/%v/%v",
            Config_->SpytConfigPath,
            releaseType,
            Config_->SpytVersion,
            Config_->SpytLaunchConfFile);
        auto releaseConfig = WaitFor(TargetClusterClient_->GetNode(releaseConfigPath)).ValueOrThrow();
        auto releaseConfigNode = ConvertToNode(releaseConfig)->AsMap();

        if (Config_->UseSquashfs) {
            layerPaths
                .Item().Value(Format("//home/spark/spyt/%v/%v/spyt-package.squashfs", releaseType, Config_->SpytVersion))
                .Item().Value(Format("//home/spark/distrib/%v/%v.squashfs", versionPath, sparkDistr));

            auto squashfsLayerPaths = releaseConfigNode->GetChildValueOrThrow<IListNodePtr>("squashfs_layer_paths")->AsList();
            for (int i = 0; i < squashfsLayerPaths->GetChildCount(); i++) {
                auto path = squashfsLayerPaths->GetChildValueOrThrow<std::string>(i);
                layerPaths.Item().Value(path);
            }
        } else {
            auto confLayerPaths = releaseConfigNode->GetChildValueOrThrow<IListNodePtr>("layer_paths")->AsList();
            for(int i = 0; i < confLayerPaths->GetChildCount(); i++) {
                auto path = confLayerPaths->GetChildValueOrThrow<std::string>(i);
                layerPaths.Item().Value(path);
            }

            filePaths
                .Item().Value(Format("//home/spark/distrib/%v/%v.tgz", versionPath, sparkDistr));
        }

        auto confFilePaths = releaseConfigNode->GetChildValueOrThrow<IListNodePtr>("file_paths")->AsList();
        for(int i = 0; i < confFilePaths->GetChildCount(); i++) {
            auto path = confFilePaths->GetChildValueOrThrow<std::string>(i);
            if (!Config_->UseSquashfs || !path.ends_with("spyt-package.zip")) {
                filePaths.Item().Value(path);
            }
        }

        filePaths
            .Item().Value(Format("//home/spark/distrib/%v/spark-connect_2.12-%v.jar", versionPath, Config_->SparkVersion));

        auto taskSpecBuilder = BuildYsonStringFluently().BeginMap()
            .Item("spark_connect_driver").BeginMap()
                .Item("command").Value(command)
                .Item("job_count").Value(1)
                .Item("cpu_limit").Value(Settings_->DriverCores)
                .Item("memory_limit").Value(Settings_->DriverMemory)
                .Item("layer_paths").Value(layerPaths.EndList())
                .Item("file_paths").Value(filePaths.EndList())
                .Item("environment").BeginMap()
                    .Item("JAVA_HOME").Value("/opt/jdk17")
                    .Item("SPARK_CONF_DIR").Value(Config_->UseSquashfs ? "/usr/lib/spyt/conf" : "spyt-package/conf")
                    .Item("SPARK_CONNECT_CLASSPATH").Value(sparkConnectJar)
                .EndMap();

        if (Settings_->SparkConf.contains("spark.ytsaurus.network.project")) {
            taskSpecBuilder.Item("network_project").Value(Settings_->SparkConf["spark.ytsaurus.network.project"]);
        }

        auto taskSpec = taskSpecBuilder.EndMap().EndMap();


        auto operationSpec = BuildYsonStringFluently();
        operationSpec.BeginMap()
            .Item("title").Value(Format("QT Spark connect server for %v", User_))
            .Item("annotations").BeginMap()
                .Item("spark_connect_server_id").Value(DriverOperationAnnotation_)
                .Item("settings_hash").Value(SettingsHash_)
            .EndMap()
            .Item("secure_vault").BeginMap()
                .Item("YT_TOKEN").Value(token)
            .EndMap()
            .Item("tasks").Value(taskSpec)
        .EndMap();
        return operationSpec.Finish();
    }

    TOperationId StartDriver(const std::string& token)
    {
        TYsonString spec = CreateDriverSpec(token);
        auto specString = spec.ToString();
        auto tokenPos = specString.find(token);
        YT_LOG_DEBUG("Created Spark connect driver specification (DriverSpecification: %v)",
            specString.replace(tokenPos, token.length(), "*****"));

        auto opFuture = TargetClusterClient_->StartOperation(EOperationType::Vanilla, spec);
        auto operationId = WaitFor(opFuture).ValueOrThrow();
        return operationId;
    }

    TString WaitSparkConnectEndpoint()
    {
        std::optional<TString> endpoint = std::nullopt;
        while (!endpoint) {
            TDelayedExecutor::WaitForDuration(Config_->StatusPollPeriod);
            auto operation = WaitFor(TargetClusterClient_->GetOperation(*DriverOperationId_)).ValueOrThrow();
            if (operation.State && IsOperationFinished(*operation.State)) {
                THROW_ERROR_EXCEPTION("Operation %v with spark connect endpoint is already in finished state", DriverOperationId_);
            }
            endpoint = GetAnnotation(operation, "spark_connect_endpoint");
        }
        YT_LOG_DEBUG("Received Spark connect endpoint (SparkConnectEndpoint: %v)", endpoint);
        return *endpoint;
    }

    std::optional<TString> GetAnnotation(TOperation& operation, std::string key)
    {
        if (operation.RuntimeParameters) {
                auto runtimeParameters = ConvertToNode(operation.RuntimeParameters)->AsMap();
                if (auto annotations = runtimeParameters->FindChild("annotations")) {
                    if (auto endpointNode = annotations->AsMap()->FindChild(key)) {
                        return endpointNode->AsString()->GetValue();
                    }
                }
            }
        return std::nullopt;
    }

    void StopDriver(TOperationId& operationId)
    {
        YT_LOG_DEBUG("Stopping Spark connect driver operation (User: %v, DriverOperationId: %v)", User_, operationId);
        auto completeFuture = TargetClusterClient_->CompleteOperation(operationId);
        WaitFor(completeFuture).ThrowOnError();
    }

    TString PrepareSession()
    {
        if (TryConnectToExistingSession()) {
            YT_LOG_DEBUG("Reusing existing driver operation (User: %v, DriverOperationId: %v)", User_, DriverOperationId_);
        } else {
            try {
                YT_LOG_DEBUG("Starting session");
                auto token = IssueToken();
                DriverOperationId_ = StartDriver(token);
                YT_LOG_DEBUG("Started new Spark connect driver operation (User: %v, DriverOperationId: %v)", User_, DriverOperationId_);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Caught error while preparing session");
                throw;
            }
        }
        TString endpoint = WaitSparkConnectEndpoint();
        return endpoint;
    }

    bool TryConnectToExistingSession()
    {
        TListOperationsOptions options{};
        options.StateFilter = EOperationState::Running;
        options.UserFilter = User_;
        options.SubstrFilter = DriverOperationAnnotation_;

        auto listOpsFuture = TargetClusterClient_->ListOperations(options);

        auto result = WaitFor(listOpsFuture).ValueOrThrow();
        if (!result.Operations.empty()) {
            TOperation& operation = result.Operations[0];
            TString runningOpSettingsHash = *GetAnnotation(operation, "settings_hash");
            YT_LOG_DEBUG("An operation already exists for a user (User: %v, RunningOpSettingsHash: %v, SettingsHash: %v)",
                User_, runningOpSettingsHash, SettingsHash_
            );
            if (runningOpSettingsHash.compare(SettingsHash_)) {
                // TODO(atokarew): Should we allow more than one session from a user with different configurations?
                YT_LOG_DEBUG("Stopping current spark connect driver operation to launch a new one (User: %v, DriverOperationId: %v)",
                    User_, *operation.Id);
                StopDriver(*operation.Id);
                return false;
            }
            DriverOperationId_ = *operation.Id;
            return true;
        }
        return false;
    }

    TSpytConnectQueryResult SubmitQuery(const TString& endpoint)
    {
        TGuid& driverOperaionGuid = (*DriverOperationId_).Underlying();
        // We need to format DriverOperationId as uuid;
        std::string sessionId = std::format("{:08x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:08x}",
            driverOperaionGuid.Parts32[0],
            driverOperaionGuid.ReversedParts8[4],
            driverOperaionGuid.ReversedParts8[5],
            driverOperaionGuid.ReversedParts8[6],
            driverOperaionGuid.ReversedParts8[7],
            driverOperaionGuid.ReversedParts8[8],
            driverOperaionGuid.ReversedParts8[9],
            driverOperaionGuid.ReversedParts8[10],
            driverOperaionGuid.ReversedParts8[11],
            driverOperaionGuid.Parts32[3]
        );

        ExecutePlanRequest request{};
        request.set_client_type("YTsaurus Query Tracker");
        request.set_session_id(sessionId);
        auto user_context = request.mutable_user_context();
        user_context->set_user_id(User_);
        user_context->set_user_name(User_);

        auto limit = request.mutable_plan()->mutable_root()->mutable_limit();
        limit->set_limit(Config_->RowCountLimit + 1);
        auto sql = limit->mutable_input()->mutable_sql();
        sql->set_query(Query_);
        auto sqlArgs = sql->mutable_args();

        for (const auto &[name, value] : Settings_->Params) {
            Expression_Literal valueLiteral{};
            valueLiteral.set_string(value);
            sqlArgs->emplace(std::move(name), std::move(valueLiteral));
        }

        YT_LOG_DEBUG("Created gRPC request for executing a plan (ExecutePlanRequest: %v)", request.DebugString());

        auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        auto service = SparkConnectService::NewStub(channel);

        auto responseReader = service->ExecutePlan(GrpcContext_.get(), request);
        ExecutePlanResponse responsePart;
        i64 rowsCount = 0;
        std::vector<TString> arrowData;
        std::optional<TArrowSchemaPtr> arrowSchema;
        while (responseReader->Read(&responsePart)) {
            if (responsePart.has_arrow_batch()) {
                auto data = responsePart.arrow_batch().Getdata();
                arrowData.push_back(data);
                rowsCount += responsePart.arrow_batch().Getrow_count();
                if (!arrowSchema) {
                    std::string strData = data;
                    auto buf = std::make_shared<arrow20::Buffer>(strData);
                    arrow20::io::BufferReader arrowBufferReader{buf};
                    auto ipcReaderResult = arrow20::ipc::RecordBatchStreamReader::Open(&arrowBufferReader);
                    if (ipcReaderResult.ok()) {
                        auto ipcReader = ipcReaderResult.ValueOrDie();
                        arrowSchema = ipcReader->schema();
                    } else {
                        THROW_ERROR_EXCEPTION("An error has occured during arrow schema reading");
                    }
                }
            }
        }

        auto status = responseReader->Finish();

        if (!status.ok()) {
            THROW_ERROR_EXCEPTION("gRPC request failed: status %v, message %v",
                 (i32)status.error_code(),
                 status.error_message());
        }

        if (!arrowSchema) {
            THROW_ERROR_EXCEPTION("Result doesn't contain a schema, probably there was an error.");
        }

        return TSpytConnectQueryResult{
            .IsTruncated = rowsCount > Config_->RowCountLimit,
            .ArrowSchema = *arrowSchema,
            .ArrowData = std::move(arrowData)
        };
    }

    void CancelQuery()
    {
        GrpcContext_->TryCancel();
        YT_LOG_DEBUG("Cancelling gRPC request for query");
    }

    TSpytConnectQueryResult Execute()
    {
        TString endpoint = PrepareSession();
        try {
            return SubmitQuery(endpoint);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Caught error while executing query");
            throw;
        }
    }

    void OnSpytConnectResponse(const TErrorOr<TSpytConnectQueryResult>& queryResultOrError)
    {
        if (!queryResultOrError.IsOK()) {
            OnQueryFailed(queryResultOrError);
            return;
        }

        auto result = queryResultOrError.Value();

        auto tableSchemaPtr = CreateYTTableSchemaFromArrowSchema(result.ArrowSchema);

        TBuildingValueConsumer valueConsumer(tableSchemaPtr, Logger, true);
        auto parser = CreateParserForArrow(&valueConsumer);
        YT_LOG_DEBUG("Processing response arrow batches (ArrowBatchesNum: %v)", result.ArrowData.size());
        for (size_t i = 0; i < result.ArrowData.size(); i++) {
            parser->Read(result.ArrowData[i]);
        }
        parser->Finish();

        std::vector<TUnversionedRow> rows = valueConsumer.GetRows();
        if (rows.size() > (size_t) Config_->RowCountLimit) {
            rows.pop_back();
        }

        OnQueryCompleted({TRowset{
            .Rowset = CreateRowset(valueConsumer.GetSchema(), MakeSharedRange(rows)),
            .IsTruncated = result.IsTruncated,
        }});
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSpytConnectEngine
    : public IQueryEngine
{
public:
    TSpytConnectEngine(IClientPtr stateClient, TYPath stateRoot) : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("SpytConnectEngineControl"))
        , ClusterDirectory_(DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection())->GetClusterDirectory())
    { }

    IQueryHandlerPtr StartOrAttachQuery(TActiveQuery activeQuery) override
    {
        if (Config_->SparkVersion.empty() || !Config_->SparkVersion.starts_with("3.5")) {
            THROW_ERROR_EXCEPTION("Spark version should not be empty, and only 3.5.x is supported");
        }
        if (Config_->SpytVersion.empty()) {
            THROW_ERROR_EXCEPTION("SPYT version should not be empty");
        }
        return New<TSpytConnectQueryHandler>(
            StateClient_,
            StateRoot_,
            ControlQueue_->GetInvoker(),
            Config_,
            activeQuery,
            ClusterDirectory_,
            NotIndexedQueriesTTL_
        );
    }

    void Reconfigure(const TEngineConfigBasePtr& config, const TDuration notIndexedQueriesTTL) override
    {
        Config_ = DynamicPointerCast<TSpytConnectEngineConfig>(config);
        NotIndexedQueriesTTL_ = notIndexedQueriesTTL;
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    const TClusterDirectoryPtr ClusterDirectory_;
    TSpytConnectEngineConfigPtr Config_;
    TDuration NotIndexedQueriesTTL_;
};

IQueryEnginePtr CreateSpytConnectEngine(IClientPtr stateClient, TYPath stateRoot)
{
    return New<TSpytConnectEngine>(
        std::move(stateClient),
        std::move(stateRoot));
}

} // namespace NYT::NQueryTracker
