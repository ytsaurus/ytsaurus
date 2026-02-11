#include "spyt_connect_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/library/formats/arrow_parser.h>
#include <yt/yt/library/arrow_adapter/public.h>

#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <pyspark/sql/connect/proto/base.grpc.pb.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>

#include <grpcpp/grpcpp.h>

#include <util/generic/guid.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NApi;
using namespace NArrow;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NFormats;
using namespace NHiveClient;
using namespace NLogging;
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

    std::optional<TYPath> DiscoveryPath;

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
        registrar.Parameter("discovery_path", &TThis::DiscoveryPath)
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

struct IConnectServerLauncher
    : public TRefCounted
{
    virtual std::string GetSessionId() = 0;

    virtual bool TryConnectToExistingSession() = 0;

    virtual void StartDriver(const std::string& token) = 0;

    virtual std::string WaitForSparkConnectEndpoint() = 0;

    virtual void Abort() = 0;

    virtual void Detach() = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnectServerLauncher)
DECLARE_REFCOUNTED_STRUCT(IConnectServerLauncher)

////////////////////////////////////////////////////////////////////////////////

class TConnectServerLauncherBase
    : public IConnectServerLauncher
{
protected:
    const TSpytConnectEngineConfigPtr Config_;
    const TSpytConnectSettingsPtr Settings_;
    const std::string User_;
    const NNative::IClientPtr TargetClusterClient_;

    const TLogger Logger;

    std::string SettingsHash_;

    TConnectServerLauncherBase(
        TSpytConnectEngineConfigPtr config,
        TSpytConnectSettingsPtr settings,
        std::string user,
        NNative::IClientPtr clusterClient,
        TLogger logger)
        : Config_(std::move(config))
        , Settings_(std::move(settings))
        , User_(std::move(user))
        , TargetClusterClient_(std::move(clusterClient))
        , Logger(std::move(logger))
    {
        SettingsHash_ = ComputeSettingsHash();
    }

    const std::string FormatAsUUID(const TGuid& guid) const
    {
        TGUID uuid;
        std::copy(guid.Parts32, guid.Parts32 + 4, uuid.dw);
        return uuid.AsUuidString();
    }

private:
    std::string ComputeSettingsHash() const
    {
        TSha256Hasher hasher;
        std::vector<std::string> confKeys;
        confKeys.reserve(Settings_->SparkConf.size());
        for (const auto& kv : Settings_->SparkConf) {
            confKeys.push_back(kv.first);
        }
        std::sort(confKeys.begin(), confKeys.end());
        for (const auto& key : confKeys) {
            hasher.Append(key);
            hasher.Append(Settings_->SparkConf[key]);
        }

        hasher.Append(Settings_->ExecutorMemory);
        hasher.Append(Format("%v;%v;%v;%v",
            Settings_->NumExecutors,
            Settings_->ExecutorCores,
            Settings_->DriverCores,
            Settings_->DriverMemory
        ));
        hasher.Append(User_);
        return hasher.GetHexDigestLowerCase();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStandaloneConnectServerLauncher
    : public TConnectServerLauncherBase
{
public:
    TStandaloneConnectServerLauncher(
        TSpytConnectEngineConfigPtr config,
        TSpytConnectSettingsPtr settings,
        std::string user,
        NNative::IClientPtr clusterClient,
        TLogger logger)
        : TConnectServerLauncherBase(std::move(config), std::move(settings), std::move(user), std::move(clusterClient), std::move(logger))
        , DriverOperationDescription_(Format("QT Spark connect driver for %v", User_))
    { }

    std::string GetSessionId() override
    {
        // We need to format DriverOperationId as uuid.
        return FormatAsUUID(DriverOperationId_.Underlying());
    }

    bool TryConnectToExistingSession() override
    {
        TListOperationsOptions options;
        options.StateFilter = EOperationState::Running;
        options.UserFilter = User_;
        options.SubstrFilter = DriverOperationDescription_;

        auto listOpsFuture = TargetClusterClient_->ListOperations(options);

        auto result = WaitFor(listOpsFuture)
            .ValueOrThrow();
        if (!result.Operations.empty()) {
            auto operation = result.Operations[0];
            if (!operation.Id) {
                THROW_ERROR_EXCEPTION("Operation doesn't have an Id field")
                    << TErrorAttribute("operation", operation);
            }
            auto operationId = *operation.Id;
            auto runningOpSettingsHash = GetAnnotation(operation, "settings_hash");
            if (runningOpSettingsHash) {
                YT_LOG_DEBUG("An operation already exists for a user (User: %v, RunningOpSettingsHash: %v, SettingsHash: %v, OperationId: %v)",
                    User_,
                    *runningOpSettingsHash,
                    SettingsHash_,
                    operationId);
            }
            if (!runningOpSettingsHash || SettingsHash_ != *runningOpSettingsHash) {
                // TODO(atokarew): Should we allow more than one session from a user with different configurations?
                YT_LOG_DEBUG("Stopping current Spark connect driver operation to launch a new one (User: %v, DriverOperationId: %v)",
                    User_,
                    operationId);
                StopDriverOperation(operationId);
                return false;
            }
            DriverOperationId_ = operationId;
            YT_LOG_DEBUG("Reusing existing driver operation (User: %v, DriverOperationId: %v)",
                User_,
                DriverOperationId_);
            return true;
        }
        return false;
    }

    void StartDriver(const std::string& token) override
    {
        auto specNode = CreateDriverSpec(token);
        auto spec = ConvertToYsonString(specNode);
        specNode->RemoveChild("secure_vault");
        auto reducedSpec = ConvertToYsonString(specNode);
        YT_LOG_DEBUG("Created Spark connect driver specification (DriverSpecification: %v)",
            reducedSpec);

        auto opFuture = TargetClusterClient_->StartOperation(EOperationType::Vanilla, spec);
        DriverOperationId_ = WaitFor(opFuture)
            .ValueOrThrow();
        YT_LOG_DEBUG("Started new Spark connect driver operation (User: %v, DriverOperationId: %v)",
            User_,
            DriverOperationId_);
    }

    std::string WaitForSparkConnectEndpoint() override
    {
        std::optional<std::string> endpoint;
        while (!endpoint) {
            TDelayedExecutor::WaitForDuration(Config_->StatusPollPeriod);
            auto operation = WaitFor(TargetClusterClient_->GetOperation(DriverOperationId_))
                .ValueOrThrow();
            if (operation.State && IsOperationFinished(*operation.State)) {
                THROW_ERROR_EXCEPTION("Operation %v with Spark connect endpoint is already in finished state",
                    DriverOperationId_);
            }
            endpoint = GetAnnotation(operation, "spark_connect_endpoint");
        }
        YT_LOG_DEBUG("Received Spark connect endpoint (SparkConnectEndpoint: %v)",
            endpoint);
        return *endpoint;
    }

    void Abort() override
    {
        YT_LOG_DEBUG("Aborting Spark connect query (User: %v, DriverOperationId: %v)",
            User_,
            DriverOperationId_);
    }

    void Detach() override
    {
        if (!Settings_->DriverReuse && DriverOperationId_) {
            StopDriverOperation(DriverOperationId_);
        }
    }

private:
    const TString DriverOperationDescription_;

    TOperationId DriverOperationId_;

    std::string CreateCommand(const std::string& sparkHome, const std::string& sparkDistr) const
    {
        TStringBuilder command;
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
        command.AppendFormat(" --conf spark.ytsaurus.squashfs.enabled=%v", Config_->UseSquashfs);
        command.AppendFormat(" --conf spark.driver.extraJavaOptions='-Djava.net.preferIPv6Addresses=%v'", Config_->PreferIpv6);
        command.AppendFormat(" --conf spark.connect.grpc.binding.port=%v", Config_->GrpcPort);
        command.AppendFormat(" --conf spark.ytsaurus.connect.token.refresh.period=%vs", Config_->RefreshTokenPeriod.Seconds());
        command.AppendString(" --conf spark.ytsaurus.arrow.stringToBinary=true");
        command.AppendString(" --conf spark.ytsaurus.driver.operation.id=$YT_OPERATION_ID");
        for (auto confEntry : Settings_->SparkConf) {
            command.AppendFormat(" --conf %v=%v", confEntry.first, confEntry.second);
        }
        command.AppendString(" spark-internal");

        return command.Flush();
    }

    std::string GetReleaseType(const std::string& spytVersion) const
    {
        if (spytVersion.contains("alpha") || spytVersion.contains("beta") || spytVersion.contains("rc")) {
            return "pre-releases";
        } else {
            return "releases";
        }
    }

    IMapNodePtr CreateDriverSpec(const std::string& token) const
    {
        std::string sparkHome = Config_->UseSquashfs ? "/usr/lib/spark" : "$HOME/spark";
        std::string sparkDistr = Format("spark-%v-bin-hadoop3", Config_->SparkVersion);
        std::string sparkConnectJar = Format("spark-connect_2.12-%v.jar", Config_->SparkVersion);
        std::string command = CreateCommand(sparkHome, sparkDistr);

        std::string versionPath{Config_->SparkVersion};
        std::replace(versionPath.begin(), versionPath.end(), '.', '/');

        auto releaseType = GetReleaseType(Config_->SpytVersion);

        TYPath releaseConfigPath = Format("%v/%v/%v/%v",
            Config_->SpytConfigPath,
            releaseType,
            Config_->SpytVersion,
            Config_->SpytLaunchConfFile);
        auto releaseConfig = WaitFor(TargetClusterClient_->GetNode(releaseConfigPath))
            .ValueOrThrow();
        auto releaseConfigNode = ConvertToNode(releaseConfig)->AsMap();

        YT_LOG_DEBUG(
            "Creating Spark connect driver specification (SparkVersion: %v, SpytVersion: %v, LaunchCommand: %v)",
            Config_->SparkVersion,
            Config_->SpytVersion,
            command);

        std::vector<std::string> layerPaths;
        std::vector<std::string> filePaths;

        if (Config_->UseSquashfs) {
            auto squashfsLayerPaths = releaseConfigNode->GetChildValueOrThrow<std::vector<std::string>>("squashfs_layer_paths");

            layerPaths.reserve(squashfsLayerPaths.size() + 2);
            layerPaths.push_back(Format("//home/spark/spyt/%v/%v/spyt-package.squashfs", releaseType, Config_->SpytVersion));
            layerPaths.push_back(Format("//home/spark/distrib/%v/%v.squashfs", versionPath, sparkDistr));
            layerPaths.insert(layerPaths.end(), squashfsLayerPaths.begin(), squashfsLayerPaths.end());
        } else {
            auto confLayerPaths = releaseConfigNode->GetChildValueOrThrow<std::vector<std::string>>("layer_paths");
            layerPaths.reserve(confLayerPaths.size());
            layerPaths.insert(layerPaths.end(), confLayerPaths.begin(), confLayerPaths.end());

            filePaths.reserve(2);
            filePaths.push_back(Format("//home/spark/distrib/%v/%v.tgz", versionPath, sparkDistr));
            filePaths.push_back(Format("//home/spark/distrib/%v/spark-connect_2.12-%v.jar", versionPath, Config_->SparkVersion));
        }

        auto confFilePaths = releaseConfigNode->GetChildValueOrThrow<std::vector<std::string>>("file_paths");
        filePaths.reserve(filePaths.size() + confFilePaths.size());
        for (size_t i = 0; i < confFilePaths.size(); i++) {
            auto path = confFilePaths[i];
            if (!Config_->UseSquashfs || !path.ends_with("spyt-package.zip")) {
                filePaths.push_back(path);
            }
        }

        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("title").Value(Format("QT Spark connect server for %v", User_))
                .Item("annotations").BeginMap()
                    .Item("spark_connect_server_description").Value(DriverOperationDescription_)
                    .Item("settings_hash").Value(SettingsHash_)
                .EndMap()
                .Item("secure_vault").BeginMap()
                    .Item("YT_TOKEN").Value(token)
                .EndMap()
                .Item("tasks").BeginMap()
                    .Item("driver").BeginMap()
                        .Item("command").Value(command)
                        .Item("job_count").Value(1)
                        .Item("cpu_limit").Value(Settings_->DriverCores)
                        .Item("memory_limit").Value(Settings_->DriverMemory)
                        .Item("layer_paths").DoListFor(layerPaths, [&] (auto fluent, auto item) {
                            fluent.Item().Value(item);
                        })
                        .Item("file_paths").DoListFor(filePaths, [&] (auto fluent, auto item) {
                            fluent.Item().Value(item);
                        })
                        .Item("environment").BeginMap()
                            .Item("JAVA_HOME").Value("/opt/jdk17")
                            .Item("SPARK_CONF_DIR").Value(Config_->UseSquashfs ? "/usr/lib/spyt/conf" : "spyt-package/conf")
                        .EndMap()
                        .DoIf(Settings_->SparkConf.contains("spark.ytsaurus.network.project"), [&] (auto fluent) {
                            fluent
                                .Item("network_project").Value(Settings_->SparkConf["spark.ytsaurus.network.project"]);
                        })
                    .EndMap()
                .EndMap()
            .EndMap()->AsMap();
    }

    std::optional<std::string> GetAnnotation(const TOperation& operation, const std::string& key)
    {
        if (operation.RuntimeParameters) {
            return TryGetString(operation.RuntimeParameters.AsStringBuf(), Format("/annotations/%v", key));
        }
        return std::nullopt;
    }

    void StopDriverOperation(const TOperationId& operationId)
    {
        YT_LOG_DEBUG("Stopping Spark connect driver operation (User: %v, DriverOperationId: %v)",
            User_,
            operationId);
        WaitFor(TargetClusterClient_->CompleteOperation(operationId))
            .ThrowOnError();
    }
};


////////////////////////////////////////////////////////////////////////////////

class TClusterConnectServerLauncher
    : public TConnectServerLauncherBase
{
public:
    TClusterConnectServerLauncher(
        TSpytConnectEngineConfigPtr config,
        TSpytConnectSettingsPtr settings,
        std::string user,
        NNative::IClientPtr clusterClient,
        TLogger logger)
        : TConnectServerLauncherBase(std::move(config), std::move(settings), std::move(user), std::move(clusterClient), std::move(logger))
        , HttpClient_(CreateClient(Config_->HttpClient, NYT::NBus::TTcpDispatcher::Get()->GetXferPoller()))
        , Headers_(New<NHttp::THeaders>())
    {
        auto discoveryValues = GetDiscoveryValues({"rest", "operation"});
        SparkMasterConnectEndpoint_ = std::format("http://{}/v1/submissions/spytConnectServer", discoveryValues[0]);
        SparkMasterKillDriverEndpoint_ = std::format("http://{}/v1/submissions/kill/", discoveryValues[0]);
        SessionId_ = FormatAsUUID(TGuid::FromString(discoveryValues[1]));

        Headers_->Add("Content-Type", "application/json");
    }

    std::string GetSessionId() override
    {
        return SessionId_;
    }

    bool TryConnectToExistingSession() override
    {
        auto response = WaitFor(HttpClient_->Get(std::format("{}?user={}", SparkMasterConnectEndpoint_, User_)))
            .ValueOrThrow();
        ValidateStatusCode(response, NHttp::EStatusCode::OK);
        auto responseNode = ParseJson(response->ReadAll());
        auto apps = responseNode->AsMap()->GetChildValueOrThrow<IListNodePtr>("apps")->GetChildren();
        if (apps.size() == 0) {
            return false;
        }
        for (const auto& runningApp : apps) {
            auto settingsHash = runningApp->AsMap()->GetChildValueOrDefault<std::string>("settingsHash", "");
            if (settingsHash == SettingsHash_) {
                UpdateFieldsFromResponse(runningApp);
                YT_LOG_DEBUG("Reusing existing Spark connect driver (User: %v, DriverId: %v)",
                    User_,
                    *DriverId_);
            } else {
                auto runningDriverId = runningApp->AsMap()->GetChildValueOrThrow<std::string>("driverId");
                YT_LOG_DEBUG("Settings hashes are different, will stop an existing driver and launch a new one (OldSettingsHash: %v, NewSettingsHash: %v, OldDriverId: %v)",
                    settingsHash,
                    SettingsHash_,
                    runningDriverId);
                StopDriver(runningDriverId);
            }
        }

        return GrpcEndpoint_ ? true : false;
    }

    void StartDriver(const std::string& token) override
    {
        auto requestBodyString = CreateStartConnectServerRequest(token);
        auto requestBody = TSharedRef::FromString(requestBodyString);
        auto response = WaitFor(HttpClient_->Post(SparkMasterConnectEndpoint_, requestBody, Headers_))
            .ValueOrThrow();
        ValidateStatusCode(response, NHttp::EStatusCode::OK);
        auto responseNode = ParseJson(response->ReadAll());
        UpdateFieldsFromResponse(responseNode);
        YT_LOG_DEBUG("Started new Spark connect driver in inner cluster (User: %v, DiscoveryPath: %v, Endpoint: %v, DriverId: %v)",
            User_,
            Settings_->DiscoveryPath,
            *GrpcEndpoint_,
            *DriverId_);
    }

    std::string WaitForSparkConnectEndpoint() override
    {
        return *GrpcEndpoint_;
    }

    void Abort() override
    {
        YT_LOG_DEBUG("Aborting Spark connect query (User: %v, DiscoveryPath: %v, DriverId: %v)",
            User_,
            Settings_->DiscoveryPath,
            DriverId_);
    }

    void Detach() override
    {
        if (!Settings_->DriverReuse && DriverId_) {
            StopDriver(*DriverId_);
        }
    }

private:
    const NHttp::IClientPtr HttpClient_;
    const NHttp::THeadersPtr Headers_;

    std::string SparkMasterConnectEndpoint_;
    std::string SparkMasterKillDriverEndpoint_;
    std::string SessionId_;
    std::optional<std::string> GrpcEndpoint_;
    std::optional<std::string> DriverId_;

    std::vector<std::string> GetDiscoveryValues(const std::vector<std::string>& keys)
    {
        auto path = Format("%v/discovery", Settings_->DiscoveryPath);
        auto discovery = WaitFor(TargetClusterClient_->GetNode(path))
            .ValueOrThrow();

        auto discoveryNode = ConvertTo<IMapNodePtr>(discovery);

        std::vector<std::string> values;
        values.reserve(keys.size());

        for (const auto& key: keys) {
            auto childNode = discoveryNode->GetChildValueOrThrow<IMapNodePtr>(key);
            if (childNode->GetKeys().size() != 1) {
                THROW_ERROR_EXCEPTION("SPYT discovery path should have exacly one %Qv element", key);
            }
            values.push_back(childNode->GetKeys()[0]);
        }
        return values;
    }

    std::string CreateStartConnectServerRequest(const std::string& token)
    {
        auto requestBody = BuildYsonNodeFluently()
            .BeginMap()
                .Item("action").Value("StartConnectServerRequest")
                .Item("driverMemory").Value(Settings_->DriverMemory)
                .Item("numExecutors").Value(Settings_->NumExecutors)
                .Item("executorCores").Value(Settings_->ExecutorCores)
                .Item("executorMemory").Value(Settings_->ExecutorMemory)
                .Item("grpcPortStart").Value(Config_->GrpcPort)
                .Item("sparkConf").DoMap([&] (auto fluent) {
                    for (const auto& kv : Settings_->SparkConf) {
                        fluent.Item(kv.first).Value(kv.second);
                    }
                    fluent
                        .Item("spark.hadoop.yt.user").Value(User_)
                        .Item("spark.hadoop.yt.token").Value(token)
                        .Item("spark.app.name").Value(std::format("Spark connect server for {}", User_))
                        .Item("spark.ytsaurus.arrow.stringToBinary").Value("true")
                        .Item("spark.ytsaurus.connect.settings.hash").Value(SettingsHash_);
                })
            .EndMap();
        return ConvertToJson(requestBody);
    }

    static INodePtr ParseJson(const TSharedRef& data)
    {
        TMemoryInput stream(data.Begin(), data.Size());
        auto factory = NYTree::CreateEphemeralNodeFactory();
        auto builder = NYTree::CreateBuilderFromFactory(factory.get());
        auto config = New<NJson::TJsonFormatConfig>();
        NJson::ParseJson(&stream, builder.get(), config);
        return builder->EndTree();
    }

    static std::string ConvertToJson(const INodePtr& ysonNode)
    {
        TString result;
        TStringOutput resultOutput(result);
        auto jsonWriter = NJson::CreateJsonConsumer(&resultOutput);
        Serialize(ysonNode, jsonWriter.get());
        jsonWriter->Flush();
        return result;
    }

    static void ValidateStatusCode(const NHttp::IResponsePtr& response, NHttp::EStatusCode expected)
    {
        if (response->GetStatusCode() != expected) {
            THROW_ERROR_EXCEPTION(
                "Unexpected HTTP status code from Spark Master: expected %Qv, actual %Qv",
                expected,
                response->GetStatusCode())
                    << TErrorAttribute("response_body", response->ReadAll().ToStringBuf());
        }
    }

    void UpdateFieldsFromResponse(const INodePtr& responseNode)
    {
        auto responseMap = responseNode->AsMap();
        GrpcEndpoint_ = responseMap->GetChildValueOrThrow<std::string>("endpoint");
        DriverId_ = responseMap->GetChildValueOrThrow<std::string>("driverId");
    }

    void StopDriver(const std::string& driverId)
    {
        YT_LOG_DEBUG("Stopping Spark connect driver operation (User: %v, DiscoveryPath: %v, DriverId: %v)",
            User_,
            Settings_->DiscoveryPath,
            driverId);
        auto response = WaitFor(HttpClient_->Post(SparkMasterKillDriverEndpoint_ + *DriverId_, TSharedRef::MakeEmpty()))
            .ValueOrThrow();
        ValidateStatusCode(response, NHttp::EStatusCode::OK);
    }
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
        , Config_(config)
        , Cluster_(Settings_->Cluster.value_or(Config_->DefaultCluster))
        , NativeConnection_(clusterDirectory->GetConnectionOrThrow(Cluster_))
        , TargetClusterClient_(NativeConnection_->CreateNativeClient(NNative::TClientOptions::FromUser(activeQuery.User)))

        , GrpcContext_(std::make_unique<grpc::ClientContext>())
    {
        if (Settings_->DiscoveryPath) {
            ServerLauncher_ = New<TClusterConnectServerLauncher>(
                config,
                Settings_,
                User_,
                TargetClusterClient_,
                Logger);
            YT_LOG_DEBUG("Discovery path is set in settings, Spark inner cluster will be used (DiscoveryPath: %v)",
                Settings_->DiscoveryPath);
        } else {
            ServerLauncher_ = New<TStandaloneConnectServerLauncher>(
                config,
                Settings_,
                User_,
                TargetClusterClient_,
                Logger);
            YT_LOG_DEBUG("Discovery path is not set, a separate operation for connect server will be launched or reused");
        }
    }

    void Start() override
    {
        YT_LOG_DEBUG("Starting Spark connect query (Query: %v)",
            Query_);
        OnQueryStarted();
        AsyncQueryResult_ = BIND(&TSpytConnectQueryHandler::Execute, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
        AsyncQueryResult_.Subscribe(BIND(&TSpytConnectQueryHandler::OnSpytConnectResponse, MakeWeak(this))
            .Via(GetCurrentInvoker()));
    }

    void Abort() override
    {
        ServerLauncher_->Abort();
        AsyncQueryResult_.Cancel(TError("Query aborted"));
        // After Abort() call there is Detach() call always.
    }

    void Detach() override
    {
        YT_LOG_DEBUG("Detaching Spark connect query (User: %v)", User_);
        AsyncQueryResult_.Cancel(TError("Query detached"));
        CancelQuery();
        ServerLauncher_->Detach();
    }

private:
    const TSpytConnectSettingsPtr Settings_;
    const TSpytConnectEngineConfigPtr Config_;
    const std::string Cluster_;
    const NNative::IConnectionPtr NativeConnection_;
    const NNative::IClientPtr TargetClusterClient_;
    const std::unique_ptr<grpc::ClientContext> GrpcContext_;

    IConnectServerLauncherPtr ServerLauncher_;
    TFuture<TSpytConnectQueryResult> AsyncQueryResult_;

    std::string IssueToken() const
    {
        auto options = TIssueTemporaryTokenOptions{ .ExpirationTimeout = Config_->TokenExpirationTimeout };
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("query_id", QueryId_);
        attributes->Set("responsible", "query_tracker");
        YT_LOG_DEBUG("Requesting token (User: %v)",
            User_);
        auto result = WaitFor(TargetClusterClient_->IssueTemporaryToken(User_, attributes, options))
            .ValueOrThrow();
        auto token = result.Token;
        YT_LOG_DEBUG("Token received (User: %v)",
            User_);
        return token;
    }

    std::string PrepareSession()
    {
        if (!ServerLauncher_->TryConnectToExistingSession()) {
            try {
                YT_LOG_DEBUG("Starting session");
                auto token = IssueToken();
                ServerLauncher_->StartDriver(token);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Caught error while preparing session");
                throw;
            }
        }
        return ServerLauncher_->WaitForSparkConnectEndpoint();
    }

    TSpytConnectQueryResult SubmitQuery(const TString& endpoint)
    {
        auto sessionId = ServerLauncher_->GetSessionId();
        ExecutePlanRequest request;
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
            Expression_Literal valueLiteral;
            valueLiteral.set_string(value);
            sqlArgs->emplace(std::move(name), std::move(valueLiteral));
        }

        YT_LOG_DEBUG("Created gRPC request for executing a plan (ExecutePlanRequest: %v)",
            request.DebugString());

        auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        auto service = SparkConnectService::NewStub(channel);

        auto responseReader = service->ExecutePlan(GrpcContext_.get(), request);
        ExecutePlanResponse responsePart;
        i64 rowCount = 0;
        std::vector<TString> arrowData;
        std::optional<TArrowSchemaPtr> arrowSchema;
        while (responseReader->Read(&responsePart)) {
            if (responsePart.has_arrow_batch()) {
                auto data = responsePart.arrow_batch().Getdata();
                arrowData.push_back(data);
                rowCount += responsePart.arrow_batch().Getrow_count();
                if (!arrowSchema) {
                    std::string strData = data;
                    auto buf = std::make_shared<arrow20::Buffer>(strData);
                    arrow20::io::BufferReader arrowBufferReader{buf};
                    auto ipcReaderResult = arrow20::ipc::RecordBatchStreamReader::Open(&arrowBufferReader);
                    if (ipcReaderResult.ok()) {
                        auto ipcReader = ipcReaderResult.ValueOrDie();
                        arrowSchema = ipcReader->schema();
                    } else {
                        THROW_ERROR_EXCEPTION("An error has occured during arrow schema reading")
                            << TErrorAttribute("error", ipcReaderResult.status().ToString());
                    }
                }
            }
        }

        auto status = responseReader->Finish();

        if (!status.ok()) {
            THROW_ERROR_EXCEPTION("gRPC request failed")
                << TErrorAttribute("status", static_cast<i32>(status.error_code()))
                << TErrorAttribute("message", status.error_message());
        }

        if (!arrowSchema) {
            THROW_ERROR_EXCEPTION("Result does not contain a schema, probably there was an error");
        }

        return TSpytConnectQueryResult{
            .IsTruncated = rowCount > Config_->RowCountLimit,
            .ArrowSchema = *arrowSchema,
            .ArrowData = std::move(arrowData)
        };
    }

    void CancelQuery()
    {
        YT_LOG_DEBUG("Cancelling gRPC request for query");
        GrpcContext_->TryCancel();
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

        auto tableSchema = CreateYTTableSchemaFromArrowSchema(result.ArrowSchema);

        TBuildingValueConsumer valueConsumer(tableSchema, Logger, true);
        auto parser = CreateParserForArrow(&valueConsumer);
        YT_LOG_DEBUG("Processing response arrow batches (ArrowBatchesNum: %v)",
            result.ArrowData.size());
        for (size_t i = 0; i < result.ArrowData.size(); i++) {
            parser->Read(result.ArrowData[i]);
        }
        parser->Finish();

        std::vector<TUnversionedRow> rows = valueConsumer.GetRows();
        if (std::ssize(rows) > Config_->RowCountLimit) {
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
    TSpytConnectEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
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
            NotIndexedQueriesTTL_);
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
    TDuration NotIndexedQueriesTTL_;

    TSpytConnectEngineConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateSpytConnectEngine(IClientPtr stateClient, TYPath stateRoot)
{
    return New<TSpytConnectEngine>(
        std::move(stateClient),
        std::move(stateRoot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
