#include "plugin.h"

#include "helpers.h"
#include "task_data_builder.h"

#include <yt/yql/plugin/config.h>
#include <yt/yql/plugin/native/plugin.h>

#include <yt/yql/plugin/lib/error_helpers.h>
#include <yt/yql/plugin/lib/progress_merger.h>

#include <yt/yql/plugin/udf_meta.h>

#include <yql/tools/yqlworker/interface/msgbus/worker_api_msgbus.h>
#include <yql/tools/yqlworker/interface/proto/task.pb.h>
#include <yql/tools/yqlworker/proto/function_registry.pb.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/stream/file.h>
#include <util/stream/str.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NYqlPlugin {

using namespace NConcurrency;
using namespace NYqlClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TTaskEventCallback
    : public NYql::NWorkerApi::ITaskResultCallback
{
public:
    ::NThreading::TFuture<void> Notify(const NYql::NProto::TTaskResult& result, ui64 /*sentTime*/) override
    {
        TGuard guard(Lock_);
        UpdateTaskResultData(TaskResult_, result);
        if (result.HasProgress()) {
            ProgressMerger_.MergeWith(result.GetProgress());
        }

        if (IsTaskTerminal(result.GetStatus()) && !IsFinished_) {
            DonePromise_.Set();
            IsFinished_ = true;
        }

        return ::NThreading::MakeFuture<void>();
    }

    TFuture<void> GetDoneFuture()
    {
        return DonePromise_.ToFuture();
    }

    NYql::NProto::TTaskResult GetTaskResult() const
    {
        TGuard guard(Lock_);
        return TaskResult_;
    }

    TString GetProgress()
    {
        TGuard guard(Lock_);
        return ProgressMerger_.ToYsonString();
    }

private:
    mutable TMutex Lock_;
    NYql::NProto::TTaskResult TaskResult_;
    NYT::NYqlPlugin::TProgressMerger ProgressMerger_;
    NYT::TPromise<void> DonePromise_ = NewPromise<void>();
    bool IsFinished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TActiveQueryConfig
    : public TRefCounted
{
    TActiveQueryConfig(
        TString flavor,
        std::optional<NYql::TGatewaysConfig> gatewaysConfig,
        std::optional<TString> defaultCluster)
        : Flavor(std::move(flavor))
        , GatewaysConfig(std::move(gatewaysConfig))
        , DefaultCluster(std::move(defaultCluster))
    { }

    TString Flavor;
    std::optional<NYql::TGatewaysConfig> GatewaysConfig;
    std::optional<TString> DefaultCluster;
};
DECLARE_REFCOUNTED_TYPE(TActiveQueryConfig)
using TConstActiveQueryConfigPtr = TIntrusivePtr<const TActiveQueryConfig>;
DEFINE_REFCOUNTED_TYPE(TActiveQueryConfig)

struct TActiveQuery
{
    using TConfig = TActiveQueryConfig;

    TConstActiveQueryConfigPtr Config;

    std::shared_ptr<NYql::NWorkerApi::ITaskHandle> TaskHandle;
    std::shared_ptr<TTaskEventCallback> Callback;
};

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString SerializeProtoToYson(const google::protobuf::Message& message)
{
    TStringStream ysonStream;
    NYson::TYsonWriter ysonWriter(&ysonStream, NYson::EYsonFormat::Binary);
    NYson::WriteProtobufMessage(&ysonWriter, message);
    ysonStream.Finish();
    return NYson::TYsonString(ysonStream.Str());
}

class TQtWorkerYqlPlugin
    : public IYqlPlugin
{
public:
    explicit TQtWorkerYqlPlugin(TYqlQTWorkerPluginOptions options)
        : QtWorkerInspectorPort_(options.QtWorkerInspectorPort)
    {
        NYql::NLog::InitLogger(std::move(options.QtWorkerLogBackend));

        auto& logger = NYql::NLog::YqlLogger();

        logger.SetDefaultPriority(ELogPriority::TLOG_DEBUG);
        for (int i = 0; i < NYql::NLog::TComponentHelpers::ToInt(NYql::NLog::EComponent::MaxValue); ++i) {
            logger.SetComponentLevel(NYql::NLog::EComponent(i), NYql::NLog::ELevel::DEBUG);
        }

        if (!options.GatewaysConfigPath.empty()) {
            TFileInput input(options.GatewaysConfigPath);
            StaticGatewaysSnapshot_ = std::make_optional<NYql::TGatewaysConfig>();
            ParseFromTextFormat(input, *StaticGatewaysSnapshot_, EParseFromTextFormatOption::AllowUnknownField);
        }

        NYql::TLangVersionBuffer buf;
        TStringBuf versionStringBuf;

        DefaultYqlApiLangVersion_ = NYql::MinLangVersion;
        NYql::FormatLangVersion(DefaultYqlApiLangVersion_, buf, versionStringBuf);
        YQL_LOG(INFO) << Format("Default YQL version for API and CLI is set (Version: %v)", versionStringBuf);

        auto initialDynamicConfig = New<TYqlPluginDynamicConfig>();
        initialDynamicConfig->Load(NYTree::ConvertToNode(options.InitialDynamicConfig));
        YT_VERIFY(initialDynamicConfig->MaxSupportedYqlVersion);

        {
            if (StaticGatewaysSnapshot_) {
                options.GatewayConfig = SerializeProtoToYson(StaticGatewaysSnapshot_->GetYt());
                options.DqGatewayConfig = SerializeProtoToYson(StaticGatewaysSnapshot_->GetDq());
                options.YtflowGatewayConfig = SerializeProtoToYson(StaticGatewaysSnapshot_->GetYtflow());
                options.PqGatewayConfig = SerializeProtoToYson(StaticGatewaysSnapshot_->GetPq());
                options.SolomonGatewayConfig = SerializeProtoToYson(StaticGatewaysSnapshot_->GetSolomon());
            }

            // NB: under debug build this method does not fit in regular fiber stack
            // due to python udf loading
            using TSignature = void(TYqlNativePluginOptions);
            auto coroutine = TCoroutine<TSignature>(
                BIND([this](TCoroutine<TSignature>& /*self*/, TYqlNativePluginOptions options) {
                    YqlPluginForGetUsedClusters_ = CreateYqlPlugin(std::move(options));
                }), EExecutionStackKind::Large);

            coroutine.Run(std::move(options));
            YT_VERIFY(coroutine.IsCompleted());
        }

        OnDynamicConfigChanged(std::move(initialDynamicConfig));
        MaxYqlLangVersionInitial_ = MaxYqlLangVersion_;
    }

    void Start() override
    {
        NYql::NWorkerApi::TMsgBusWorkerApiConfig busConfig;
        busConfig.Port = QtWorkerInspectorPort_;

        WorkerApi_ = NYql::NWorkerApi::MakeMsgBusWorkerApi(std::move(busConfig));

        YqlPluginForGetUsedClusters_->Start();
    }

    bool IsReady() const override
    {
        return WorkerApi_ && WorkerApi_->IsHealthy();
    }

    TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files) override
    {
        return YqlPluginForGetUsedClusters_->GetUsedClusters(queryId, queryText, settings, files);
    }

    TClustersResult GetClustersInfo(TQueryId queryId) override
    {
        const auto queryConfig = GetQueryConfig(queryId);

        TClustersResult result{
            .DefaultCluster = queryConfig->DefaultCluster,
        };

        YT_VERIFY(queryConfig->GatewaysConfig);
        const auto& gatewaysConfig = *queryConfig->GatewaysConfig;

        const auto& ytConfig = gatewaysConfig.GetYt();
        for (const auto& mapping : ytConfig.GetClusterMapping()) {
            result.Clusters.emplace_back(mapping.name(), mapping.cluster());
        }

        const auto& pqConfig = gatewaysConfig.GetPq();
        for (const auto& mapping : pqConfig.GetClusterMapping()) {
            result.Clusters.emplace_back(mapping.name(), mapping.endpoint());
        }

        const auto& solomonConfig = gatewaysConfig.GetSolomon();
        for (const auto& mapping : solomonConfig.GetClusterMapping()) {
            result.Clusters.emplace_back(mapping.name(), mapping.cluster());
        }

        return result;
    }

    TQueryResult Run(
        TQueryId queryId,
        TString user,
        TYsonString credentials,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode,
        NYqlClient::EQueryType queryType) override
    {
        try {
            auto action = ExecuteModeToProto(executeMode);
            auto data = BuildTaskData(queryId, user, queryText, settings, credentials, files, queryType);
            auto callback = RunTaskToCompletion(queryId, action, std::move(data), /*persist*/ true);

            const auto snapshot = callback->GetTaskResult();
            auto progressYson = callback->GetProgress();
            return TaskResultToYqlResult(snapshot, std::move(progressYson));
        } catch (const NYql::NWorkerApi::TRunTaskError& ex) {
            if (ex.GetReason() == NYql::NWorkerApi::TRunTaskError::EReason::REJECTED) {
                THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::YqlAgentNotReady, "%v", ex.GetMessage());
            }
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(ex.GetMessage()),
            };
        } catch (const std::exception& ex) {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(TString{ex.what()})
            };
        }
    }

    TQueryResult GetProgress(TQueryId queryId) override
    {
        NYql::NProto::TTaskResult taskResult;
        TString progress;

        {
            TGuard guard(ActiveQueriesLock_);
            auto it = ActiveQueries_.find(queryId);
            if (it == ActiveQueries_.end() || !it->second.Callback) {
                return TQueryResult{
                    .YsonError = MessageToYtErrorYson(Format("No progress for query: %v", queryId)),
                };
            }
            taskResult = it->second.Callback->GetTaskResult();
            progress = it->second.Callback->GetProgress();
        }

        return TaskResultToYqlResult(taskResult, progress);
    }

    TAbortResult Abort(TQueryId queryId) override
    {
        std::shared_ptr<NYql::NWorkerApi::ITaskHandle> taskHandle;
        {
            TGuard guard(ActiveQueriesLock_);
            auto it = ActiveQueries_.find(queryId);
            if (it == ActiveQueries_.end() || !it->second.TaskHandle) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(Format("Query %v is not found", queryId)),
                };
            }

            taskHandle = it->second.TaskHandle;
        }

        if (auto cancel = taskHandle->Cancel(); !cancel) {
            return TAbortResult{
                .YsonError = MessageToYtErrorYson(cancel.error()),
            };
        }

        return {};
    }

    void OnDynamicConfigChanged(TYqlPluginDynamicConfigPtr config) override
    {
        if (!config->MaxSupportedYqlVersion) {
            MaxYqlLangVersion_ = MaxYqlLangVersionInitial_;
        } else {
            NYql::TLangVersion maxVersion;
            if (NYql::ParseLangVersion(config->MaxSupportedYqlVersion, maxVersion)) {
                MaxYqlLangVersion_ = maxVersion;
            } else {
                YQL_LOG(ERROR) << "Cannot parse config.MaxSupportedYqlVersion: " << config->MaxSupportedYqlVersion;
                MaxYqlLangVersion_ = MaxYqlLangVersionInitial_;
            }
        }

        for (const auto& [flavor, protoConfig] : config->ProtoGatewaysConfigs) {
            NYql::TGatewaysConfig protoGatewaysConfig;
            TStringInput input(protoConfig);
            ParseFromTextFormat(input, protoGatewaysConfig, EParseFromTextFormatOption::AllowUnknownField);

            if (flavor == "default") {
                auto defaultConfig = CloneYsonStruct(config);
                defaultConfig->GatewaysConfig = SerializeProtoToYson(protoGatewaysConfig);
                YqlPluginForGetUsedClusters_->OnDynamicConfigChanged(std::move(defaultConfig));
            }

            {
                TGuard guard(FlavorConfigsLock_);
                GatewaysConfigSnapshotByFlavor_[flavor] = protoGatewaysConfig;
            }
        }
    }

    void OnUdfMetaChanged(TUdfMetaPtr udfMeta) override
    {
        TProtobufWriterOptions protobufWriterOptions;
        protobufWriterOptions.UnknownYsonFieldModeResolver =
            TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Skip);

        NYql::NProto::TFunctionRegistryData functionRegistryData;
        for (const auto& [packageName, packageMeta] : udfMeta->Udfs) {
            auto* package = functionRegistryData.add_packages();
            package->set_name(packageName);
            package->set_defaultversion(0);

            auto* resource = package->add_resources();
            resource->set_url(packageMeta->Alias);
            resource->set_version(0);
            resource->set_istrusted(false);

            for (const auto& [moduleName, moduleMeta] : packageMeta->Modules) {
                auto* module = resource->add_modules();
                module->set_name(moduleName);

                for (const auto& functionNode : moduleMeta->Functions->GetChildren()) {
                    // TODO(ziganshinmr): switch proto fields to optional with default value somehow
                    auto functionMap = CloneNode(functionNode)->AsMap();
                    if (!functionMap->FindChild("ArgCount")) {
                        functionMap->AddChild("ArgCount", NYTree::ConvertToNode(0u));
                    }
                    if (!functionMap->FindChild("OptionalArgCount")) {
                        functionMap->AddChild("OptionalArgCount", NYTree::ConvertToNode(0u));
                    }

                    module->add_functions()->ParseFromStringOrThrow(YsonStringToProto(
                        ConvertToYsonString(functionMap),
                        ReflectProtobufMessageType<NYql::NProto::TFunction>(),
                        protobufWriterOptions));
                }
            }
        }

        TString textProto;
        YT_VERIFY(::google::protobuf::TextFormat::PrintToString(functionRegistryData, &textProto));
        FunctionRegistryData_.Store(std::move(textProto));

        YQL_LOG(INFO) << Format("UDF meta updated (PackageCount: %v)", functionRegistryData.packages_size());
    }

    TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId queryId,
        TString user,
        TString queryText,
        TYsonString settings,
        TYsonString credentials) override
    {
        auto data = BuildTaskData(queryId, user, queryText, settings, credentials, /*files*/ {});
        auto callback = RunTaskToCompletion(
            queryId,
            NYql::NProto::ETaskAction::EXTRACT_PARAMS_META,
            std::move(data),
            /*persist*/ false);

        const auto snapshot = callback->GetTaskResult();
        if (snapshot.GetStatus() == NYql::NProto::ETaskStatus::ERROR) {
            if (snapshot.IssuesSize() > 0) {
                NYql::TIssues issues;
                IssuesFromMessage(snapshot.GetIssues(), issues);
                ythrow yexception() << IssuesToYtErrorYson(issues);
            }
            ythrow yexception() << "Failed to extract query parameters metadata on worker";
        }

        return TGetDeclaredParametersInfoResult{
            .YsonParameters = snapshot.ResultsSize() > 0
                ? std::make_optional(snapshot.GetResults(0))
                : std::nullopt,
        };
    }

    void RegisterQuery(TQueryId queryId, TYsonString settings) override
    {
        auto flavor = DetectFlavorFromSettings(settings);
        auto gatewaysConfig = GetCurrentGatewaysConfig(flavor);
        auto defaultCluster = gatewaysConfig
            ? ExtractDefaultCluster(*gatewaysConfig)
            : std::nullopt;
        auto settingsMap = NYTree::ConvertTo<NYTree::IMapNodePtr>(settings);
        if (auto cluster = settingsMap->FindChildValue<TString>("cluster")) {
            defaultCluster = *cluster;
        }

        {
            TGuard guard(ActiveQueriesLock_);
            auto [_, inserted] = ActiveQueries_.emplace(queryId, TActiveQuery{
                .Config = New<TActiveQuery::TConfig>(
                    std::move(flavor),
                    std::move(gatewaysConfig),
                    std::move(defaultCluster)),
            });
            YT_VERIFY(inserted);
        }

        try {
            YqlPluginForGetUsedClusters_->RegisterQuery(queryId, settings);
        } catch (...) {
            TGuard guard(ActiveQueriesLock_);
            ActiveQueries_.erase(queryId);
            throw;
        }
    }

    void UnregisterQuery(TQueryId queryId) override
    {
        {
            TGuard guard(ActiveQueriesLock_);
            auto erased = ActiveQueries_.erase(queryId);
            YT_VERIFY(erased == 1);
        }

        YqlPluginForGetUsedClusters_->UnregisterQuery(queryId);
    }

private:
    const int QtWorkerInspectorPort_;

    std::unique_ptr<IYqlPlugin> YqlPluginForGetUsedClusters_;

    std::shared_ptr<NYql::NWorkerApi::IWorkerApi> WorkerApi_;

    TMutex ActiveQueriesLock_;
    THashMap<TQueryId, TActiveQuery> ActiveQueries_;

    std::optional<NYql::TGatewaysConfig> StaticGatewaysSnapshot_;

    std::atomic<NYql::TLangVersion> MaxYqlLangVersion_;
    NYql::TLangVersion MaxYqlLangVersionInitial_;
    NYql::TLangVersion DefaultYqlApiLangVersion_;

    TMutex FlavorConfigsLock_;
    THashMap<TString, NYql::TGatewaysConfig> GatewaysConfigSnapshotByFlavor_;

    NThreading::TAtomicObject<TString> FunctionRegistryData_;

    std::optional<NYql::TGatewaysConfig> GetCurrentGatewaysConfig(const TString& flavor)
    {
        TGuard guard(FlavorConfigsLock_);
        if (auto snapshot = GatewaysConfigSnapshotByFlavor_.find(flavor);
            snapshot != GatewaysConfigSnapshotByFlavor_.end())
        {
            return snapshot->second;
        }
        return StaticGatewaysSnapshot_;
    }

    TConstActiveQueryConfigPtr GetQueryConfig(TQueryId queryId)
    {
        TGuard guard(ActiveQueriesLock_);
        auto it = ActiveQueries_.find(queryId);
        YT_VERIFY(it != ActiveQueries_.end());
        return it->second.Config;
    }

    NYql::NProto::TTaskData BuildTaskData(
        TQueryId queryId,
        const TString& user,
        const TString& queryText,
        const TYsonString& settings,
        const TYsonString& credentials,
        const std::vector<TQueryFile>& files,
        NYqlClient::EQueryType queryType = NYqlClient::EQueryType::Regular)
    {
        const auto queryConfig = GetQueryConfig(queryId);
        auto builder = CreateTaskDataBuilder(queryConfig->Flavor);

        auto functionRegistryData = FunctionRegistryData_.Load();

        return builder->Build(TTaskDataBuildContext{
            .QueryId = queryId,
            .User = user,
            .QueryText = queryText,
            .Settings = settings,
            .Credentials = credentials,
            .Files = files,
            .FunctionRegistryData = functionRegistryData,
            .GatewaysConfig = queryConfig->GatewaysConfig,
            .DefaultCluster = queryConfig->DefaultCluster,
            .MaxYqlLangVersion = NYql::FormatLangVersion(MaxYqlLangVersion_.load()),
            .DefaultYqlLangVersion = NYql::FormatLangVersion(DefaultYqlApiLangVersion_),
            .QueryType = queryType,
        });
    }

    std::shared_ptr<TTaskEventCallback> RunTaskToCompletion(
        TQueryId queryId,
        NYql::NProto::ETaskAction action,
        NYql::NProto::TTaskData data,
        bool persist)
    {
        auto callback = std::make_shared<TTaskEventCallback>();

        auto runTaskResult = WorkerApi_->RunTask(action, std::move(data), callback);
        if (!runTaskResult) {
            throw runTaskResult.error();
        }

        if (!persist) {
            NYT::NConcurrency::WaitFor(callback->GetDoneFuture())
                .ThrowOnError();
            return callback;
        }

        {
            TGuard guard(ActiveQueriesLock_);
            auto it = ActiveQueries_.find(queryId);
            YT_VERIFY(it != ActiveQueries_.end());
            auto& activeQuery = it->second;
            activeQuery.Callback = callback;
            activeQuery.TaskHandle = *runTaskResult;
        }

        NYT::NConcurrency::WaitFor(callback->GetDoneFuture())
            .ThrowOnError();
        return callback;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateQtWorkerYqlPlugin(TYqlQTWorkerPluginOptions options)
{
    return std::make_unique<TQtWorkerYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
