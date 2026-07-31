#include "plugin.h"

#include "helpers.h"

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

#include <library/cpp/yson/node/node_io.h>
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

struct TActiveQuery
{
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

std::optional<TString> ExtractDefaultCluster(const NYql::TGatewaysConfig& config)
{
    if (config.HasYt()) {
        for (const auto& mapping : config.GetYt().GetClusterMapping()) {
            if (mapping.GetDefault()) {
                return mapping.GetName();
            }
        }
    }
    return {};
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

    TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files) noexcept override
    {
        return YqlPluginForGetUsedClusters_->GetUsedClusters(queryId, queryText, settings, files);
    }

    TQueryResult Run(
        TQueryId queryId,
        TString user,
        TYsonString credentials,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode,
        NYqlClient::EQueryType queryType) noexcept override
    {
        if (!WorkerApi_ || !WorkerApi_->IsHealthy()) {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson("No healthy workers"),
            };
        }

        try {
            auto action = ExecuteModeToProto(executeMode);
            auto data = BuildTaskData(queryId, user, queryText, settings, credentials, files, queryType);
            auto callback = RunTaskToCompletion(queryId, action, std::move(data), /*persist*/ true);

            const auto snapshot = callback->GetTaskResult();
            auto progressYson = callback->GetProgress();
            return TaskResultToYqlResult(snapshot, std::move(progressYson));
        } catch (const std::exception& ex) {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(TString{ex.what()})
            };
        }
    }

    TQueryResult GetProgress(TQueryId queryId) noexcept override
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

    TAbortResult Abort(TQueryId queryId) noexcept override
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

    void OnDynamicConfigChanged(TYqlPluginDynamicConfigPtr config) noexcept override
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
                    // TODO: switch proto fields to optional with default value somehow
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

        YQL_LOG(INFO) << Format("Udf meta updated (packages count: %v)", functionRegistryData.packages_size());
    }

    TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId queryId,
        TString user,
        TString queryText,
        TYsonString settings,
        TYsonString credentials) override
    {
        if (!WorkerApi_ || !WorkerApi_->IsHealthy()) {
            ythrow yexception() << "No healthy workers";
        }

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

    void RegisterQuery(TQueryId /*queryId*/) override
    { }
    void UnregisterQuery(TQueryId /*queryId*/) override
    { }

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

    NYql::NProto::TTaskData BuildTaskData(
        TQueryId queryId,
        const TString& user,
        const TString& queryText,
        const TYsonString& settings,
        const TYsonString& credentials,
        const std::vector<TQueryFile>& files,
        NYqlClient::EQueryType queryType = NYqlClient::EQueryType::Regular)
    {
        NYql::NProto::TTaskData data;
        data.SetId(ToString(queryId));
        data.SetSyntax((queryType == NYqlClient::EQueryType::UdfMeta) ? NYql::NProto::ESyntax::UDF_META : NYql::NProto::ESyntax::SQLv1);
        data.SetProgram(queryText);
        data.SetUsername(user);
        data.SetResultFormat(NYql::NProto::EDataFormat::YSON_TEXT);
        data.SetAuthData(SerializeCredentials(credentials));
        data.SetIsSystemRequest(false);
        data.SetFunctionRegistryData(FunctionRegistryData_.Load());
        data.SetPersistedId(true);

        std::optional<NYql::TGatewaysConfig> gatewaysConfig;
        {
            TGuard guard(FlavorConfigsLock_);
            // TODO(mpereskokova): Change to custom query flavor
            if (auto snapshot = GatewaysConfigSnapshotByFlavor_.find("default"); snapshot != GatewaysConfigSnapshotByFlavor_.end()) {
                gatewaysConfig = snapshot->second;
            }
        }
        if (!gatewaysConfig && StaticGatewaysSnapshot_) {
            gatewaysConfig = *StaticGatewaysSnapshot_;
        }

        std::optional<TString> defaultTranslationCluster;
        if (gatewaysConfig) {
            TString fullTextProto;
            if (!::google::protobuf::TextFormat::PrintToString(*gatewaysConfig, &fullTextProto)) {
                ythrow yexception() << "Failed to serialize gateways config to TextProto";
            }

            data.SetGatewaysConfig(fullTextProto);
            defaultTranslationCluster = ExtractDefaultCluster(*gatewaysConfig);
        }

        auto settingsMap = NodeFromYsonString(settings.ToString()).AsMap();
        if (auto cluster = settingsMap.FindPtr("cluster")) {
            defaultTranslationCluster = cluster->AsString();
        }
        if (auto maxLangVer = NYql::FormatLangVersion(MaxYqlLangVersion_.load())) {
            data.SetMaxLangVer(*maxLangVer);
        }
        if (auto version = settingsMap.FindPtr("yql_version")) {
            data.SetLangVer(version->AsString());
        } else if (auto defaultLangVer = NYql::FormatLangVersion(DefaultYqlApiLangVersion_)) {
            data.SetLangVer(*defaultLangVer);
        }
        if (auto parameters = settingsMap.FindPtr("declared_parameters")) {
            data.SetParameters(parameters->AsString());
        }

        if (defaultTranslationCluster) {
            data.SetDefaultTranslationCluster(*defaultTranslationCluster);
            data.SetUrl(*defaultTranslationCluster);
        }
        data.SetRunner("yql-agent");

        for (const auto& file : files) {
            auto* protoFile = data.MutableFiles()->Add();
            protoFile->SetName(TString(file.Name));
            protoFile->SetType(FileTypeToProto(file.Type));
            protoFile->SetContent(TString(file.Content));
        }

        return data;
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
            ythrow yexception() << runTaskResult.error().GetMessage();
        }

        if (!persist) {
            NYT::NConcurrency::WaitFor(callback->GetDoneFuture())
                .ThrowOnError();
            return callback;
        }

        {
            TGuard guard(ActiveQueriesLock_);
            auto& activeQuery = ActiveQueries_[queryId];
            activeQuery.Callback = callback;
            activeQuery.TaskHandle = *runTaskResult;
        }

        try {
            NYT::NConcurrency::WaitFor(callback->GetDoneFuture())
                .ThrowOnError();
        } catch (...) {
            TGuard guard(ActiveQueriesLock_);
            ActiveQueries_.erase(queryId);
            throw;
        }

        TGuard guard(ActiveQueriesLock_);
        ActiveQueries_.erase(queryId);
        return callback;
    }

    TString SerializeCredentials(const TYsonString& credentials)
    {
        NYql::NProto::TTaskAuthTokens authTokens;

        auto credentialsNode = NodeFromYsonString(credentials.ToString());
        if (!credentialsNode.IsMap()) {
            return authTokens.SerializeAsString();
        }

        for (const auto& [alias, value] : credentialsNode.AsMap()) {
            auto* token = authTokens.AddTokens();
            token->SetAlias(alias);
            token->SetCategory(value.HasKey("category") ? value.ChildAsString("category") : "");
            token->SetSubcategory(value.HasKey("subcategory") ? value.ChildAsString("subcategory") : "");
            token->SetContent(value.HasKey("content") ? value.ChildAsString("content") : "");
        }

        return authTokens.SerializeAsString();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateQtWorkerYqlPlugin(TYqlQTWorkerPluginOptions options) noexcept
{
    return std::make_unique<TQtWorkerYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
