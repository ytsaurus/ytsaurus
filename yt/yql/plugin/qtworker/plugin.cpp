#include "plugin.h"

#include "helpers.h"

#include <yt/yql/plugin/native/plugin.h>

#include <yt/yql/plugin/lib/error_helpers.h>
#include <yt/yql/plugin/lib/progress_merger.h>

#include <yql/tools/yqlworker/interface/msgbus/worker_api_msgbus.h>
#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NYqlPlugin {

using namespace NConcurrency;
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

        {
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
        int executeMode) noexcept override
    {
        if (!WorkerApi_ || !WorkerApi_->IsHealthy()) {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson("No healthy workers"),
            };
        }

        try {
            auto action = ExecuteModeToProto(executeMode);
            auto callback = std::make_shared<TTaskEventCallback>();

            NYql::NProto::TTaskData data;
            data.SetId(ToString(queryId));
            data.SetSyntax(NYql::NProto::ESyntax::SQLv1);
            data.SetProgram(queryText);
            data.SetUsername(user);
            data.SetResultFormat(NYql::NProto::EDataFormat::YSON_TEXT);
            data.SetAttributes(settings.ToString());
            data.SetAuthData(SerializeCredentials(credentials));
            data.SetIsSystemRequest(false);

            auto patch = GatewaysConfigPatch_.Load();
            if (!patch.empty()) {
                data.SetGatewaysConfigPatch(patch);
            }

            for (const auto& file : files) {
                auto* protoFile = data.MutableFiles()->Add();
                protoFile->SetName(TString(file.Name));
                protoFile->SetType(FileTypeToProto(file.Type));
                protoFile->SetContent(TString(file.Content));
            }

            auto runTaskResult = WorkerApi_->RunTask(action, std::move(data), callback);
            if (!runTaskResult) {
                return TQueryResult{
                    .YsonError = std::make_optional(
                        MessageToYtErrorYson(TString{runTaskResult.error().GetMessage()})),
                };
            }
            {
                TGuard guard(ActiveQueriesLock_);
                auto& activeQuery = ActiveQueries_[queryId];
                activeQuery.Callback = callback;
                activeQuery.TaskHandle = *runTaskResult;
            }
            NYT::NConcurrency::WaitFor(callback->GetDoneFuture())
                .ThrowOnError();

            const auto snapshot = callback->GetTaskResult();
            auto progressYson = callback->GetProgress();
            {
                TGuard guard(ActiveQueriesLock_);
                ActiveQueries_.erase(queryId);
            }
            return TaskResultToYqlResult(snapshot, std::move(progressYson));
        } catch (const std::exception& ex) {
            TGuard guard(ActiveQueriesLock_);
            ActiveQueries_.erase(queryId);

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

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) noexcept override
    {
        YqlPluginForGetUsedClusters_->OnDynamicConfigChanged(config);

        NYql::TGatewaysConfig dynamicGatewaysConfig;
        if (config.GatewaysConfig) {
            TProtobufWriterOptions protobufWriterOptions;
            protobufWriterOptions.ConvertSnakeToCamelCase = true;
            dynamicGatewaysConfig.ParseFromStringOrThrow(YsonStringToProto(
                config.GatewaysConfig,
                ReflectProtobufMessageType<NYql::TGatewaysConfig>(),
                protobufWriterOptions));
        }

        TString patchTextProto;
        if (!::google::protobuf::TextFormat::PrintToString(dynamicGatewaysConfig, &patchTextProto)) {
            YQL_LOG(ERROR) << "Failed to serialize gateways patch to TextProto";
            return;
        }

        GatewaysConfigPatch_.Store(std::move(patchTextProto));
    }

    TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId /*queryId*/,
        TString /*user*/,
        TString /*queryText*/,
        TYsonString /*settings*/,
        TYsonString /*credentials*/) override
    {
        return TGetDeclaredParametersInfoResult{};
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

    NThreading::TAtomicObject<TString> GatewaysConfigPatch_;

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
