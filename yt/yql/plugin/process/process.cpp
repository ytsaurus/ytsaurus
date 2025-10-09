#include "process.h"
#include "private.h"

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/library/process/process.h>

namespace NYT::NYqlPlugin::NProcess {

static constexpr auto& Logger = YqlExecutorProcessLogger;

using NYqlClient::NProto::TYqlQueryFile_EContentType;
using NYqlClient::NProto::TYqlResponse;

using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

void SetQueryResultField(
    std::optional<TString>& queryResultField,
    const TYqlResponse& response,
    std::function<bool(const TYqlResponse*)> isFieldPresent,
    std::function<TString(const TYqlResponse*)> fieldValue)
{
    if (isFieldPresent(&response)) {
        queryResultField = fieldValue(&response);
    }
}

TQueryResult ToQueryResult(const TYqlResponse& yqlResponse)
{
    TQueryResult result;

    SetQueryResultField(result.YsonResult, yqlResponse, &TYqlResponse::has_result, &TYqlResponse::result);
    SetQueryResultField(result.Plan, yqlResponse, &TYqlResponse::has_plan, &TYqlResponse::plan);
    SetQueryResultField(result.Progress, yqlResponse, &TYqlResponse::has_progress, &TYqlResponse::progress);
    SetQueryResultField(result.Statistics, yqlResponse, &TYqlResponse::has_statistics, &TYqlResponse::statistics);
    SetQueryResultField(result.YsonError, yqlResponse, &TYqlResponse::has_error, &TYqlResponse::error);
    SetQueryResultField(result.TaskInfo, yqlResponse, &TYqlResponse::has_task_info, &TYqlResponse::task_info);
    SetQueryResultField(result.Ast, yqlResponse, &TYqlResponse::has_ast, &TYqlResponse::ast);

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYqlExecutorProcess::TYqlExecutorProcess(
    int slotIndex,
    int dynamicConfigVersion,
    TYqlPluginProxy pluginProxy,
    TString unixSocketPath,
    TProcessBasePtr yqlPluginProcess,
    TFuture<void> processFinishFuture,
    TDuration runRequestTimeout)
    : SlotIndex_(slotIndex)
    , DynamicConfigVersion_(dynamicConfigVersion)
    , PluginProxy_(std::move(pluginProxy))
    , UnixSocketPath_(unixSocketPath)
    , YqlPluginProcess_(yqlPluginProcess)
    , ProcessFinishFuture_(std::move(processFinishFuture))
    , RunRequestTimeout_(runRequestTimeout)
    { }

////////////////////////////////////////////////////////////////////////////////

TClustersResult TYqlExecutorProcess::GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files)
{
    auto getUsedClustersReq = PluginProxy_.GetUsedClusters();

    ToProto(getUsedClustersReq->mutable_query_id(), queryId);
    getUsedClustersReq->set_query_text(queryText);
    getUsedClustersReq->set_settings(settings.ToString());

    for (const auto& file : files) {
        auto queryFile = getUsedClustersReq->add_files();
        queryFile->set_name(file.Name);
        queryFile->set_content(file.Content);
        queryFile->set_type(static_cast<TYqlQueryFile_EContentType>(file.Type));
    }

    auto response = WaitFor(getUsedClustersReq->Invoke());
    if (!response.IsOK()) {
        YT_LOG_ERROR(response, "Failed to get cluster result from subprocess (QueryId: %v, SlotIndex: %v)", queryId, SlotIndex_);
        return ToErrorResponse<TClustersResult>("Failed to get used clusters result from subprocess", response);
    }

    auto responseValue = response.Value();
    TClustersResult result;
    for (const auto& cluster: responseValue->clusters()) {
        result.Clusters.emplace_back(cluster.cluster_name(), cluster.cluster_address());
    }

    if (responseValue->has_error()) {
      result.YsonError = responseValue->error();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TQueryResult TYqlExecutorProcess::Run(
    TQueryId queryId,
    TString user,
    NYson::TYsonString credentials,
    TString queryText,
    NYson::TYsonString settings,
    std::vector<TQueryFile> files,
    int executeMode)
{
    {
        auto guard = Guard(ActiveQueryIdLock_);
        ActiveQueryId_ = queryId;
    }
    auto runQueryReq = PluginProxy_.RunQuery();
    runQueryReq->SetTimeout(RunRequestTimeout_);

    ToProto(runQueryReq->mutable_query_id(), queryId);
    runQueryReq->set_user(user);
    runQueryReq->set_credentials(credentials.ToString());
    runQueryReq->set_query_text(queryText);
    runQueryReq->set_settings(settings.ToString());

    for (const auto& file : files) {
      auto queryFile = runQueryReq->add_files();
      queryFile->set_name(file.Name);
      queryFile->set_content(file.Content);
      queryFile->set_type(static_cast<TYqlQueryFile_EContentType>(file.Type));
    }

    runQueryReq->set_mode(executeMode);

    auto response = WaitFor(runQueryReq->Invoke());
    if (!response.IsOK()) {
        YT_LOG_ERROR(response, "Failed to run query in subprocess (QueryId: %v, SlotIndex %v)", queryId, SlotIndex_);
        return ToErrorResponse<TQueryResult>("Failed to run query in subprocess", response);
    }

    return ToQueryResult(response.Value()->response());
}

////////////////////////////////////////////////////////////////////////////////

TQueryResult TYqlExecutorProcess::GetProgress(TQueryId queryId)
{
    YT_LOG_INFO("Getting query progress (SlotIndex: %v, QueryId: %v)", SlotIndex_, queryId);

    auto getProgressReq = PluginProxy_.GetQueryProgress();
    ToProto(getProgressReq->mutable_query_id(), queryId);
    auto response = WaitFor(getProgressReq->Invoke());

    if (!response.IsOK()) {
        YT_LOG_ERROR("Failed to get query progress from subprocess (QueryId: %v, SlotIndex %v)", queryId, SlotIndex_);
        return ToErrorResponse<TQueryResult>("Failed to get query result from subprocess", response);
    }

    return ToQueryResult(response.Value()->response());
}

////////////////////////////////////////////////////////////////////////////////

TAbortResult TYqlExecutorProcess::Abort(TQueryId queryId) {
    YT_LOG_INFO("Aborting query (SlotIndex: %v, QueryId: %v)", SlotIndex_, queryId);
    auto abortQueryReq = PluginProxy_.AbortQuery();
    ToProto(abortQueryReq->mutable_query_id(), queryId);

    auto response = WaitFor(abortQueryReq->Invoke());
    if (!response.IsOK()) {
        YT_LOG_ERROR(response, "Failed to abort query (QueryId: %v, SlotIndex: %v)", queryId, SlotIndex_);
        return ToErrorResponse<TAbortResult>("Failed to abort query", response);
    }

    TAbortResult abortResult;
    if (response.Value()->has_error()) {
        abortResult.YsonError = response.Value()->error();
    }
    return abortResult;
}

////////////////////////////////////////////////////////////////////////////////

TGetDeclaredParametersInfoResult TYqlExecutorProcess::GetDeclaredParametersInfo(
    TQueryId queryId,
    TString user,
    TString queryText,
    NYson::TYsonString settings,
    NYson::TYsonString credentials)
{
    auto getDeclaredParametersInfoReq = PluginProxy_.GetDeclaredParametersInfo();

    ToProto(getDeclaredParametersInfoReq->mutable_query_id(), queryId);
    getDeclaredParametersInfoReq->set_user(user);
    getDeclaredParametersInfoReq->set_query_text(queryText);
    getDeclaredParametersInfoReq->set_settings(settings.ToString());
    getDeclaredParametersInfoReq->set_credentials(credentials.ToString());

    auto response = WaitFor(getDeclaredParametersInfoReq->Invoke());
    if (!response.IsOK()) {
        YT_LOG_ERROR(response, "Failed to get declared parameters info");
        THROW_ERROR response;
    }

    return TGetDeclaredParametersInfoResult{
        .YsonParameters = response.Value()->yson_parameters()
    };
}

////////////////////////////////////////////////////////////////////////////////

template<typename T, typename R>
T TYqlExecutorProcess::ToErrorResponse(const TFormatString<>& errorMessage, const TErrorOr<R>& response) const {
    TError error = TError(errorMessage)
        << response
        << TErrorAttribute("slot_index", SlotIndex_);

    return T{
        .YsonError = ConvertToYsonString<TError>(error).ToString()
    };
}

////////////////////////////////////////////////////////////////////////////////

int TYqlExecutorProcess::SlotIndex() const {
    return SlotIndex_;
}

////////////////////////////////////////////////////////////////////////////////

int TYqlExecutorProcess::DynamicConfigVersion() const {
    return DynamicConfigVersion_;
}

////////////////////////////////////////////////////////////////////////////////

void TYqlExecutorProcess::OnDynamicConfigChanged(TYqlPluginDynamicConfig /*config*/) {
    // do nothing
}

////////////////////////////////////////////////////////////////////////////////

void TYqlExecutorProcess::Start() {
    // do nothing
}

////////////////////////////////////////////////////////////////////////////////

void TYqlExecutorProcess::Stop() {
    if (ActiveQueryId_) {
        Abort(*ActiveQueryId_);
    }
    YqlPluginProcess_->Kill(SIGKILL);
}

////////////////////////////////////////////////////////////////////////////////

void TYqlExecutorProcess::SubscribeOnFinish(TCallback<void (const TErrorOr<void>& )> callback) {
    ProcessFinishFuture_.Subscribe(callback);
}

////////////////////////////////////////////////////////////////////////////////

bool TYqlExecutorProcess::WaitReady() {
    // Here we are waiting for rpc server inside started subprocess to be ready to accept calls.
    YT_LOG_DEBUG("Waiting for process to be ready (SlotIndex: %v)", SlotIndex_);
    return DoWithRetry<std::exception>(
        BIND(&TYqlExecutorProcess::CheckReady, MakeStrong(this)),
        StartPluginRetryPolicy_,
        false,
        [this](const std::exception& exception) {
            YT_LOG_WARNING(exception, "Failed to start yql plugin, retrying (SlotIndex: %v)", SlotIndex_);
        });
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TQueryId> TYqlExecutorProcess::ActiveQueryId() const {
    auto guard = Guard(ActiveQueryIdLock_);
    return ActiveQueryId_;
}

////////////////////////////////////////////////////////////////////////////////

void TYqlExecutorProcess::CheckReady() {
    Y_ENSURE(NFS::Exists(UnixSocketPath_), "Unix socket must exist for process to be ready");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::TYqlPlugin::NProcess
