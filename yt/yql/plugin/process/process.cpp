#include "process.h"
#include "private.h"

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/process/process.h>

namespace NYT::NYqlPlugin::NProcess {

static constexpr auto& Logger = YqlExecutorProcessLogger;

using namespace NConcurrency;
using namespace NYson;

using NYqlClient::NProto::TYqlQueryFile_EContentType;
using NYqlClient::NProto::TYqlResponse;

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

TClustersResult TYqlExecutorProcess::GetUsedClusters(
    TQueryId queryId,
    TString queryText,
    TYsonString settings,
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
        YT_TLOG_ERROR("Failed to get cluster result from subprocess")
            .With("QueryId", queryId)
            .With("SlotIndex", SlotIndex_)
            .With(response);
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

TQueryResult TYqlExecutorProcess::Run(
    TQueryId queryId,
    TString user,
    TYsonString credentials,
    TString queryText,
    TYsonString settings,
    std::vector<TQueryFile> files,
    int executeMode,
    NYqlClient::EQueryType queryType)
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
    runQueryReq->set_query_type(ToProto(queryType));

    auto response = WaitFor(runQueryReq->Invoke());
    if (!response.IsOK()) {
        YT_TLOG_ERROR("Failed to run query in subprocess")
            .With("QueryId", queryId)
            .With("SlotIndex", SlotIndex_)
            .With(response);
        return ToErrorResponse<TQueryResult>("Failed to run query in subprocess", response);
    }

    return ToQueryResult(response.Value()->response());
}

TQueryResult TYqlExecutorProcess::GetProgress(TQueryId queryId)
{
    YT_TLOG_INFO("Getting query progress")
        .With("SlotIndex", SlotIndex_)
        .With("QueryId", queryId);

    auto getProgressReq = PluginProxy_.GetQueryProgress();
    ToProto(getProgressReq->mutable_query_id(), queryId);
    auto response = WaitFor(getProgressReq->Invoke());

    if (!response.IsOK()) {
        YT_TLOG_ERROR("Failed to get query progress from subprocess")
            .With("QueryId", queryId)
            .With("SlotIndex", SlotIndex_)
            .With(response);
        return ToErrorResponse<TQueryResult>("Failed to get query progress from subprocess", response);
    }

    return ToQueryResult(response.Value()->response());
}

TAbortResult TYqlExecutorProcess::Abort(TQueryId queryId)
{
    YT_TLOG_INFO("Aborting query")
        .With("SlotIndex", SlotIndex_)
        .With("QueryId", queryId);
    auto abortQueryReq = PluginProxy_.AbortQuery();
    ToProto(abortQueryReq->mutable_query_id(), queryId);

    auto response = WaitFor(abortQueryReq->Invoke());
    if (!response.IsOK()) {
        YT_TLOG_ERROR("Failed to abort query")
            .With("QueryId", queryId)
            .With("SlotIndex", SlotIndex_)
            .With(response);
        return ToErrorResponse<TAbortResult>("Failed to abort query", response);
    }

    TAbortResult abortResult;
    if (response.Value()->has_error()) {
        abortResult.YsonError = response.Value()->error();
    }
    return abortResult;
}

TGetDeclaredParametersInfoResult TYqlExecutorProcess::GetDeclaredParametersInfo(
    TQueryId queryId,
    TString user,
    TString queryText,
    TYsonString settings,
    TYsonString credentials)
{
    auto getDeclaredParametersInfoReq = PluginProxy_.GetDeclaredParametersInfo();

    ToProto(getDeclaredParametersInfoReq->mutable_query_id(), queryId);
    getDeclaredParametersInfoReq->set_user(user);
    getDeclaredParametersInfoReq->set_query_text(queryText);
    getDeclaredParametersInfoReq->set_settings(settings.ToString());
    getDeclaredParametersInfoReq->set_credentials(credentials.ToString());

    auto response = WaitFor(getDeclaredParametersInfoReq->Invoke());
    if (!response.IsOK()) {
        YT_TLOG_ERROR("Failed to get declared parameters info")
            .With(response);
        THROW_ERROR response;
    }

    return TGetDeclaredParametersInfoResult{
        .YsonParameters = response.Value()->yson_parameters()
    };
}

template<typename T, typename R>
T TYqlExecutorProcess::ToErrorResponse(const TFormatString<>& errorMessage, const TErrorOr<R>& response) const
{
    TError error = TError(errorMessage)
        .With(response)
        .With("slot_index", SlotIndex_);

    return T{
        .YsonError = ConvertToYsonString<TError>(error).ToString()
    };
}

int TYqlExecutorProcess::SlotIndex() const
{
    return SlotIndex_;
}

int TYqlExecutorProcess::DynamicConfigVersion() const
{
    return DynamicConfigVersion_;
}

void TYqlExecutorProcess::OnDynamicConfigChanged(TYqlPluginDynamicConfigPtr /*config*/)
{
    // do nothing
}

void TYqlExecutorProcess::OnUdfMetaChanged(TUdfMetaPtr /*udfMeta*/)
{
    // Not implemented
}

void TYqlExecutorProcess::RegisterQuery(TQueryId queryId)
{
    auto registerQueryReq = PluginProxy_.RegisterQuery();

    ToProto(registerQueryReq->mutable_query_id(), queryId);

    auto response = WaitFor(registerQueryReq->Invoke());
    if (!response.IsOK()) {
        YT_TLOG_ERROR("Failed to register query")
            .With("QueryId", queryId)
            .With("SlotIndex", SlotIndex_)
            .With(response);

        THROW_ERROR response;
    }
}

void TYqlExecutorProcess::UnregisterQuery(TQueryId queryId)
{
    auto unregisterQueryReq = PluginProxy_.UnregisterQuery();

    ToProto(unregisterQueryReq->mutable_query_id(), queryId);

    auto response = WaitFor(unregisterQueryReq->Invoke());
    if (!response.IsOK()) {
        YT_TLOG_ERROR("Failed to unregister query")
            .With("QueryId", queryId)
            .With("SlotIndex", SlotIndex_)
            .With(response);

        THROW_ERROR response;
    }
}

void TYqlExecutorProcess::Start()
{
    // do nothing
}

void TYqlExecutorProcess::Stop()
{
    if (ActiveQueryId_) {
        Abort(*ActiveQueryId_);
    }
    YqlPluginProcess_->Kill(SIGKILL);
}

void TYqlExecutorProcess::SubscribeOnFinish(TCallback<void (const TErrorOr<void>&)> callback)
{
    ProcessFinishFuture_.Subscribe(callback);
}

bool TYqlExecutorProcess::WaitReady()
{
    // Here we are waiting for rpc server inside started subprocess to be ready to accept calls.
    YT_TLOG_DEBUG("Waiting for process to be ready")
        .With("SlotIndex", SlotIndex_);
    return DoWithRetry<std::exception>(
        BIND(&TYqlExecutorProcess::CheckReady, MakeStrong(this)),
        StartPluginRetryPolicy_,
        false,
        [this](const std::exception& exception) {
            YT_TLOG_WARNING("Failed to start yql plugin, retrying")
                .With("SlotIndex", SlotIndex_)
                .With(exception);
        });
}

std::optional<TQueryId> TYqlExecutorProcess::ActiveQueryId() const
{
    auto guard = Guard(ActiveQueryIdLock_);
    return ActiveQueryId_;
}

void TYqlExecutorProcess::CheckReady()
{
    THROW_ERROR_EXCEPTION_UNLESS(
        NFS::Exists(UnixSocketPath_),
        "Unix socket must exist for process to be ready");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::TYqlPlugin::NProcess
