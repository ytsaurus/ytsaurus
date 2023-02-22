#include "raw_requests.h"

#include "mapreduce/yt/http/fwd.h"
#include "mapreduce/yt/http/helpers.h"
#include "mapreduce/yt/http/http_client.h"
#include "raw_batch_request.h"
#include "rpc_parameters_serialization.h"
#include "util/generic/guid.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/interface/config.h>
#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/interface/serialize.h>
#include <mapreduce/yt/interface/tvm.h>

#include <mapreduce/yt/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/scope.h>

namespace NYT::NDetail::NRawClient {

///////////////////////////////////////////////////////////////////////////////

void ExecuteBatch(
    IRequestRetryPolicyPtr retryPolicy,
    const TAuth& auth,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options)
{
    if (batchRequest.IsExecuted()) {
        ythrow yexception() << "Cannot execute batch request since it is already executed";
    }
    Y_DEFER {
        batchRequest.MarkExecuted();
    };

    const auto concurrency = options.Concurrency_.GetOrElse(50);
    const auto batchPartMaxSize = options.BatchPartMaxSize_.GetOrElse(concurrency * 5);

    if (!retryPolicy) {
        retryPolicy = CreateDefaultRequestRetryPolicy();
    }

    while (batchRequest.BatchSize()) {
        TRawBatchRequest retryBatch;

        while (batchRequest.BatchSize()) {
            auto parameters = TNode::CreateMap();
            TInstant nextTry;
            batchRequest.FillParameterList(batchPartMaxSize, &parameters["requests"], &nextTry);
            if (nextTry) {
                SleepUntil(nextTry);
            }
            parameters["concurrency"] = concurrency;
            auto body = NodeToYsonString(parameters);
            THttpHeader header("POST", "execute_batch");
            header.AddMutationId();
            NDetail::TResponseInfo result;
            try {
                result = RetryRequestWithPolicy(retryPolicy, auth, header, body);
            } catch (const yexception& e) {
                batchRequest.SetErrorResult(std::current_exception());
                retryBatch.SetErrorResult(std::current_exception());
                throw;
            }
            batchRequest.ParseResponse(std::move(result), retryPolicy.Get(), &retryBatch);
        }

        batchRequest = std::move(retryBatch);
    }
}

TNode Get(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    THttpHeader header("GET", "get");
    header.MergeParameters(SerializeParamsForGet(transactionId, path, options));
    return NodeFromYsonString(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

TNode TryGet(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    try {
        return Get(retryPolicy, auth, transactionId, path, options);
    } catch (const TErrorResponse& error) {
        if (!error.IsResolveError()) {
            throw;
        }
        return TNode();
    }
}

void Set(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    THttpHeader header("PUT", "set");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForSet(transactionId, path, options));
    auto body = NodeToYsonString(value);
    RetryRequestWithPolicy(retryPolicy, auth, header, body);
}

void MultisetAttributes(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode::TMapType& value,
    const TMultisetAttributesOptions& options)
{
    THttpHeader header("PUT", "api/v4/multiset_attributes", false);
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForMultisetAttributes(transactionId, path, options));

    auto body = NodeToYsonString(value);
    RetryRequestWithPolicy(retryPolicy, auth, header, body);
}

bool Exists(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options)
{
    THttpHeader header("GET", "exists");
    header.MergeParameters(SerializeParamsForExists(transactionId, path, options));
    return ParseBoolFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

TNodeId Create(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options)
{
    THttpHeader header("POST", "create");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForCreate(transactionId, path, type, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

TNodeId Copy(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    THttpHeader header("POST", "copy");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForCopy(transactionId, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

TNodeId Move(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    THttpHeader header("POST", "move");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForMove(transactionId, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

void Remove(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    THttpHeader header("POST", "remove");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForRemove(transactionId, path, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

TNode::TListType List(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    THttpHeader header("GET", "list");

    TYPath updatedPath = AddPathPrefix(path);
    // Translate "//" to "/"
    // Translate "//some/constom/prefix/from/config/" to "//some/constom/prefix/from/config"
    if (path.empty() && updatedPath.EndsWith('/')) {
        updatedPath.pop_back();
    }
    header.MergeParameters(SerializeParamsForList(transactionId, updatedPath, options));
    auto result = RetryRequestWithPolicy(retryPolicy, auth, header);
    return NodeFromYsonString(result.Response).AsList();
}

TNodeId Link(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    THttpHeader header("POST", "link");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForLink(transactionId, targetPath, linkPath, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

TLockId Lock(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    THttpHeader header("POST", "lock");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForLock(transactionId, path, mode, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

void Unlock(
    IRequestRetryPolicyPtr retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options)
{
    THttpHeader header("POST", "unlock");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForUnlock(transactionId, path, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void Concatenate(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options)
{
    THttpHeader header("POST", "concatenate");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForConcatenate(transactionId, sourcePaths, destinationPath, options));
    RequestWithoutRetry(auth, header);
}

void PingTx(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    THttpHeader header("POST", "ping_tx");
    header.MergeParameters(SerializeParamsForPingTx(transactionId));
    TRequestConfig requestConfig;
    requestConfig.HttpConfig = NHttpClient::THttpConfig{
        .SocketTimeout = TConfig::Get()->PingTimeout
    };
    RetryRequestWithPolicy(retryPolicy, auth, header, {}, requestConfig);
}

TOperationAttributes ParseOperationAttributes(const TNode& node)
{
    const auto& mapNode = node.AsMap();
    TOperationAttributes result;

    if (auto idNode = mapNode.FindPtr("id")) {
        result.Id = GetGuid(idNode->AsString());
    }

    if (auto typeNode = mapNode.FindPtr("type")) {
        result.Type = FromString<EOperationType>(typeNode->AsString());
    } else if (auto operationTypeNode = mapNode.FindPtr("operation_type")) {
        // COMPAT(levysotsky): "operation_type" is a deprecated synonim for "type".
        // This branch should be removed when all clusters are updated.
        result.Type = FromString<EOperationType>(operationTypeNode->AsString());
    }

    if (auto stateNode = mapNode.FindPtr("state")) {
        result.State = stateNode->AsString();
        // We don't use FromString here, because OS_IN_PROGRESS unites many states: "initializing", "running", etc.
        if (*result.State == "completed") {
            result.BriefState = EOperationBriefState::Completed;
        } else if (*result.State == "aborted") {
            result.BriefState = EOperationBriefState::Aborted;
        } else if (*result.State == "failed") {
            result.BriefState = EOperationBriefState::Failed;
        } else {
            result.BriefState = EOperationBriefState::InProgress;
        }
    }
    if (auto authenticatedUserNode = mapNode.FindPtr("authenticated_user")) {
        result.AuthenticatedUser = authenticatedUserNode->AsString();
    }
    if (auto startTimeNode = mapNode.FindPtr("start_time")) {
        result.StartTime = TInstant::ParseIso8601(startTimeNode->AsString());
    }
    if (auto finishTimeNode = mapNode.FindPtr("finish_time")) {
        result.FinishTime = TInstant::ParseIso8601(finishTimeNode->AsString());
    }
    auto briefProgressNode = mapNode.FindPtr("brief_progress");
    if (briefProgressNode && briefProgressNode->HasKey("jobs")) {
        result.BriefProgress.ConstructInPlace();
        static auto load = [] (const TNode& item) {
            // Backward compatibility with old YT versions
            return item.IsInt64() ? item.AsInt64() : item["total"].AsInt64();
        };
        const auto& jobs = (*briefProgressNode)["jobs"];
        result.BriefProgress->Aborted = load(jobs["aborted"]);
        result.BriefProgress->Completed = load(jobs["completed"]);
        result.BriefProgress->Running = jobs["running"].AsInt64();
        result.BriefProgress->Total = jobs["total"].AsInt64();
        result.BriefProgress->Failed = jobs["failed"].AsInt64();
        result.BriefProgress->Lost = jobs["lost"].AsInt64();
        result.BriefProgress->Pending = jobs["pending"].AsInt64();
    }
    if (auto briefSpecNode = mapNode.FindPtr("brief_spec")) {
        result.BriefSpec = *briefSpecNode;
    }
    if (auto specNode = mapNode.FindPtr("spec")) {
        result.Spec = *specNode;
    }
    if (auto fullSpecNode = mapNode.FindPtr("full_spec")) {
        result.FullSpec = *fullSpecNode;
    }
    if (auto unrecognizedSpecNode = mapNode.FindPtr("unrecognized_spec")) {
        result.UnrecognizedSpec = *unrecognizedSpecNode;
    }
    if (auto suspendedNode = mapNode.FindPtr("suspended")) {
        result.Suspended = suspendedNode->AsBool();
    }
    if (auto resultNode = mapNode.FindPtr("result")) {
        result.Result.ConstructInPlace();
        auto error = TYtError((*resultNode)["error"]);
        if (error.GetCode() != 0) {
            result.Result->Error = std::move(error);
        }
    }
    if (auto progressNode = mapNode.FindPtr("progress")) {
        const auto& progressMap = progressNode->AsMap();
        TMaybe<TInstant> buildTime;
        if (auto buildTimeNode = progressMap.FindPtr("build_time")) {
            buildTime = TInstant::ParseIso8601(buildTimeNode->AsString());
        }
        TJobStatistics jobStatistics;
        if (auto jobStatisticsNode = progressMap.FindPtr("job_statistics")) {
            jobStatistics = TJobStatistics(*jobStatisticsNode);
        }
        TJobCounters jobCounters;
        if (auto jobCountersNode = progressMap.FindPtr("total_job_counter")) {
            jobCounters = TJobCounters(*jobCountersNode);
        }
        result.Progress = TOperationProgress{
            .JobStatistics = std::move(jobStatistics),
            .JobCounters = std::move(jobCounters),
            .BuildTime = buildTime,
        };
    }
    if (auto eventsNode = mapNode.FindPtr("events")) {
        result.Events.ConstructInPlace().reserve(eventsNode->Size());
        for (const auto& eventNode : eventsNode->AsList()) {
            result.Events->push_back(TOperationEvent{
                eventNode["state"].AsString(),
                TInstant::ParseIso8601(eventNode["time"].AsString()),
            });
        }
    }
    if (auto alertsNode = mapNode.FindPtr("alerts")) {
        result.Alerts.ConstructInPlace();
        for (const auto& [alertType, alertError] : alertsNode->AsMap()) {
            result.Alerts->emplace(alertType, TYtError(alertError));
        }
    }

    return result;
}

TOperationAttributes GetOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    THttpHeader header("GET", "get_operation");
    header.MergeParameters(SerializeParamsForGetOperation(operationId, options));
    auto result = RetryRequestWithPolicy(retryPolicy, auth, header);
    return ParseOperationAttributes(NodeFromYsonString(result.Response));
}

void AbortOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId)
{
    THttpHeader header("POST", "abort_op");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForAbortOperation(operationId));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void CompleteOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId)
{
    THttpHeader header("POST", "complete_op");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForCompleteOperation(operationId));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void SuspendOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    THttpHeader header("POST", "suspend_op");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForSuspendOperation(operationId, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void ResumeOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    THttpHeader header("POST", "resume_op");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForResumeOperation(operationId, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

template <typename TKey>
static THashMap<TKey, i64> GetCounts(const TNode& countsNode)
{
    THashMap<TKey, i64> counts;
    for (const auto& entry : countsNode.AsMap()) {
        counts.emplace(FromString<TKey>(entry.first), entry.second.AsInt64());
    }
    return counts;
}

TListOperationsResult ListOperations(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TListOperationsOptions& options)
{
    THttpHeader header("GET", "list_operations");
    header.MergeParameters(SerializeParamsForListOperations(options));
    auto responseInfo = RetryRequestWithPolicy(retryPolicy, auth, header);
    auto resultNode = NodeFromYsonString(responseInfo.Response);

    TListOperationsResult result;
    for (const auto& operationNode : resultNode["operations"].AsList()) {
        result.Operations.push_back(ParseOperationAttributes(operationNode));
    }

    if (resultNode.HasKey("pool_counts")) {
        result.PoolCounts = GetCounts<TString>(resultNode["pool_counts"]);
    }
    if (resultNode.HasKey("user_counts")) {
        result.UserCounts = GetCounts<TString>(resultNode["user_counts"]);
    }
    if (resultNode.HasKey("type_counts")) {
        result.TypeCounts = GetCounts<EOperationType>(resultNode["type_counts"]);
    }
    if (resultNode.HasKey("state_counts")) {
        result.StateCounts = GetCounts<TString>(resultNode["state_counts"]);
    }
    if (resultNode.HasKey("failed_jobs_count")) {
        result.WithFailedJobsCount = resultNode["failed_jobs_count"].AsInt64();
    }

    result.Incomplete = resultNode["incomplete"].AsBool();

    return result;
}

void UpdateOperationParameters(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    THttpHeader header("POST", "update_op_parameters");
    header.MergeParameters(SerializeParamsForUpdateOperationParameters(operationId, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

TJobAttributes ParseJobAttributes(const TNode& node)
{
    const auto& mapNode = node.AsMap();
    TJobAttributes result;

    // Currently "get_job" returns "job_id" field and "list_jobs" returns "id" field.
    auto idNode = mapNode.FindPtr("id");
    if (!idNode) {
        idNode = mapNode.FindPtr("job_id");
    }
    if (idNode) {
        result.Id = GetGuid(idNode->AsString());
    }

    if (auto typeNode = mapNode.FindPtr("type")) {
        result.Type = FromString<EJobType>(typeNode->AsString());
    }
    if (auto stateNode = mapNode.FindPtr("state")) {
        result.State = FromString<EJobState>(stateNode->AsString());
    }
    if (auto addressNode = mapNode.FindPtr("address")) {
        result.Address = addressNode->AsString();
    }
    if (auto taskNameNode = mapNode.FindPtr("task_name")) {
        result.TaskName = taskNameNode->AsString();
    }
    if (auto startTimeNode = mapNode.FindPtr("start_time")) {
        result.StartTime = TInstant::ParseIso8601(startTimeNode->AsString());
    }
    if (auto finishTimeNode = mapNode.FindPtr("finish_time")) {
        result.FinishTime = TInstant::ParseIso8601(finishTimeNode->AsString());
    }
    if (auto progressNode = mapNode.FindPtr("progress")) {
        result.Progress = progressNode->AsDouble();
    }
    if (auto stderrSizeNode = mapNode.FindPtr("stderr_size")) {
        result.StderrSize = stderrSizeNode->AsUint64();
    }
    if (auto errorNode = mapNode.FindPtr("error")) {
        result.Error.ConstructInPlace(*errorNode);
    }
    if (auto briefStatisticsNode = mapNode.FindPtr("brief_statistics")) {
        result.BriefStatistics = *briefStatisticsNode;
    }
    if (auto inputPathsNode = mapNode.FindPtr("input_paths")) {
        const auto& inputPathNodesList = inputPathsNode->AsList();
        result.InputPaths.ConstructInPlace();
        result.InputPaths->reserve(inputPathNodesList.size());
        for (const auto& inputPathNode : inputPathNodesList) {
            TRichYPath path;
            Deserialize(path, inputPathNode);
            result.InputPaths->push_back(std::move(path));
        }
    }
    if (auto coreInfosNode = mapNode.FindPtr("core_infos")) {
        const auto& coreInfoNodesList = coreInfosNode->AsList();
        result.CoreInfos.ConstructInPlace();
        result.CoreInfos->reserve(coreInfoNodesList.size());
        for (const auto& coreInfoNode : coreInfoNodesList) {
            TCoreInfo coreInfo;
            coreInfo.ProcessId = coreInfoNode["process_id"].AsInt64();
            coreInfo.ExecutableName = coreInfoNode["executable_name"].AsString();
            if (coreInfoNode.HasKey("size")) {
                coreInfo.Size = coreInfoNode["size"].AsUint64();
            }
            if (coreInfoNode.HasKey("error")) {
                coreInfo.Error.ConstructInPlace(coreInfoNode["error"]);
            }
            result.CoreInfos->push_back(std::move(coreInfo));
        }
    }
    return result;
}

TJobAttributes GetJob(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options)
{
    THttpHeader header("GET", "get_job");
    header.MergeParameters(SerializeParamsForGetJob(operationId, jobId, options));
    auto responseInfo = RetryRequestWithPolicy(retryPolicy, auth, header);
    auto resultNode = NodeFromYsonString(responseInfo.Response);
    return ParseJobAttributes(resultNode);
}

TListJobsResult ListJobs(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options)
{
    THttpHeader header("GET", "list_jobs");
    header.MergeParameters(SerializeParamsForListJobs(operationId, options));
    auto responseInfo = RetryRequestWithPolicy(retryPolicy, auth, header);
    auto resultNode = NodeFromYsonString(responseInfo.Response);

    TListJobsResult result;

    const auto& jobNodesList = resultNode["jobs"].AsList();
    result.Jobs.reserve(jobNodesList.size());
    for (const auto& jobNode : jobNodesList) {
        result.Jobs.push_back(ParseJobAttributes(jobNode));
    }

    if (resultNode.HasKey("cypress_job_count") && !resultNode["cypress_job_count"].IsNull()) {
        result.CypressJobCount = resultNode["cypress_job_count"].AsInt64();
    }
    if (resultNode.HasKey("controller_agent_job_count") && !resultNode["controller_agent_job_count"].IsNull()) {
        result.ControllerAgentJobCount = resultNode["scheduler_job_count"].AsInt64();
    }
    if (resultNode.HasKey("archive_job_count") && !resultNode["archive_job_count"].IsNull()) {
        result.ArchiveJobCount = resultNode["archive_job_count"].AsInt64();
    }

    return result;
}

class TResponseReader
    : public IFileReader
{
public:
    TResponseReader(const TAuth& auth, THttpHeader header)
    {
        header.SetToken(auth.Token);

        if (auth.ServiceTicketAuth) {
            header.SetServiceTicket(auth.ServiceTicketAuth->Ptr->IssueServiceTicket());
        }

        auto hostName = GetProxyForHeavyRequest(auth);
        auto requestId = CreateGuidAsString();

        Response_ = auth.HttpClient->Request(GetFullUrl(hostName, auth, header), requestId, header);
        ResponseStream_ = Response_->GetResponseStream();
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        return ResponseStream_->Read(buf, len);
    }

    size_t DoSkip(size_t len) override
    {
        return ResponseStream_->Skip(len);
    }

private:
    THttpRequest Request_;
    NHttpClient::IHttpResponsePtr Response_;
    IInputStream* ResponseStream_;
};

IFileReaderPtr GetJobInput(
    const TAuth& auth,
    const TJobId& jobId,
    const TGetJobInputOptions& /* options */)
{
    THttpHeader header("GET", "get_job_input");
    header.AddParameter("job_id", GetGuidAsString(jobId));
    return new TResponseReader(auth, std::move(header));
}

IFileReaderPtr GetJobFailContext(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& /* options */)
{
    THttpHeader header("GET", "get_job_fail_context");
    header.AddOperationId(operationId);
    header.AddParameter("job_id", GetGuidAsString(jobId));
    return new TResponseReader(auth, std::move(header));
}

TString GetJobStderrWithRetries(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& /* options */)
{
    THttpHeader header("GET", "get_job_stderr");
    header.AddOperationId(operationId);
    header.AddParameter("job_id", GetGuidAsString(jobId));
    TRequestConfig config;
    config.IsHeavy = true;
    auto responseInfo = RetryRequestWithPolicy(retryPolicy, auth, header, {}, config);
    return responseInfo.Response;
}

IFileReaderPtr GetJobStderr(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& /* options */)
{
    THttpHeader header("GET", "get_job_stderr");
    header.AddOperationId(operationId);
    header.AddParameter("job_id", GetGuidAsString(jobId));
    return new TResponseReader(auth, std::move(header));
}

TMaybe<TYPath> GetFileFromCache(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    THttpHeader header("GET", "get_file_from_cache");
    header.MergeParameters(SerializeParamsForGetFileFromCache(transactionId, md5Signature, cachePath, options));
    auto responseInfo = RetryRequestWithPolicy(retryPolicy, auth, header);
    auto path = NodeFromYsonString(responseInfo.Response).AsString();
    return path.empty() ? Nothing() : TMaybe<TYPath>(path);
}

TYPath PutFileToCache(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    THttpHeader header("POST", "put_file_to_cache");
    header.MergeParameters(SerializeParamsForPutFileToCache(transactionId, filePath, md5Signature, cachePath, options));
    auto result = RetryRequestWithPolicy(retryPolicy, auth, header);
    return NodeFromYsonString(result.Response).AsString();
}

TNode::TListType SkyShareTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options)
{
    THttpHeader header("POST", "api/v1/share", /*IsApi*/ false);

    auto proxyName = auth.ServerName.substr(0,  auth.ServerName.find('.'));

    auto host = TConfig::Get()->SkynetApiHost;
    if (host == "") {
        host = "skynet." + proxyName + ".yt.yandex.net";
    }

    header.MergeParameters(SerializeParamsForSkyShareTable(proxyName, tablePaths, options));
    TAuth skyApiHost({host, "", nullptr, NHttpClient::CreateDefaultHttpClient()});
    TResponseInfo response = {};

    // As documented at https://wiki.yandex-team.ru/yt/userdoc/blob_tables/#shag3.sozdajomrazdachu
    // first request returns HTTP status code 202 (Accepted). And we need retrying until we have 200 (OK).
    while (response.HttpCode != 200) {
        response = RetryRequestWithPolicy(retryPolicy, skyApiHost, header, "");
        TWaitProxy::Get()->Sleep(TDuration::Seconds(5));
    }

    if (options.KeyColumns_) {
        return NodeFromJsonString(response.Response)["torrents"].AsList();
    } else {
        TNode torrent;

        torrent["key"] = TNode::CreateList();
        torrent["rbtorrent"] = response.Response;

        return TNode::TListType{ torrent };
    }
}

TCheckPermissionResponse ParseCheckPermissionResponse(const TNode& node)
{
    auto parseSingleResult = [] (const TNode::TMapType& node) {
        TCheckPermissionResult result;
        result.Action = ::FromString<ESecurityAction>(node.at("action").AsString());
        if (auto objectId = node.FindPtr("object_id")) {
            result.ObjectId = GetGuid(objectId->AsString());
        }
        if (auto objectName = node.FindPtr("object_name")) {
            result.ObjectName = objectName->AsString();
        }
        if (auto subjectId = node.FindPtr("subject_id")) {
            result.SubjectId = GetGuid(subjectId->AsString());
        }
        if (auto subjectName = node.FindPtr("subject_name")) {
            result.SubjectName = subjectName->AsString();
        }
        return result;
    };

    const auto& mapNode = node.AsMap();
    TCheckPermissionResponse result;
    static_cast<TCheckPermissionResult&>(result) = parseSingleResult(mapNode);
    if (auto columns = mapNode.FindPtr("columns")) {
        result.Columns.reserve(columns->AsList().size());
        for (const auto& columnNode : columns->AsList()) {
            result.Columns.push_back(parseSingleResult(columnNode.AsMap()));
        }
    }
    return result;
}

TCheckPermissionResponse CheckPermission(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    THttpHeader header("GET", "check_permission");
    header.MergeParameters(SerializeParamsForCheckPermission(user, permission, path, options));
    auto response = RetryRequestWithPolicy(retryPolicy, auth, header);
    return ParseCheckPermissionResponse(NodeFromYsonString(response.Response));
}

TVector<TTabletInfo> GetTabletInfos(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    THttpHeader header("POST", "api/v4/get_tablet_infos", false);
    header.MergeParameters(SerializeParamsForGetTabletInfos(path, tabletIndexes, options));
    auto response = RetryRequestWithPolicy(retryPolicy, auth, header);
    TVector<TTabletInfo> result;
    Deserialize(result, *NodeFromYsonString(response.Response).AsMap().FindPtr("tablets"));
    return result;
}

TVector<TTableColumnarStatistics> GetTableColumnarStatistics(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    THttpHeader header("GET", "get_table_columnar_statistics");
    header.MergeParameters(SerializeParamsForGetTableColumnarStatistics(transactionId, paths, options));
    TRequestConfig config;
    config.IsHeavy = true;
    auto requestResult = RetryRequestWithPolicy(retryPolicy, auth, header, {}, config);
    auto response = NodeFromYsonString(requestResult.Response);
    TVector<TTableColumnarStatistics> result;
    Deserialize(result, response);
    return result;
}

TMultiTablePartitions GetTablePartitions(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options)
{
    THttpHeader header("GET", "partition_tables");
    header.MergeParameters(SerializeParamsForGetTablePartitions(transactionId, paths, options));
    TRequestConfig config;
    config.IsHeavy = true;
    auto requestResult = RetryRequestWithPolicy(retryPolicy, auth, header, {}, config);
    auto response = NodeFromYsonString(requestResult.Response);
    TMultiTablePartitions result;
    Deserialize(result, response);
    return result;
}

TRichYPath CanonizeYPath(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TRichYPath& path)
{
    return CanonizeYPaths(retryPolicy, auth, {path}).front();
}

TVector<TRichYPath> CanonizeYPaths(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TVector<TRichYPath>& paths)
{
    TRawBatchRequest batch;
    TVector<NThreading::TFuture<TRichYPath>> futures;
    futures.reserve(paths.size());
    for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
        futures.push_back(batch.CanonizeYPath(paths[i]));
    }
    ExecuteBatch(retryPolicy, auth, batch, TExecuteBatchOptions{});
    TVector<TRichYPath> result;
    result.reserve(futures.size());
    for (auto& future : futures) {
        result.push_back(future.ExtractValueSync());
    }
    return result;
}

void AlterTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options)
{
    THttpHeader header("POST", "alter_table");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForAlterTable(transactionId, path, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void AlterTableReplica(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options)
{
    THttpHeader header("POST", "alter_table_replica");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForAlterTableReplica(replicaId, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void DeleteRows(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options)
{
    THttpHeader header("PUT", "delete_rows");
    header.SetInputFormat(TFormat::YsonBinary());
    header.MergeParameters(NRawClient::SerializeParametersForDeleteRows(path, options));

    auto body = NodeListToYsonString(keys);
    TRequestConfig requestConfig;
    requestConfig.IsHeavy = true;
    RetryRequestWithPolicy(retryPolicy, auth, header, body, requestConfig);
}

void FreezeTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    THttpHeader header("POST", "freeze_table");
    header.MergeParameters(SerializeParamsForFreezeTable(path, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void UnfreezeTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    THttpHeader header("POST", "unfreeze_table");
    header.MergeParameters(SerializeParamsForUnfreezeTable(path, options));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void AbortTransaction(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    THttpHeader header("POST", "abort_tx");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForAbortTransaction(transactionId));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

void CommitTransaction(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    THttpHeader header("POST", "commit_tx");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForCommitTransaction(transactionId));
    RetryRequestWithPolicy(retryPolicy, auth, header);
}

TTransactionId StartTransaction(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& parentTransactionId,
    const TStartTransactionOptions& options)
{
    THttpHeader header("POST", "start_tx");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForStartTransaction(parentTransactionId, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(retryPolicy, auth, header).Response);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
