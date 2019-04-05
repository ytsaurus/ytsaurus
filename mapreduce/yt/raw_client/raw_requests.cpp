#include "raw_requests.h"

#include "raw_batch_request.h"
#include "rpc_parameters_serialization.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/http/retry_request.h>
#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/interface/serialize.h>
#include <mapreduce/yt/node/node_io.h>

namespace NYT::NDetail::NRawClient {

///////////////////////////////////////////////////////////////////////////////

void ExecuteBatch(
    const TAuth& auth,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    if (batchRequest.IsExecuted()) {
        ythrow yexception() << "Cannot execute batch request since it is already executed";
    }
    NDetail::TFinallyGuard g([&] {
        batchRequest.MarkExecuted();
    });

    const auto concurrency = options.Concurrency_.GetOrElse(50);
    const auto batchPartMaxSize = options.BatchPartMaxSize_.GetOrElse(concurrency * 5);

    IRequestRetryPolicyPtr defaultRetryPolicy = nullptr;
    if (!retryPolicy) {
        defaultRetryPolicy = CreateDefaultRetryPolicy();
        retryPolicy = defaultRetryPolicy.Get();
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
                result = RetryRequestWithPolicy(auth, header, body, retryPolicy);
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
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "get");
    header.MergeParameters(SerializeParamsForGet(transactionId, path, options));
    return NodeFromYsonString(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("PUT", "set");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForSet(transactionId, path, options));
    auto body = NodeToYsonString(value);
    RetryRequestWithPolicy(auth, header, body, retryPolicy);
}

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "exists");
    header.MergeParameters(SerializeParamsForExists(transactionId, path));
    return ParseBoolFromResponse(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

TNodeId Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "create");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForCreate(transactionId, path, type, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

TNodeId Copy(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "copy");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForCopy(transactionId, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

TNodeId Move(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "move");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForMove(transactionId, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

void Remove(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "remove");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForRemove(transactionId, path, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

TNode::TListType List(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "list");

    TYPath updatedPath = AddPathPrefix(path);
    // Translate "//" to "/"
    // Translate "//some/constom/prefix/from/config/" to "//some/constom/prefix/from/config"
    if (path.empty() && updatedPath.EndsWith('/')) {
        updatedPath.pop_back();
    }
    header.MergeParameters(SerializeParamsForList(transactionId, updatedPath, options));
    auto result = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    return NodeFromYsonString(result.Response).AsList();
}

TNodeId Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "link");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForLink(transactionId, targetPath, linkPath, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

TLockId Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "lock");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForLock(transactionId, path, mode, options));
    return ParseGuidFromResponse(RetryRequestWithPolicy(auth, header, "", retryPolicy).Response);
}

void Concatenate(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "concatenate");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForConcatenate(transactionId, sourcePaths, destinationPath, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void PingTx(
    const TAuth& auth,
    const TTransactionId& transactionId,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "ping_tx");
    header.MergeParameters(SerializeParamsForPingTx(transactionId));
    TRequestConfig requestConfig;
    requestConfig.SocketTimeout = TConfig::Get()->PingTimeout;
    RetryRequestWithPolicy(auth, header, "", retryPolicy, requestConfig);
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
        result.Progress = TOperationProgress{TJobStatistics((*progressNode)["job_statistics"])};
    }
    if (auto eventsNode = mapNode.FindPtr("events")) {
        result.Events.ConstructInPlace().reserve(eventsNode->Size());
        for (const auto& eventNode : eventsNode->AsList()) {
            result.Events->push_back(TOperationEvent{
                eventNode["state"].AsString(),
                TInstant::ParseIso8601(eventNode["time"].AsString())});
        }
    }

    return result;
}

TOperationAttributes GetOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "get_operation");
    header.MergeParameters(SerializeParamsForGetOperation(operationId, options));
    auto result = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    return ParseOperationAttributes(NodeFromYsonString(result.Response));
}

void AbortOperation(const TAuth& auth, const TOperationId& operationId, IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "abort_op");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForAbortOperation(operationId));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void CompleteOperation(const TAuth& auth, const TOperationId& operationId, IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "complete_op");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForCompleteOperation(operationId));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
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
    const TAuth& auth,
    const TListOperationsOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "list_operations");
    header.MergeParameters(SerializeParamsForListOperations(options));
    auto responseInfo = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    auto resultNode = NodeFromYsonString(responseInfo.Response);

    TListOperationsResult result;
    for (const auto operationNode : resultNode["operations"].AsList()) {
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
    const TAuth& auth,
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "update_op_parameters");
    header.MergeParameters(SerializeParamsForUpdateOperationParameters(operationId, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
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
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "get_job");
    header.MergeParameters(SerializeParamsForGetJob(operationId, jobId, options));
    auto responseInfo = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    auto resultNode = NodeFromYsonString(responseInfo.Response);
    return ParseJobAttributes(resultNode);
}

TListJobsResult ListJobs(
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "list_jobs");
    header.MergeParameters(SerializeParamsForListJobs(operationId, options));
    auto responseInfo = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    auto resultNode = NodeFromYsonString(responseInfo.Response);

    TListJobsResult result;

    const auto& jobNodesList = resultNode["jobs"].AsList();
    result.Jobs.reserve(jobNodesList.size());
    for (const auto jobNode : jobNodesList) {
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
        : Request_(GetProxyForHeavyRequest(auth))
    {
        header.SetToken(auth.Token);

        Request_.Connect();
        Request_.StartRequest(header);
        Request_.FinishRequest();

        Response_ = Request_.GetResponseStream();
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        return Response_->Read(buf, len);
    }

    size_t DoSkip(size_t len) override
    {
        return Response_->Skip(len);
    }

private:
    THttpRequest Request_;
    THttpResponse* Response_;
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
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& /* options */,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "get_job_stderr");
    header.AddOperationId(operationId);
    header.AddParameter("job_id", GetGuidAsString(jobId));
    TRequestConfig config;
    config.IsHeavy = true;
    auto responseInfo = RetryRequestWithPolicy(auth, header, "", retryPolicy, config);
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
    const TAuth& auth,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("GET", "get_file_from_cache");
    header.MergeParameters(SerializeParamsForGetFileFromCache(md5Signature, cachePath, options));
    auto responseInfo = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    auto path = NodeFromYsonString(responseInfo.Response).AsString();
    return path.empty() ? Nothing() : TMaybe<TYPath>(path);
}

TYPath PutFileToCache(
    const TAuth& auth,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "put_file_to_cache");
    header.MergeParameters(SerializeParamsForPutFileToCache(filePath, md5Signature, cachePath, options));
    auto result = RetryRequestWithPolicy(auth, header, "", retryPolicy);
    return NodeFromYsonString(result.Response).AsString();
}

void AlterTable(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "alter_table");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForAlterTable(transactionId, path, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void AlterTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "alter_table_replica");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForAlterTableReplica(replicaId, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void DeleteRows(
    const TAuth& auth,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("PUT", "delete_rows");
    header.SetInputFormat(TFormat::YsonBinary());
    header.MergeParameters(NRawClient::SerializeParametersForDeleteRows(path, options));

    auto body = NodeListToYsonString(keys);
    TRequestConfig requestConfig;
    requestConfig.IsHeavy = true;
    RetryRequestWithPolicy(auth, header, body, retryPolicy, requestConfig);
}

void EnableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "enable_table_replica");
    header.MergeParameters(SerializeParamsForEnableTableReplica(replicaId));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void DisableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "disable_table_replica");
    header.MergeParameters(SerializeParamsForDisableTableReplica(replicaId));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void FreezeTable(
    const TAuth& auth,
    const TYPath& path,
    const TFreezeTableOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "freeze_table");
    header.MergeParameters(SerializeParamsForFreezeTable(path, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

void UnfreezeTable(
    const TAuth& auth,
    const TYPath& path,
    const TUnfreezeTableOptions& options,
    IRequestRetryPolicyPtr retryPolicy)
{
    THttpHeader header("POST", "unfreeze_table");
    header.MergeParameters(SerializeParamsForUnfreezeTable(path, options));
    RetryRequestWithPolicy(auth, header, "", retryPolicy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
