#include "requests.h"

#include "error.h"
#include "transaction.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>

#include <library/json/json_reader.h>

#include <util/generic/buffer.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ParseJson(const Stroka& response, NJson::TJsonValue& value)
{
    TStringInput input(response);
    NJson::ReadJsonTree(&input, &value);
}

Stroka ParseJsonString(const Stroka& response)
{
    NJson::TJsonValue value;
    ParseJson(response, value);
    return value.GetString();
}

bool ParseBool(const Stroka& response)
{
    return ParseJsonString(response) == "true";
}

TGUID ParseGuid(const Stroka& response)
{
    return GetGuid(ParseJsonString(response));
}

void ParseStringArray(const Stroka& response, yvector<Stroka>& result)
{
    NJson::TJsonValue value;
    ParseJson(response, value);

    const NJson::TJsonValue::TArray& array = value.GetArray();
    result.clear();
    result.reserve(array.size());
    for (size_t i = 0; i < array.size(); ++i) {
        result.push_back(array[i].GetString());
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransactionId StartTransaction(
    const Stroka& serverName,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout,
    bool pingAncestors,
    const TMaybe<TNode>& attributes)
{
    THttpHeader header("POST", "start_tx");
    header.AddTransactionId(parentId);

    header.AddMutationId();
    if (timeout) {
        header.AddParam("timeout", timeout->MilliSeconds());
    }
    if (pingAncestors) {
        header.AddParam("ping_ancestor_transactions", "true");
    }
    if (attributes) {
        header.SetParameters(AttributesToJsonString(*attributes));
    }

    auto txId = RetryRequest(serverName, header);
    LOG_INFO("Transaction %s started", ~txId);
    return ParseGuid(txId);
}

void TransactionRequest(
    const Stroka& command,
    const Stroka& serverName,
    const TTransactionId& transactionId)
{
    THttpHeader header("POST", command);
    header.AddTransactionId(transactionId);
    header.AddMutationId();
    RetryRequest(serverName, header);
}

void PingTransaction(
    const Stroka& serverName,
    const TTransactionId& transactionId)
{
    TransactionRequest("ping_tx", serverName, transactionId);
}

void AbortTransaction(
    const Stroka& serverName,
    const TTransactionId& transactionId)
{
    TransactionRequest("abort_tx", serverName, transactionId);
    LOG_INFO("Transaction %s aborted", ~GetGuidAsString(transactionId));
}

void CommitTransaction(
    const Stroka& serverName,
    const TTransactionId& transactionId)
{
    TransactionRequest("commit_tx", serverName, transactionId);
    LOG_INFO("Transaction %s commited", ~GetGuidAsString(transactionId));
}

////////////////////////////////////////////////////////////////////////////////

Stroka Get(const Stroka& serverName, const TYPath& path)
{
    THttpHeader header("GET", "get");
    header.AddPath(path);
    return RetryRequest(serverName, header);
}

bool Exists(const Stroka& serverName, const TYPath& path)
{
    THttpHeader header("GET", "exists");
    header.AddPath(path);
    return ParseBool(RetryRequest(serverName, header));
}

////////////////////////////////////////////////////////////////////////////////

TOperationId StartOperation(
    const Stroka& serverName,
    const TGUID& transactionId,
    const Stroka& operationName,
    const Stroka& ysonSpec,
    bool wait)
{
    THttpHeader header("POST", operationName);
    header.AddTransactionId(transactionId);
    header.AddMutationId();

    TOperationId operationId = ParseGuid(RetryRequest(serverName, header, ysonSpec));
    LOG_INFO("Operation %s started", ~GetGuidAsString(operationId));

    if (wait) {
        WaitForOperation(serverName, operationId);
    }
    return operationId;
}

void WaitForOperation(const Stroka& serverName, const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval = TDuration::Seconds(1);

    while (true) {
        Stroka opIdStr = GetGuidAsString(operationId);
        Stroka opPath = Sprintf("//sys/operations/%s", ~opIdStr);
        if (!Exists(serverName, opPath)) {
            LOG_FATAL("Operation %s does not exist", ~opIdStr);
        }

        Stroka statePath = opPath + "/@state";
        Stroka state = ParseJsonString(Get(serverName, statePath));

        if (state == "completed") {
            LOG_INFO("Operation %s completed", ~opIdStr);
            break;

        } else if (state == "aborted") {
            LOG_FATAL("Operation %s aborted", ~opIdStr);

        } else if (state == "failed") {
            Stroka errorPath = opPath + "/@result/error";
            Stroka jsonError = Get(serverName, errorPath);
            TError error;
            error.ParseFrom(jsonError);
            LOG_FATAL("Operation %s failed", ~opIdStr);
        }

        Sleep(checkOperationStateInterval);
    }
}

void AbortOperation(const Stroka& serverName, const TOperationId& operationId)
{
    THttpHeader header("POST", "abort_op");
    header.AddOperationId(operationId);
    header.AddMutationId();
    RetryRequest(serverName, header);
}

////////////////////////////////////////////////////////////////////////////////

Stroka GetProxyForHeavyRequest(const Stroka& serverName)
{
    yvector<Stroka> result;
    while (result.empty()) {
        THttpHeader header("GET", TConfig::Get()->Hosts, false);
        Stroka response = RetryRequest(serverName, header);
        ParseStringArray(response, result);
        if (result.empty()) {
            Sleep(TConfig::Get()->RetryInterval);
        }
    }
    return result.front();
}

Stroka RetryRequest(
    const Stroka& serverName,
    THttpHeader& header,
    const Stroka& body,
    bool isHeavy)
{
    int retryCount = TConfig::Get()->RetryCount;

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        Stroka requestId;
        Stroka response;
        try {
            Stroka hostName(serverName);
            if (isHeavy) {
                hostName = GetProxyForHeavyRequest(serverName);
            }

            THttpRequest request(hostName);
            requestId = request.GetRequestId();

            if (attempt > 0) {
                header.AddParam("retry", "true");
            }

            try {
                request.Connect();
            } catch (yexception&) {
                throw;
            }
            try {
                TOutputStream* output = request.StartRequest(header);
                output->Write(body);
                request.FinishRequest();
            } catch (yexception&) {
                // try to read error in response
            }
            try {
                response = request.GetResponse();
            } catch (yexception&) {
                throw;
            }

        } catch (TErrorResponse& e) {
            LOG_ERROR("RSP %s - attempt %d failed",
                ~requestId, attempt);

            if (!e.IsRetriable() || attempt + 1 == retryCount) {
                throw;
            }
            Sleep(e.GetRetryInterval());
            continue;

        } catch (yexception& e) {
            LOG_ERROR("RSP %s - %s - attempt %d failed",
                ~requestId, e.what(), attempt);

            if (attempt + 1 == retryCount) {
                throw;
            }
            Sleep(TConfig::Get()->RetryInterval);
            continue;
        }

        return response;
    }

    return "";
}

void RetryHeavyWriteRequest(
    const Stroka& serverName,
    const TTransactionId& parentId,
    THttpHeader& header,
    const TBuffer& buffer)
{
    int retryCount = TConfig::Get()->RetryCount;

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        Stroka requestId;
        try {
            TPingableTransaction attemptTx(serverName, parentId);

            Stroka proxyName = GetProxyForHeavyRequest(serverName);
            THttpRequest request(proxyName);
            requestId = request.GetRequestId();

            header.AddTransactionId(attemptTx.GetId());

            try {
                request.Connect();
            } catch (yexception&) {
                throw;
            }
            try {
                TOutputStream* output = request.StartRequest(header);
                output->Write(buffer.Data(), buffer.Size());
                request.FinishRequest();
            } catch (yexception&) {
                // try to read error in response
            }
            try {
                request.GetResponse();
            } catch (yexception&) {
                throw;
            }

            attemptTx.Commit();

        } catch (TErrorResponse& e) {
            LOG_ERROR("RSP %s - attempt %d failed",
                ~requestId, attempt);

            if (!e.IsRetriable() || attempt + 1 == retryCount) {
                throw;
            }
            Sleep(e.GetRetryInterval());
            continue;

        } catch (yexception& e) {
            LOG_ERROR("RSP %s - %s - attempt %d failed",
                ~requestId, e.what(), attempt);

            if (attempt + 1 == retryCount) {
                throw;
            }
            Sleep(TConfig::Get()->RetryInterval);
            continue;
        }
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
