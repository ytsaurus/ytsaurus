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

bool ParseBool(const Stroka& response)
{
    auto node = NodeFromYsonString(response);
    if (node.IsBool()) {
        return node.AsBool();
    } else if (node.IsString()) {
        return node.AsString() == "true";
    } else {
        LOG_FATAL("Cannot parse yson boolean for response '%s'", ~response);
    }
    return false;
}

TGUID ParseGuid(const Stroka& response)
{
    auto node = NodeFromYsonString(response);
    return GetGuid(node.AsString());
}

void ParseJsonStringArray(const Stroka& response, yvector<Stroka>& result)
{
    NJson::TJsonValue value;
    TStringInput input(response);
    NJson::ReadJsonTree(&input, &value);

    const NJson::TJsonValue::TArray& array = value.GetArray();
    result.clear();
    result.reserve(array.size());
    for (size_t i = 0; i < array.size(); ++i) {
        result.push_back(array[i].GetString());
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransactionId StartTransaction(
    const TAuth& auth,
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

    auto txId = ParseGuid(RetryRequest(auth, header));
    LOG_INFO("Transaction %s started", ~GetGuidAsString(txId));
    return txId;
}

void TransactionRequest(
    const TAuth& auth,
    const Stroka& command,
    const TTransactionId& transactionId)
{
    THttpHeader header("POST", command);
    header.AddTransactionId(transactionId);
    header.AddMutationId();
    RetryRequest(auth, header);
}

void PingTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    TransactionRequest(auth, "ping_tx", transactionId);
}

void AbortTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    TransactionRequest(auth, "abort_tx", transactionId);
    LOG_INFO("Transaction %s aborted", ~GetGuidAsString(transactionId));
}

void CommitTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    TransactionRequest(auth, "commit_tx", transactionId);
    LOG_INFO("Transaction %s commited", ~GetGuidAsString(transactionId));
}

////////////////////////////////////////////////////////////////////////////////

Stroka Get(const TAuth& auth, const TYPath& path)
{
    THttpHeader header("GET", "get");
    header.AddPath(path);
    return RetryRequest(auth, header);
}

bool Exists(const TAuth& auth, const TYPath& path)
{
    THttpHeader header("GET", "exists");
    header.AddPath(path);
    return ParseBool(RetryRequest(auth, header));
}

////////////////////////////////////////////////////////////////////////////////

TOperationId StartOperation(
    const TAuth& auth,
    const TGUID& transactionId,
    const Stroka& operationName,
    const Stroka& ysonSpec,
    bool wait)
{
    THttpHeader header("POST", operationName);
    header.AddTransactionId(transactionId);
    header.AddMutationId();

    TOperationId operationId = ParseGuid(RetryRequest(auth, header, ysonSpec));
    LOG_INFO("Operation %s started", ~GetGuidAsString(operationId));

    if (wait) {
        WaitForOperation(auth, operationId);
    }
    return operationId;
}

void WaitForOperation(const TAuth& auth, const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval = TDuration::Seconds(1);

    Stroka opIdStr = GetGuidAsString(operationId);
    Stroka opPath = Sprintf("//sys/operations/%s", ~opIdStr);
    Stroka statePath = opPath + "/@state";

    while (true) {
       if (!Exists(auth, opPath)) {
            LOG_FATAL("Operation %s does not exist", ~opIdStr);
        }

        Stroka state = NodeFromYsonString(Get(auth, statePath)).AsString();
        if (state == "completed") {
            LOG_INFO("Operation %s completed", ~opIdStr);
            break;

        } else if (state == "aborted") {
            LOG_FATAL("Operation %s aborted", ~opIdStr);

        } else if (state == "failed") {
            Stroka errorPath = opPath + "/@result/error";
            Stroka jsonError = Get(auth, errorPath);
            TError error;
            error.ParseFrom(jsonError);
            LOG_FATAL("Operation %s failed", ~opIdStr);
        }

        Sleep(checkOperationStateInterval);
    }
}

void AbortOperation(const TAuth& auth, const TOperationId& operationId)
{
    THttpHeader header("POST", "abort_op");
    header.AddOperationId(operationId);
    header.AddMutationId();
    RetryRequest(auth, header);
}

////////////////////////////////////////////////////////////////////////////////

Stroka GetProxyForHeavyRequest(const TAuth& auth)
{
    yvector<Stroka> result;
    while (result.empty()) {
        THttpHeader header("GET", TConfig::Get()->Hosts, false);
        Stroka response = RetryRequest(auth, header);
        ParseJsonStringArray(response, result);
        if (result.empty()) {
            Sleep(TConfig::Get()->RetryInterval);
        }
    }
    return result.front();
}

Stroka RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    const Stroka& body,
    bool isHeavy)
{
    int retryCount = TConfig::Get()->RetryCount;
    header.SetToken(auth.Token);

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        Stroka requestId;
        Stroka response;
        try {
            Stroka hostName(auth.ServerName);
            if (isHeavy) {
                hostName = GetProxyForHeavyRequest(auth);
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
    const TAuth& auth,
    const TTransactionId& parentId,
    THttpHeader& header,
    const TBuffer& buffer)
{
    int retryCount = TConfig::Get()->RetryCount;
    header.SetToken(auth.Token);

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        Stroka requestId;
        try {
            TPingableTransaction attemptTx(auth, parentId);

            Stroka proxyName = GetProxyForHeavyRequest(auth);
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
