#include "requests.h"

#include "retry_request.h"

#include <mapreduce/yt/client/transaction.h>

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/interface/logging/log.h>
#include <mapreduce/yt/common/node_builder.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/serialize.h>

#include <library/json/json_reader.h>

#include <util/random/normal.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/buffer.h>
#include <util/generic/ymath.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TAuth& lhs, const TAuth& rhs)
{
    return lhs.ServerName == rhs.ServerName &&
           lhs.Token == rhs.Token;
}

bool operator!=(const TAuth& lhs, const TAuth& rhs)
{
    return !(rhs == lhs);
}

////////////////////////////////////////////////////////////////////////////////

static TString GetDefaultTransactionTitle(const TProcessState& processState)
{
    TStringStream res;

    res << "User transaction. Created by: " << processState.UserName << " on " << processState.HostName
        << " client: " << processState.ClientVersion << " pid: " << processState.Pid;
    if (!processState.CommandLine.empty()) {
        res << " command line:";
        for (const auto& arg : processState.CommandLine) {
            res << ' ' << arg;
        }
    } else {
        res << " command line is unknown probably NYT::Initialize was never called";
    }
    return res.Str();
}

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const TString& response)
{
    return GetBool(NodeFromYsonString(response));
}

TGUID ParseGuidFromResponse(const TString& response)
{
    auto node = NodeFromYsonString(response);
    return GetGuid(node.AsString());
}

void ParseJsonStringArray(const TString& response, TVector<TString>& result)
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

TRichYPath CanonizePath(const TAuth& auth, const TRichYPath& path)
{
    TRichYPath result;
    if (path.Path_.find_first_of("<>{}[]") != TString::npos) {
        THttpHeader header("GET", "parse_ypath");
        auto pathNode = PathToNode(path);
        header.AddParameter("path", pathNode);
        auto response = NodeFromYsonString(RetryRequest(auth, header));
        for (const auto& item : pathNode.GetAttributes().AsMap()) {
            response.Attributes()[item.first] = item.second;
        }
        Deserialize(result, response);
    } else {
        result = path;
    }
    result.Path_ = AddPathPrefix(result.Path_);
    return result;
}

TVector<TRichYPath> CanonizePaths(const TAuth& auth, const TVector<TRichYPath>& paths)
{
    TVector<TRichYPath> result;
    for (const auto& path : paths) {
        result.push_back(CanonizePath(auth, path));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionId StartTransaction(
    const TAuth& auth,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout,
    bool pingAncestors,
    const TMaybe<TString>& title,
    const TMaybe<TNode>& maybeAttributes)
{
    THttpHeader header("POST", "start_tx");
    header.AddTransactionId(parentId);

    header.AddMutationId();
    header.AddParameter("timeout",
        static_cast<i64>((timeout ? timeout : TConfig::Get()->TxTimeout)->MilliSeconds()));
    if (pingAncestors) {
        header.AddParameter("ping_ancestor_transactions", true);
    }

    if (maybeAttributes && !maybeAttributes->IsMap()) {
        ythrow TApiUsageError() << "Attributes must be a Map node";
    }
    TNode attributes = maybeAttributes ? *maybeAttributes : TNode::CreateMap();

    if (title) {
        attributes["title"] = *title;
    } else if (!attributes.HasKey("title")) {
        attributes["title"] = GetDefaultTransactionTitle(*TProcessState::Get());
    }

    header.AddParameter("attributes", attributes);

    auto txId = ParseGuidFromResponse(RetryRequest(auth, header));
    LOG_DEBUG("Transaction %s started", GetGuidAsString(txId).data());
    return txId;
}

static void TransactionRequest(
    const TAuth& auth,
    const TString& command,
    const TTransactionId& transactionId)
{
    THttpHeader header("POST", command);
    header.AddTransactionId(transactionId);
    header.AddMutationId();
    RetryRequest(auth, header, Nothing(), false, false);
}

void AbortTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    TransactionRequest(auth, "abort_tx", transactionId);

    LOG_DEBUG("Transaction %s aborted", GetGuidAsString(transactionId).data());
}

void CommitTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId)
{
    TransactionRequest(auth, "commit_tx", transactionId);

    LOG_DEBUG("Transaction %s commited", GetGuidAsString(transactionId).data());
}

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TAuth& auth)
{
    if (!TConfig::Get()->UseHosts) {
        return auth.ServerName;
    }

    TVector<TString> hosts;
    TString hostsEndpoint = TConfig::Get()->Hosts;
    while (hostsEndpoint.StartsWith("/")) {
        hostsEndpoint = hostsEndpoint.substr(1);
    }
    THttpHeader header("GET", hostsEndpoint, false);
    TString response = RetryRequest(auth, header);
    ParseJsonStringArray(response, hosts);
    if (hosts.empty()) {
        ythrow yexception() << "returned list of proxies is empty";
    }

    if (hosts.size() < 3) {
        return hosts.front();
    }
    size_t hostIdx = -1;
    do {
        hostIdx = Abs<double>(NormalRandom<double>(0, hosts.size() / 2));
    } while (hostIdx >= hosts.size());

    return hosts[hostIdx];
}

TString RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    const TMaybe<TStringBuf>& body,
    bool isHeavy,
    bool isOperation)
{
    int retryCount = isOperation ?
        TConfig::Get()->StartOperationRetryCount :
        TConfig::Get()->RetryCount;

    header.SetToken(auth.Token);

    TDuration socketTimeout = (header.GetCommand() == "ping_tx") ?
        TConfig::Get()->PingTimeout : TDuration::Zero();

    bool needMutationId = false;
    bool needRetry = false;

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        bool hasError = false;
        TString response;
        TString requestId;
        TDuration retryInterval;

        try {
            TString hostName = auth.ServerName;
            if (isHeavy) {
                hostName = GetProxyForHeavyRequest(auth);
            }

            THttpRequest request(hostName);
            requestId = request.GetRequestId();

            if (needMutationId) {
                header.AddMutationId();
                needMutationId = false;
                needRetry = false;
            }

            if (needRetry) {
                header.AddParameter("retry", true, /* overwrite = */ true);
            } else {
                header.RemoveParameter("retry");
                needRetry = true;
            }

            request.Connect(socketTimeout);
            try {
                request.SmallRequest(header, body);
            } catch (yexception&) {
                // try to read error in response
            }
            response = request.GetResponse();
        } catch (TErrorResponse& e) {
            LogRequestError(
                requestId,
                header,
                e.GetError().GetMessage(),
                TStringBuilder() << "attempt " << attempt << " of " << retryCount);

            if (!IsRetriable(e) || attempt + 1 == retryCount) {
                throw;
            }
            if (e.IsConcurrentOperationsLimitReached()) {
                needMutationId = true;
            }

            hasError = true;
            retryInterval = GetRetryInterval(e);
        } catch (yexception& e) {
            LogRequestError(
                requestId,
                header,
                e.what(),
                TStringBuilder() << "attempt " << attempt << " of " << retryCount);

            if (attempt + 1 == retryCount) {
                throw;
            }
            hasError = true;
            retryInterval = TConfig::Get()->RetryInterval;
        }

        if (!hasError) {
            return response;
        }

        NDetail::TWaitProxy::Get()->Sleep(retryInterval);
    }

    Y_FAIL("Must be unreachable");
}

void LogRequestError(
    const TString& requestId,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription)
{
    LOG_ERROR("RSP %s - %s - %s - %s - X-YT-Parameters: %s",
        requestId.data(),
        header.GetUrl().data(),
        message.data(),
        attemptDescription.data(),
        NodeToYsonString(header.GetParameters()).data());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
