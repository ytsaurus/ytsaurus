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

#include <library/cpp/json/json_reader.h>

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
        auto requestResult = NDetail::RetryRequestWithPolicy(nullptr, auth, header);
        auto response = NodeFromYsonString(requestResult.Response);
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

TString GetProxyForHeavyRequest(const TAuth& auth)
{
    if (!TConfig::Get()->UseHosts) {
        return auth.ServerName;
    }

    TString hostsEndpoint = TConfig::Get()->Hosts;
    while (hostsEndpoint.StartsWith("/")) {
        hostsEndpoint = hostsEndpoint.substr(1);
    }
    THttpHeader header("GET", hostsEndpoint, false);

    TVector<TString> hosts;
    {
        THttpRequest request;
        // TODO: we need to set socket timeout here
        request.Connect(auth.ServerName);
        request.SmallRequest(header, {});
        ParseJsonStringArray(request.GetResponse(), hosts);
    }

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

void LogRequestError(
    const THttpRequest& request,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription)
{
    LOG_ERROR("RSP %s - %s - %s - %s - X-YT-Parameters: %s",
        request.GetRequestId().data(),
        header.GetUrl().data(),
        message.data(),
        attemptDescription.data(),
        NodeToYsonString(header.GetParameters()).data());
    if (TConfig::Get()->TraceHttpRequestsMode == ETraceHttpRequestsMode::Error) {
        TraceRequest(request);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
