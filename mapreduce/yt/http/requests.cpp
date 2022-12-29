#include "requests.h"

#include "host_manager.h"
#include "retry_request.h"

#include <mapreduce/yt/client/transaction.h>

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/node_builder.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/logging/yt_log.h>
#include <mapreduce/yt/interface/serialize.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/buffer.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TAuth& lhs, const TAuth& rhs)
{
    return lhs.ServerName == rhs.ServerName &&
           lhs.Token == rhs.Token &&
           lhs.ServiceTicketAuth == rhs.ServiceTicketAuth;
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

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TAuth& auth)
{
    if (!TConfig::Get()->UseHosts) {
        return auth.ServerName;
    }

    return NPrivate::THostManager::Get().GetProxyForHeavyRequest(auth.ServerName);
}

void LogRequestError(
    const THttpRequest& request,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription)
{
    YT_LOG_ERROR("RSP %v - %v - %v - %v - X-YT-Parameters: %v",
        request.GetRequestId(),
        header.GetUrl(),
        message,
        attemptDescription,
        NodeToYsonString(header.GetParameters()));
    if (TConfig::Get()->TraceHttpRequestsMode == ETraceHttpRequestsMode::Error) {
        TraceRequest(request);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
