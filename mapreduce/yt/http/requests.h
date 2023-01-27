#pragma once

#include "fwd.h"
#include "http.h"

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/public.h>

#include <util/generic/maybe.h>
#include <util/str_stl.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

struct TAuth
{
    TString ServerName;
    TString Token;
    NAuth::IServiceTicketAuthPtrWrapperPtr ServiceTicketAuth;
    NHttpClient::IHttpClientPtr HttpClient;
};

bool operator==(const TAuth& lhs, const TAuth& rhs);
bool operator!=(const TAuth& lhs, const TAuth& rhs);

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const TString& response);

TGUID ParseGuidFromResponse(const TString& response);

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TAuth& auth);

void LogRequestError(
    const TString& requestId,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
