#pragma once

#include "fwd.h"

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/config.h>
#include <mapreduce/yt/interface/public.h>


namespace NYT {

///////////////////////////////////////////////////////////////////////////////

struct TClientContext
{
    TString ServerName;
    TString Token;
    NAuth::IServiceTicketAuthPtrWrapperPtr ServiceTicketAuth;
    NHttpClient::IHttpClientPtr HttpClient;
    bool TvmOnly = false;
    bool UseTLS = false;
    TConfigPtr Config = TConfig::Get();
};

bool operator==(const TClientContext& lhs, const TClientContext& rhs);
bool operator!=(const TClientContext& lhs, const TClientContext& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
