#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

TNullable<TString> GatherHeader(const NHttp::THeadersPtr& headers, const TString& headerName);

NYTree::IMapNodePtr ParseQueryString(TStringBuf queryString);

////////////////////////////////////////////////////////////////////////////////

struct TNetworkStatistics
{
    i64 TotalRxBytes = 0;
    i64 TotalTxBytes = 0;
};

TNullable<TNetworkStatistics> GetNetworkStatistics();

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
