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

void FixupNodesWithAttributes(const NYTree::IMapNodePtr& node);

NYTree::IMapNodePtr HideSecretParameters(const TString& commandName, NYTree::IMapNodePtr parameters);

struct TPythonWrapperVersion
{
    int Major = 0;
    int Minor = 0;
    int Patch = 0;
};

TNullable<TPythonWrapperVersion> DetectPythonWrapper(const TString& userAgent);

bool IsWrapperBuggy(const NHttp::IRequestPtr& req);

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
