#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/public.h>

#include <optional>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GatherHeader(const NHttp::THeadersPtr& headers, const std::string& headerName);

NYTree::IMapNodePtr ParseQueryString(TStringBuf queryString);

void FixupNodesWithAttributes(const NYTree::IMapNodePtr& node);

NYTree::IMapNodePtr HideSecretParameters(const TString& commandName, NYTree::IMapNodePtr parameters);

struct TPythonWrapperVersion
{
    int Major = 0;
    int Minor = 0;
    int Patch = 0;
};

std::optional<TPythonWrapperVersion> DetectPythonWrapper(TStringBuf userAgent);
std::optional<i64> DetectJavaIceberg(TStringBuf userAgent);
std::optional<i64> DetectGo(TStringBuf userAgent);

bool IsBrowserRequest(const NHttp::IRequestPtr& req);

bool EnableRequestBodyWorkaround(const NHttp::IRequestPtr& req);

////////////////////////////////////////////////////////////////////////////////

struct TNetworkStatistics
{
    i64 TotalRxBytes = 0;
    i64 TotalTxBytes = 0;
};

std::optional<TNetworkStatistics> GetNetworkStatistics();

////////////////////////////////////////////////////////////////////////////////

void ProcessDebugHeaders(
    const NHttp::IRequestPtr& request,
    const NHttp::IResponseWriterPtr& response,
    const TCoordinatorPtr& coordinator);

void RedirectToDataProxy(
    const NHttp::IRequestPtr& request,
    const NHttp::IResponseWriterPtr& response,
    const TCoordinatorPtr& coordinator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
