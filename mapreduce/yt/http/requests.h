#pragma once

#include "http.h"

#include <mapreduce/yt/interface/common.h>

#include <util/generic/maybe.h>
#include <util/str_stl.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

struct TAuth
{
    TString ServerName;
    TString Token;
};

bool operator==(const TAuth& lhs, const TAuth& rhs);
bool operator!=(const TAuth& lhs, const TAuth& rhs);

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const TString& response);

TGUID ParseGuidFromResponse(const TString& response);

TRichYPath CanonizePath(
    const TAuth& auth, const TRichYPath& path);

TVector<TRichYPath> CanonizePaths(
    const TAuth& auth, const TVector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TAuth& auth);

void LogRequestError(
    const THttpRequest& request,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

template <>
struct THash<NYT::TAuth> {
    size_t operator()(const NYT::TAuth& auth) const
    {
        return CombineHashes(THash<TString>()(auth.ServerName),
                             THash<TString>()(auth.Token));
    }
};
