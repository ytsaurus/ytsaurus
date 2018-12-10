#pragma once

#include "public.h"

#include "rb_torrent.h"

#include <yt/core/http/public.h>

#include <yt/core/ypath/public.h>

namespace NYT::NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

std::vector<TTableShard> ReadSkynetMetaFromTable(
    const NHttp::IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const NYPath::TYPath& path,
    const std::vector<TString>& keyColumns,
    const TProgressCallback& progressCallback);

std::vector<TRowRangeLocation> FetchSkynetPartsLocations(
    const NHttp::IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkynetManager
