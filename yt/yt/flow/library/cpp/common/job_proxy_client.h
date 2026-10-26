#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/cache/cache.h>

#include <util/generic/strbuf.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

NApi::IClientPtr GetJobProxyClient(
    const NApi::IClientPtr& client,
    TStringBuf cluster,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath);

//! Routes clients for the vanilla execution cluster through the local job proxy.
NClient::NCache::IClientsCachePtr CreateJobProxyClientsCache(NClient::NCache::IClientsCachePtr cache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
