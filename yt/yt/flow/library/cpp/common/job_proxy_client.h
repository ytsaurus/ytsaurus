#pragma once

#include <yt/yt/client/api/public.h>

#include <util/generic/strbuf.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

NApi::IClientPtr GetJobProxyClient(
    const NApi::IClientPtr& client,
    TStringBuf cluster,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
