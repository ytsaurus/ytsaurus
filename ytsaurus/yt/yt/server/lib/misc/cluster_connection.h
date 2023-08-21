#pragma once

#include "private.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Download cluster connection via HTTP from `RemoteClusterProxyAddress`.
//! If address does not contain :, '.yt.yandex.net:80' will be appended automatically,
//! i.e. "hahn" -> "hahn.yt.yandex.net:80".
NYTree::INodePtr DownloadClusterConnection(
    TString remoteClusterProxyAddress,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
