#pragma once

#include "directory.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/yson/string.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

IEphemeralNodeKeeperPtr CreateEphemeralNodeKeeper(
    NApi::NNative::IClientPtr client,
    TString directoryPath,
    TString name,
    THashMap<TString, TString> attributes,
    TDuration sessionTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
