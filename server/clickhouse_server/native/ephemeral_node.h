#pragma once

#include "directory.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

IEphemeralNodeKeeperPtr CreateEphemeralNodeKeeper(
    NApi::IClientPtr client,
    TString directoryPath,
    TString name,
    THashMap<TString, TString> attributes,
    TDuration sessionTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
