#pragma once

#include <yt/server/clickhouse/interop/directory.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IEphemeralNodeKeeperPtr CreateEphemeralNodeKeeper(
    NApi::IClientPtr client,
    TString directoryPath,
    TString name,
    THashMap<TString, TString> attributes,
    TDuration sessionTimeout);

}   // namespace NClickHouse
}   // namespace NYT
