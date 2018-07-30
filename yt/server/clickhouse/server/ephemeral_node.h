#pragma once

#include <yt/server/clickhouse/interop/directory.h>

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IEphemeralNodeKeeperPtr CreateEphemeralNodeKeeper(
    NApi::IClientPtr client,
    TString directoryPath,
    TString nameHint,
    TString content,
    TDuration sessionTimeout);

}   // namespace NClickHouse
}   // namespace NYT
