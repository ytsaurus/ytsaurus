#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> SyncHiveCellWithOthers(
    const TCellDirectoryPtr& cellDirectory,
    const std::vector<TCellId>& srcCellIds,
    TCellId dstCellId,
    TDuration rpcTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
