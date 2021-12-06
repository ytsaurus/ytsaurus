#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateLocalSnapshotService(
    NElection::TCellId cellId,
    TFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
