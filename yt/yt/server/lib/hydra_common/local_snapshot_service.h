#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateLocalSnapshotService(
    NElection::TCellId cellId,
    TFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
