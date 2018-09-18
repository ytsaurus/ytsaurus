#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateLocalSnapshotService(
    const NElection::TCellId& cellId,
    TFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
