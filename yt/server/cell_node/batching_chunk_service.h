#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateBatchingChunkService(
    const NElection::TCellId& cellId,
    TBatchingChunkServiceConfigPtr config,
    NRpc::IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
