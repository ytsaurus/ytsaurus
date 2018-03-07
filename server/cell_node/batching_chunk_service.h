#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateBatchingChunkService(
    const NElection::TCellId& cellId,
    TBatchingChunkServiceConfigPtr serviceConfig,
    NApi::TMasterConnectionConfigPtr connectionConfig,
    NRpc::IChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
