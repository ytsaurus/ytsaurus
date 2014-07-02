#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel pointing to the scheduler of a given cell.
NRpc::IChannelPtr CreateSchedulerChannel(
    TSchedulerConnectionConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    NRpc::IChannelPtr masterChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
