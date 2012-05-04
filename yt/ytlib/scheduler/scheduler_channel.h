#pragma once

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel pointing to the scheduler of a given cell.
NRpc::IChannelPtr CreateSchedulerChannel(
    TNullable<TDuration> defaultTimeout,
    NRpc::IChannelPtr masterChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
