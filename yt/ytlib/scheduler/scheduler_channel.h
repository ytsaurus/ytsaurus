#pragma once

#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel pointing to the scheduler of a given cell.
NRpc::IChannel::TPtr CreateSchedulerChannel(
    TNullable<TDuration> defaultTimeout,
    NRpc::IChannel::TPtr masterChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
