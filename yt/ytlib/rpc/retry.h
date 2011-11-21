#pragma once

#include "common.h"
#include "channel.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Constructs a channel that implements a simple retry policy.
/*!
 *  If a remote call fails at RPC level (see #NRpc::EErrorCode::IsRpcError)
 *  it is retried a given number of times with a given back off time.
 *  
 *  If the request is still failing, then EErrorCode::Unavailable is returned.
 *  
 *  \param underlyingChannel An underlying channel.
 *  \param backoffTime A interval between successive attempts.
 *  \param retryCount Maximum number of retry attempts.
 *  \returns A retriable channel.
 */ 
IChannel::TPtr CreateRetriableChannel(
    IChannel* underlyingChannel,
    TDuration backoffTime,
    int retryCount);


////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT