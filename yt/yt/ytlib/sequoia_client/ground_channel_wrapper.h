#pragma once

#include "public.h"

namespace NYT::NSequoiaClient {

///////////////////////////////////////////////////////////////////////////////

//! Wraps a channel so that any errors get wrapped into a SequoiaRetriableError.
NRpc::IChannelPtr WrapGroundChannel(NRpc::IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
