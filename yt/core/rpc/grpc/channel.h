#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpc {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via GRPC.
NRpc::IChannelPtr CreateGrpcChannel(TChannelConfigPtr config);

//! Returns the factory for creating GRPC channels.
NRpc::IChannelFactoryPtr GetGrpcChannelFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT
