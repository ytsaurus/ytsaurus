#pragma once

#include "public.h"

#include <yt/ytlib/transaction_client/timestamp_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTimestampServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTimestampServiceProxy, TimestampService);

    DEFINE_RPC_PROXY_METHOD(NProto, GenerateTimestamps,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

