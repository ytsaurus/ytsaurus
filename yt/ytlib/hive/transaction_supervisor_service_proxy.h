#pragma once

#include "public.h"

#include <yt/ytlib/hive/transaction_supervisor_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTransactionSupervisorServiceProxy, TransactionSupervisorService,
        .SetProtocolVersion(2));

    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionSupervisor, CommitTransaction,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionSupervisor, AbortTransaction,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionSupervisor, PingTransaction,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionSupervisor, GetDownedParticipants,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
