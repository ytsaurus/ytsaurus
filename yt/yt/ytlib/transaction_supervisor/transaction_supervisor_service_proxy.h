#pragma once

#include <yt/yt/ytlib/transaction_supervisor/proto/transaction_supervisor_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTransactionSupervisorServiceProxy, TransactionSupervisorService,
        .SetProtocolVersion(2)
        .SetAcceptsBaggage(false));

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

} // namespace NYT::NTransactionSupervisor
