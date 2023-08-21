#pragma once

#include <yt/yt/ytlib/transaction_supervisor/proto/transaction_participant_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

class TTransactionParticipantServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTransactionParticipantServiceProxy, TransactionParticipantService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionParticipant, PrepareTransaction,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionParticipant, CommitTransaction,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto::NTransactionParticipant, AbortTransaction,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
