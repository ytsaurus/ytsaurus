#pragma once

#include "public.h"

#include <yt/ytlib/hive/proto/transaction_participant_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NHiveClient {

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

} // namespace NHiveClient
} // namespace NYT
