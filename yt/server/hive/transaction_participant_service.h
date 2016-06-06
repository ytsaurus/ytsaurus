#pragma once

#include "public.h"

#include <yt/server/hive/transaction_participant_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionParticipantServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "TransactionParticipantService";
    }

    static int GetProtocolVersion()
    {
        return 0;
    }

    explicit TTransactionParticipantServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, PrepareTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, CommitTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortTransaction);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
