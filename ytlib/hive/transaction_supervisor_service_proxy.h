#pragma once

#include "public.h"

#include <yt/ytlib/hive/transaction_supervisor_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "TransactionSupervisorService";
    }

    static int GetProtocolVersion()
    {
        return 0;
    }

    explicit TTransactionSupervisorServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, CommitTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, PingTransaction);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
