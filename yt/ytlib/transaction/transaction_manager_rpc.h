#pragma once

#include "common.h"
#include "transaction_manager_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TTransactionManagerProxy> TPtr;

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode,
        ((NoSuchTransaction)(1))
    );

    static Stroka GetServiceName()
    {
        return "TransactionManager";
    }

    TTransactionManagerProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, StartTransaction);
    RPC_PROXY_METHOD(NProto, CommitTransaction);
    RPC_PROXY_METHOD(NProto, AbortTransaction);
    RPC_PROXY_METHOD(NProto, RenewTransactionLease);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
