#pragma once

#include "common.h"
#include "transaction.h"

#include "../rpc/channel.h"
#include "../transaction_manager/transaction_service_rpc.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
    {
        TDuration PingPeriod;
        TDuration RpcTimeout;
    };

    /*!
     * \param channel - channel to master (e.g. TCellChannel)
     */
    TTransactionManager(
        const TConfig& config,
        NRpc::IChannel::TPtr channel);

    ITransaction::TPtr StartTransaction();

private:
    typedef NTransaction::TTransactionServiceProxy TProxy;

    USE_RPC_PROXY_METHOD(TProxy, StartTransaction)
    USE_RPC_PROXY_METHOD(TProxy, CommitTransaction)
    USE_RPC_PROXY_METHOD(TProxy, AbortTransaction)
    USE_RPC_PROXY_METHOD(TProxy, RenewTransactionLease)

    void PingTransaction(const TTransactionId& transactionId);
    void OnPingResponse(
        TRspRenewTransactionLease::TPtr rsp,
        const TTransactionId& transactionId);

    class TTransaction;

    void RegisterTransaction(TIntrusivePtr<TTransaction> tx);
    void UnregisterTransaction(const TTransactionId& transactionId);

    typedef yhash_map<TTransactionId, TTransaction*> TTransactionMap;

    TSpinLock SpinLock;
    TTransactionMap TransactionMap;

    const TConfig Config;
    NRpc::IChannel::TPtr Channel;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
