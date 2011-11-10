#pragma once

#include "common.h"
#include "transaction.h"

#include "../rpc/channel.h"
#include "../transaction_server/transaction_service_rpc.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Controls transactions at client-side.
/*!
 *  Provides a factory for all client-side transactions.
 *  It keeps track of all active transactions and sends pings to master servers periodically.
 */
class TTransactionManager
    : public virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
    {
        //! An internal between successive transaction pings.
        TDuration PingPeriod;
        //! A timeout for RPC requests.
        /*! 
         *  Particularly useful for
         *  #NTransaction::TTransactionServiceProxy::StartTransaction,
         *  #NTransaction::TTransactionServiceProxy::CommitTransaction and
         *  #NTransaction::TTransactionServiceProxy::AbortTransaction calls
         *  since they are done synchronously.
         */
        TDuration RpcTimeout;
    };

    //! Initializes an instance.
    /*!
     * \param config A configuration.
     * \param channel A channel used for communicating with masters.
     */
    TTransactionManager(
        const TConfig& config,
        NRpc::IChannel::TPtr channel);

    //! Starts a new transaction.
    /*!
     *  \note
     *  This call may block.
     *  Thread affinity: any.
     */
    ITransaction::TPtr StartTransaction();

private:
    typedef NTransaction::TTransactionServiceProxy TProxy;

    void PingTransaction(const TTransactionId& transactionId);
    void OnPingResponse(
        TProxy::TRspRenewTransactionLease::TPtr rsp,
        const TTransactionId& id);

    class TTransaction;

    void RegisterTransaction(TIntrusivePtr<TTransaction> transaction);
    void UnregisterTransaction(const TTransactionId& id);

    typedef yhash_map<TTransactionId, TTransaction*> TTransactionMap;

    const TConfig Config;
    NRpc::IChannel::TPtr Channel;

    TSpinLock SpinLock;
    TTransactionMap TransactionMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
