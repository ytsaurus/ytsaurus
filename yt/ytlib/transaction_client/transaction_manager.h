#pragma once

#include "common.h"
#include "transaction.h"

#include "../misc/config.h"
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
        : TConfigBase
    {
        TConfig()
        {
            Register("ping_period", PingPeriod).Default(TDuration::Seconds(5));
            Register("master_rpc_timeout", PingPeriod).Default(TDuration::Seconds(5));

            SetDefaults();
        }

        //! An internal between successive transaction pings.
        TDuration PingPeriod;

        //! A timeout for RPC requests to masters.
        /*! 
         *  Particularly useful for
         *  #NTransactionServer::TTransactionServiceProxy::StartTransaction,
         *  #NTransactionServer::TTransactionServiceProxy::CommitTransaction and
         *  #NTransactionServer::TTransactionServiceProxy::AbortTransaction calls
         *  since they are done synchronously.
         */
        TDuration MasterRpcTimeout;
    };

    //! Initializes an instance.
    /*!
     * \param config A configuration.
     * \param channel A channel used for communicating with masters.
     */
    TTransactionManager(
        const TConfig& config,
        NRpc::IChannel* channel);

    //! Starts a new transaction.
    /*!
     *  \note
     *  This call may block.
     *  Thread affinity: any.
     */
    ITransaction::TPtr StartTransaction();

private:
    typedef NTransactionServer::TTransactionServiceProxy TProxy;

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
