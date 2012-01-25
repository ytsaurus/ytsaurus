#pragma once

#include "common.h"
#include "transaction.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Controls transactions at client-side.
/*!
 *  Provides a factory for all client-side transactions.
 *  It keeps track of all active transactions and sends pings to master servers periodically.
 */
class TTransactionManager
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! An internal between successive transaction pings.
        TDuration PingPeriod;

        //! A timeout for RPC requests to masters.
        TDuration MasterRpcTimeout;

        TConfig()
        {
            Register("ping_period", PingPeriod)
                .Default(TDuration::Seconds(5));
            Register("master_rpc_timeout", PingPeriod)
                .Default(TDuration::Seconds(5));
        }
    };

    //! Initializes an instance.
    /*!
     * \param config A configuration.
     * \param channel A channel used for communicating with masters.
     */
    TTransactionManager(
        TConfig* config,
        NRpc::IChannel* channel);

    //! Starts a new transaction.
    /*!
     *  \note
     *  This call may block.
     *  Thread affinity: any.
     */
    ITransaction::TPtr Start(const TTransactionId& parentId = NullTransactionId);

private:
    void PingTransaction(const TTransactionId& transactionId);
    void OnPingResponse(
        NTransactionServer::TTransactionYPathProxy::TRspRenewLease::TPtr rsp,
        const TTransactionId& id);

    class TTransaction;

    void RegisterTransaction(TIntrusivePtr<TTransaction> transaction);
    void UnregisterTransaction(const TTransactionId& id);

    typedef yhash_map<TTransactionId, TTransaction*> TTransactionMap;

    TConfig::TPtr Config;
    NRpc::IChannel::TPtr Channel;
    NCypress::TCypressServiceProxy CypressProxy;

    TSpinLock SpinLock;
    TTransactionMap TransactionMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
