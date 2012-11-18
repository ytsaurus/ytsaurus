#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/transaction_client//transaction_ypath_proxy.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Controls transactions at client-side.
/*!
 *  Provides a factory for all client-side transactions.
 *  Keeps track of all active transactions and sends pings to master servers periodically.
 */
class TTransactionManager
    : public virtual TRefCounted
{
public:
    //! Initializes an instance.
    /*!
     * \param config A configuration.
     * \param channel A channel used for communicating with masters.
     */
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        NRpc::IChannelPtr channel);

    //! Starts a new transaction.
    /*!
     *
     *  If #ping is True then the transaction manager will be renewing
     *  the lease of this transaction.
     *
     *  If #pingAncestors is True then the transaction manager will be renewing
     *  the leases of all ancestors of this transaction.
     *
     *  \note
     *  This call does not block.
     *  Thread affinity: any.
     */
    ITransactionPtr Start(
        NYTree::IAttributeDictionary* attributes = NULL,
        const TTransactionId& parentId = NullTransactionId,
        bool ping = true,
        bool pingAncestors = false);

    //! Attaches to an existing transaction.
    /*!
     *  If #autoAbort is True then the transaction object will be aborted
     *  (if not committed) at the end of its lifetime.
     *
     *  If #ping is True then the transaction manager will be renewing
     *  the lease of this transaction.
     *
     *  If #pingAncestors is True then the transaction manager will be renewing
     *  the leases of all ancestors of this transaction.
     *  
     *  \note
     *  This call may block.
     *  Thread affinity: any.
     */
    ITransactionPtr Attach(
        const TTransactionId& id,
        bool autoAbort,
        bool ping = true,
        bool pingAncestors = false);

private:
    class TTransaction;
    typedef TIntrusivePtr<TTransaction> TTransactionPtr;

    typedef TTransactionManager TThis;

    void RegisterTransaction(TTransactionPtr transaction);
    void UnregisterTransaction(const TTransactionId& id);
    TTransactionPtr FindTransaction(const TTransactionId& id);

    void SchedulePing(TTransactionPtr transaction);
    void SendPing(const TTransactionId& id);

    void OnPingResponse(
        const TTransactionId& id,
        TIntrusivePtr<NTransactionClient::TTransactionYPathProxy::TRspRenewLease> rsp);

    TTransactionManagerConfigPtr Config;
    NRpc::IChannelPtr Channel;
    NObjectClient::TObjectServiceProxy ObjectProxy;

    TSpinLock SpinLock;

    typedef yhash_map<TTransactionId, TWeakPtr<TTransaction> > TTransactionMap;
    TTransactionMap TransactionMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
