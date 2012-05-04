#pragma once

#include "common.h"
#include "public.h"
#include "transaction.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/ytree/public.h>

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
     *  \note
     *  This call may block.
     *  Thread affinity: any.
     */
    ITransaction::TPtr Start(
        NYTree::IAttributeDictionary* attributes = NULL,
        const TTransactionId& parentId = NullTransactionId);

    ITransaction::TPtr Attach(const TTransactionId& id);

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
        NTransactionServer::TTransactionYPathProxy::TRspRenewLease::TPtr rsp);

    TTransactionManagerConfigPtr Config;
    NRpc::IChannelPtr Channel;
    NObjectServer::TObjectServiceProxy ObjectProxy;

    TSpinLock SpinLock;

    typedef yhash_map<TTransactionId, TWeakPtr<TTransaction> > TTransactionMap;
    TTransactionMap TransactionMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
