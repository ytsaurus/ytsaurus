#include "stdafx.h"

#include "transaction_manager.h"
#include <yt/ytlib/transaction_manager/transaction_service_rpc.h>

#include "../actions/signal.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransaction
    : public ITransaction
{
public:
    typedef TIntrusivePtr<TTransaction> TPtr;

    TTransaction(NRpc::IChannel::TPtr channel)
        : Proxy(new TProxy(channel))
        , 
    {

    }

    TTransactionId GetId() const
    {
        return Id;
    }

    virtual void Commit() 
    {
        throw std::exception("The method or operation is not implemented.");
    }

    virtual void Abort() 
    {
        throw std::exception("The method or operation is not implemented.");
    }

    virtual void SubscribeOnCommit( IAction::TPtr callback ) 
    {
        OnCommited.Subscribe(callback);
    }

    virtual void SubscribeOnAbort(IAction::TPtr callback)
    {
        OnAborted.Subscribe(callback);
    }


private:
    TTransactionId Id;

    TSignal OnCommited;
    TSignal OnAborted;

    TAtomic FailCount;
    const int MaxFailCount;

    NRpc::IChannel::TPtr Channel;

    typedef NTransaction::TTransactionServiceProxy TProxy;

    USE_RPC_PROXY_METHOD(TProxy, StartTransaction)
    USE_RPC_PROXY_METHOD(TProxy, CommitTransaction)
    USE_RPC_PROXY_METHOD(TProxy, AbortTransaction)
    USE_RPC_PROXY_METHOD(TProxy, RenewTransactionLease)

    THolder<TProxy> Proxy;

    DECLARE_ENUM(ETransactionState,
        (Init)
        (Active)
        (Commited)
        (Aborted)
    );

    ETransactionState State;
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(NRpc::IChannel::TPtr channel)
{
}

ITransaction::TPtr TTransactionManager::StartTransaction()
{

}


    typedef yhash_map<TTransactionId, TTransaction> TTransactionMap;
    TTransactionMap TransactionMap;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT