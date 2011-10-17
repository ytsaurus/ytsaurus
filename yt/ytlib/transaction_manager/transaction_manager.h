#pragma once

#include "common.h"
#include "transaction.h"
#include "transaction_manager.pb.h"

#include "../misc/lease_manager.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////
    
//! Manages client transactions.
class TTransactionManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
    {
        TDuration TransactionTimeout;

        TConfig()
            : TransactionTimeout(TDuration::Seconds(60))
        { }
    };

    //! Creates an instance.
    TTransactionManager(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState);

    //! Called when a new transaction is started.
    TParamSignal<TTransaction&>& OnTransactionStarted();
    
    //! Called during transaction commit.
    TParamSignal<TTransaction&>& OnTransactionCommitted();
    
    //! Called during transaction abort.
    TParamSignal<TTransaction&>& OnTransactionAborted();

    TTransactionId StartTransaction(const NProto::TMsgCreateTransaction& message);
    TVoid CommitTransaction(const NProto::TMsgCommitTransaction& message);
    TVoid AbortTransaction(const NProto::TMsgAbortTransaction& message);

    void RenewLease(const TTransactionId& id);

    METAMAP_ACCESSORS_DECL(Transaction, TTransaction, TTransactionId);

private:
    typedef TTransactionManager TThis;

    TConfig Config;
    NMetaState::TMetaStateMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    TParamSignal<TTransaction&> OnTransactionStarted_;
    TParamSignal<TTransaction&> OnTransactionCommitted_;
    TParamSignal<TTransaction&> OnTransactionAborted_;

    virtual void OnStartLeading();
    virtual void OnStopLeading();

    void OnTransactionExpired(const TTransactionId& id);

    void CreateLease(const TTransaction& transaction);
    void CloseLease(const TTransaction& transaction);

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const;
    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream, IInvoker::TPtr invoker);
    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream, IInvoker::TPtr invoker);
    virtual void Clear();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
