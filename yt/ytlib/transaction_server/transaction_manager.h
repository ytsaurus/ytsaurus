#pragma once

#include "common.h"
#include "transaction.h"
#include "transaction_manager.pb.h"

#include "../misc/property.h"
#include "../misc/id_generator.h"
#include "../misc/lease_manager.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_change.h"
#include "../meta_state/map.h"

namespace NYT {
namespace NTransaction {

// TODO: get rid
using NMetaState::TMetaChange;

////////////////////////////////////////////////////////////////////////////////
    
//! Manages client transactions.
class TTransactionManager
    : public NMetaState::TMetaStatePart
{
    //! Called when a new transaction is started.
    DECLARE_BYREF_RW_PROPERTY(OnTransactionStarted, TParamSignal<const TTransaction&>);
    //! Called during transaction commit.
    DECLARE_BYREF_RW_PROPERTY(OnTransactionCommitted, TParamSignal<const TTransaction&>);
    //! Called during transaction abort.
    DECLARE_BYREF_RW_PROPERTY(OnTransactionAborted, TParamSignal<const TTransaction&>);

public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
    {
        TDuration TransactionTimeout;

        TConfig()
            : TransactionTimeout(TDuration::Seconds(10))
        { }
    };

    //! Creates an instance.
    TTransactionManager(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState);

    TMetaChange<TTransactionId>::TPtr InitiateStartTransaction();
    TMetaChange<TVoid>::TPtr          InitiateCommitTransaction(const TTransactionId& id);
    TMetaChange<TVoid>::TPtr          InitiateAbortTransaction(const TTransactionId& id);

    void RenewLease(const TTransactionId& id);

    METAMAP_ACCESSORS_DECL(Transaction, TTransaction, TTransactionId);

private:
    typedef TTransactionManager TThis;

    TConfig Config;

    TIdGenerator<TTransactionId> TransactionIdGenerator;
    NMetaState::TMetaStateMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    TTransactionId StartTransaction(const NProto::TMsgStartTransaction& message);
    TVoid CommitTransaction(const NProto::TMsgCommitTransaction& message);
    TVoid AbortTransaction(const NProto::TMsgAbortTransaction& message);

    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

    void OnTransactionExpired(const TTransactionId& id);

    void CreateLease(const TTransaction& transaction);
    void CloseLease(const TTransaction& transaction);

    // TMetaStatePart overrides
    TFuture<TVoid>::TPtr Save(const NMetaState::TCompositeMetaState::TSaveContext& context);
    void Load(TInputStream* input);
    virtual void Clear();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
