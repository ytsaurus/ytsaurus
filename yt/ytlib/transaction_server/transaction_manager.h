#pragma once

#include "id.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/misc/lease_manager.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/meta_change.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/object_server/object_manager.h>


// TODO(babenko): consider getting rid of this
namespace NYT {
namespace NCypress {

class TCypressManager;

}
}

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction;
class TTransactionManifest;

//! Manages client transactions.
class TTransactionManager
    : public NMetaState::TMetaStatePart
{
    //! Called when a new transaction is started.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TTransaction&>, OnTransactionStarted);
    //! Called during transaction commit.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TTransaction&>, OnTransactionCommitted);
    //! Called during transaction abort.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TTransaction&>, OnTransactionAborted);

public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration TransactionTimeout;

        TConfig()
        {
            Register("transaction_timeout", TransactionTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Seconds(10));
        }
    };

    //! Creates an instance.
    TTransactionManager(
        TConfig* config,
        NMetaState::IMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        NObjectServer::TObjectManager* objectManager);

    void SetCypressManager(NCypress::TCypressManager* cypressManager);
    NObjectServer::TObjectManager::TPtr GetObjectManager() const;

    TTransaction& Start(TTransactionManifest* manifest);
    void Commit(TTransaction& transaction);
    void Abort(TTransaction& transaction);
    void RenewLease(const TTransactionId& id);

    DECLARE_METAMAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

private:
    typedef TTransactionManager TThis;
    class TTransactionTypeHandler;
    class TTransactionProxy;
    friend class TTransactionProxy;

    TConfig::TPtr Config;
    NObjectServer::TObjectManager::TPtr ObjectManager;
    TIntrusivePtr<NCypress::TCypressManager> CypressManager;

    NMetaState::TMetaStateMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

    void OnTransactionExpired(const TTransactionId& id);

    void CreateLease(const TTransaction& transaction);
    void CloseLease(const TTransaction& transaction);
    void FinishTransaction(TTransaction& transaction);

    // TMetaStatePart overrides
    TFuture<TVoid>::TPtr Save(const NMetaState::TCompositeMetaState::TSaveContext& context);
    void Load(TInputStream* input);
    virtual void Clear();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
