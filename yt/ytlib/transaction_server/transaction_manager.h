#pragma once

#include "public.h"

#include <ytlib/actions/bind.h>
#include <ytlib/actions/signal.h>
#include <ytlib/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/misc/lease_manager.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/meta_change.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/object_server/object_manager.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction;
struct TTransactionManifest;

//! Manages client transactions.
class TTransactionManager
    : public NMetaState::TMetaStatePart
{
    // TODO(babenko): clarify what "during" means
    //! Raised when a new transaction is started.
    DEFINE_SIGNAL(void(TTransaction&), TransactionStarted);
    //! Raised during transaction commit.
    DEFINE_SIGNAL(void(TTransaction&), TransactionCommitted);
    //! Raised during transaction abort.
    DEFINE_SIGNAL(void(TTransaction&), TransactionAborted);

public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration DefaultTransactionTimeout;
        TDuration TransactionAbortBackoffTime;

        TConfig()
        {
            Register("default_transaction_timeout", DefaultTransactionTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Seconds(15));
            Register("transaction_abort_backoff_time", TransactionAbortBackoffTime)
                .GreaterThan(TDuration())
                .Default(TDuration::Seconds(15));
        }
    };

    //! Creates an instance.
    TTransactionManager(
        TConfig* config,
        NCellMaster::TBootstrap* bootstrap);

    NObjectServer::IObjectProxy::TPtr GetRootTransactionProxy();

    TTransaction& Start(TTransaction* parent, TNullable<TDuration> timeout);
    void Commit(TTransaction& transaction);
    void Abort(TTransaction& transaction);
    void RenewLease(const TTransactionId& id);

    DECLARE_METAMAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

    //! Returns the list of all transaction ids on the path up to the root.
    //! This list includes #transactionId itself and #NullTransactionId.
    std::vector<TTransactionId> GetTransactionPath(const TTransactionId& transactionId) const;

private:
    typedef TTransactionManager TThis;
    class TTransactionTypeHandler;
    class TTransactionProxy;
    friend class TTransactionProxy;

    TConfig::TPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NMetaState::TMetaStateMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

    void OnTransactionExpired(const TTransactionId& id);

    void CreateLease(const TTransaction& transaction, TNullable<TDuration> timeout);
    void CloseLease(const TTransaction& transaction);
    void FinishTransaction(TTransaction& transaction);

    // TMetaStatePart overrides
    void SaveKeys(TOutputStream* output);
    void SaveValues(TOutputStream* output);
    void LoadKeys(TInputStream* input);
    void LoadValues(NCellMaster::TLoadContext context, TInputStream* input);
    virtual void Clear();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
