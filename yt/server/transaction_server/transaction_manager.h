#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/misc/property.h>
#include <core/misc/id_generator.h>
#include <core/misc/lease_manager.h>

#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/mutation.h>
#include <ytlib/meta_state/map.h>

#include <server/object_server/public.h>

#include <server/cell_master/public.h>

#include <server/object_server/type_handler.h>

#include <server/cypress_server/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

//! Manages client transactions.
class TTransactionManager
    : public NMetaState::TMetaStatePart
{
    //! Raised when a new transaction is started.
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TTransaction*>, TopmostTransactions);

public:
    //! Creates an instance.
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Inititialize();

    TTransaction* StartTransaction(TTransaction* parent, TNullable<TDuration> timeout);
    void CommitTransaction(TTransaction* transaction);
    void AbortTransaction(TTransaction* transaction);
    void PingTransaction(const TTransaction* transaction, bool pingAncestors = false);

    DECLARE_METAMAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

    //! Returns the list of all transaction ids on the path up to the root.
    //! This list includes #transaction itself and |nullptr|.
    TTransactionPath GetTransactionPath(TTransaction* transaction) const;

    //! Registers and references the object with the transaction.
    void StageObject(TTransaction* transaction, NObjectServer::TObjectBase* object);

    //! Unregisters the object from the transaction, calls IObjectTypeHandler::Unstage and
    //! unreferences the object. Throws on failure.
    /*!
     *  If #recursive is |true| then all child objects are also released.
     */
    void UnstageObject(
        TTransaction* transaction,
        NObjectServer::TObjectBase* object,
        bool recursive);

    //! Registers (and references) the node with the transaction.
    void StageNode(TTransaction* transaction, NCypressServer::TCypressNodeBase* node);

private:
    typedef TTransactionManager TThis;
    class TTransactionTypeHandler;
    class TTransactionProxy;
    friend class TTransactionProxy;

    TTransactionManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NMetaState::TMetaStateMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    void OnTransactionExpired(const TTransactionId& id);

    void CreateLease(const TTransaction* transaction, TDuration timeout);
    void CloseLease(const TTransaction* transaction);
    void FinishTransaction(TTransaction* transaction);

    void DoPingTransaction(const TTransaction* transaction);

    // TMetaStatePart overrides
    virtual void OnActiveQuorumEstablished() override;
    virtual void OnStopLeading() override;

    void SaveKeys(NCellMaster::TSaveContext& context);
    void SaveValues(NCellMaster::TSaveContext& context);

    virtual void OnBeforeLoaded() override;
    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    virtual void OnAfterLoaded() override;

    void DoClear();
    virtual void Clear() override;

    TDuration GetActualTimeout(TNullable<TDuration> timeout);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
