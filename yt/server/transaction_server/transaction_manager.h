#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/misc/property.h>
#include <core/misc/id_generator.h>
#include <core/misc/lease_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/mutation.h>
#include <server/hydra/entity_map.h>

#include <server/hive/transaction_manager.h>

#include <server/object_server/public.h>

#include <server/cell_master/automaton.h>

#include <server/object_server/type_handler.h>

#include <server/cypress_server/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public NCellMaster::TMasterAutomatonPart
    , public NHive::ITransactionManager
{
public:
    //! Raised when a new transaction is started.
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TTransaction*>, TopmostTransactions);

public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    TTransaction* StartTransaction(TTransaction* parent, TNullable<TDuration> timeout);
    void CommitTransaction(TTransaction* transaction);
    void AbortTransaction(TTransaction* transaction, bool force);
    void PingTransaction(TTransaction* transaction, bool pingAncestors = false);

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

    //! Finds transaction by id, throws if nothing is found.
    TTransaction* GetTransactionOrThrow(const TTransactionId& id);

    //! Returns the list of all transaction ids on the path up to the root.
    //! This list includes #transaction itself and |nullptr|.
    TTransactionPath GetTransactionPath(TTransaction* transaction) const;

    //! Registers and references the object with the transaction.
    void StageObject(TTransaction* transaction, NObjectServer::TObjectBase* object);

    //! Unregisters the object from its staging transaction,
    //! calls IObjectTypeHandler::Unstage and
    //! unreferences the object. Throws on failure.
    /*!
     *  If #recursive is |true| then all child objects are also released.
     */
    void UnstageObject(NObjectServer::TObjectBase* object, bool recursive);

    //! Registers (and references) the node with the transaction.
    void StageNode(TTransaction* transaction, NCypressServer::TCypressNodeBase* node);

private:
    typedef TTransactionManager TThis;
    class TTransactionTypeHandler;
    class TTransactionProxy;
    friend class TTransactionProxy;

    TTransactionManagerConfigPtr Config;

    NHydra::TEntityMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLease> LeaseMap;

    void OnTransactionExpired(const TTransactionId& id);

    void CreateLease(TTransaction* transaction, TDuration timeout);
    void CloseLease(TTransaction* transaction);
    void FinishTransaction(TTransaction* transaction);

    void DoPingTransaction(TTransaction* transaction);

    // TAutomatonPart overrides
    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    void SaveKeys(NCellMaster::TSaveContext& context);
    void SaveValues(NCellMaster::TSaveContext& context);

    virtual void OnBeforeSnapshotLoaded() override;
    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    virtual void OnAfterSnapshotLoaded() override;

    void DoClear();
    virtual void Clear() override;

    TDuration GetActualTimeout(TNullable<TDuration> timeout);

    // ITransactionManager overrides
    virtual void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp) override;
    virtual void PrepareTransactionAbort(
        const TTransactionId& transactionId,
        bool force) override;
    virtual void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp) override;
    virtual void AbortTransaction(
        const TTransactionId& transactionId,
        bool force) override;
    virtual void PingTransaction(
        const TTransactionId& transactionId,
        const NHive::NProto::TReqPingTransaction& request) override;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
