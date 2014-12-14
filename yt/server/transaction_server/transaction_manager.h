#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/actions/signal.h>

#include <server/hydra/entity_map.h>

#include <server/hive/transaction_manager.h>

#include <server/object_server/public.h>

#include <server/cypress_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public NHive::ITransactionManager
{
public:
    //! Raised when a new transaction is started.
    DECLARE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DECLARE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DECLARE_SIGNAL(void(TTransaction*), TransactionAborted);

    //! A set of transactions with no parent.
    DECLARE_BYREF_RO_PROPERTY(yhash_set<TTransaction*>, TopmostTransactions);

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
    class TImpl;
    class TTransactionTypeHandler;
    class TTransactionProxy;

    TIntrusivePtr<TImpl> Impl_;

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

};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
