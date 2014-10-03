#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/yson/public.h>

#include <ytlib/tablet_client/tablet_service.pb.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/entity_map.h>

#include <server/hive/transaction_manager.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public NHive::ITransactionManager
{
public:
    //! Raised when a new transaction is started.
    DECLARE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is prepared.
    DECLARE_SIGNAL(void(TTransaction*), TransactionPrepared);

    //! Raised when a transaction is committed.
    DECLARE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DECLARE_SIGNAL(void(TTransaction*), TransactionAborted);

public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);

    ~TTransactionManager();

    TDuration GetActualTimeout(TNullable<TDuration> timeout);

    NHydra::TMutationPtr CreateStartTransactionMutation(
        const NTabletClient::NProto::TReqStartTransaction& request);

    //! Finds transaction by id, throws if nothing is found.
    TTransaction* GetTransactionOrThrow(const TTransactionId& id);

    void BuildOrchidYson(NYson::IYsonConsumer* consumer);
    

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    /// ITransactionManager overrides.
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

} // namespace NTabletNode
} // namespace NYT
