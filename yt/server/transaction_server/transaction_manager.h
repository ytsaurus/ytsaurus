#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/hive/transaction_manager.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/server/object_server/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/misc/property.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public NHiveServer::ITransactionManager
{
public:
    //! Raised when a new transaction is started.
    DECLARE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DECLARE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DECLARE_SIGNAL(void(TTransaction*), TransactionAborted);

    //! A set of transactions with no parent.
    DECLARE_BYREF_RO_PROPERTY(THashSet<TTransaction*>, TopmostTransactions);

public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    TTransaction* StartTransaction(
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        const NObjectClient::TCellTagList& secondaryCellTags,
        const NObjectClient::TCellTagList& replicateToCellTags,
        TNullable<TDuration> timeout,
        TNullable<TInstant> deadline,
        const TNullable<TString>& title,
        const NYTree::IAttributeDictionary& attributes,
        const TTransactionId& hintId = NullTransactionId);
    void CommitTransaction(
        TTransaction* transaction,
        TTimestamp commitTimestamp);
    void AbortTransaction(
        TTransaction* transaction,
        bool force);

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction);

    //! Finds transaction by id, throws if nothing is found.
    TTransaction* GetTransactionOrThrow(const TTransactionId& transactionId);

    //! Asynchronously returns the (approximate) moment when transaction with
    //! a given #transactionId was last pinged.
    TFuture<TInstant> GetLastPingTime(const TTransaction* transaction);

    //! Sets the transaction timeout. Current lease is not renewed.
    void SetTransactionTimeout(
        TTransaction* transaction,
        TDuration timeout);

    //! Registers and references the object with the transaction.
    //! The same object can only be staged once.
    void StageObject(
        TTransaction* transaction,
        NObjectServer::TObjectBase* object);

    //! Unregisters the object from its staging transaction (which must be equal to #transaction),
    //! calls IObjectTypeHandler::UnstageObject and
    //! unreferences the object. Throws on failure.
    /*!
     *  If #recursive is |true| then all child objects are also released.
     */
    void UnstageObject(
        TTransaction* transaction,
        NObjectServer::TObjectBase* object,
        bool recursive);

    //! Registers (and references) the node with the transaction.
    void StageNode(
        TTransaction* transaction,
        NCypressServer::TCypressNodeBase* trunkNode);

    //! Registers and references the object with the transaction.
    //! The reference is dropped if the transaction aborts
    //! but is preserved if the transaction commits.
    //! The same object as be exported more than once.
    void ExportObject(
        TTransaction* transaction,
        NObjectServer::TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag);

    //! Registers and references the object with the transaction.
    //! The reference is dropped if the transaction aborts or aborts.
    //! The same object as be exported more than once.
    void ImportObject(
        TTransaction* transaction,
        NObjectServer::TObjectBase* object);

    void RegisterTransactionActionHandlers(
        const NHiveServer::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NHiveServer::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NHiveServer::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor);

    using TCtxStartTransaction = NRpc::TTypedServiceContext<
        NTransactionClient::NProto::TReqStartTransaction,
        NTransactionClient::NProto::TRspStartTransaction>;
    using TCtxStartTransactionPtr = TIntrusivePtr<TCtxStartTransaction>;
    std::unique_ptr<NHydra::TMutation> CreateStartTransactionMutation(
        TCtxStartTransactionPtr context,
        const NTransactionServer::NProto::TReqStartTransaction& request);

    using TCtxRegisterTransactionActions = NRpc::TTypedServiceContext<
        NProto::TReqRegisterTransactionActions,
        NProto::TRspRegisterTransactionActions>;
    using TCtxRegisterTransactionActionsPtr = TIntrusivePtr<TCtxRegisterTransactionActions>;
    std::unique_ptr<NHydra::TMutation> CreateRegisterTransactionActionsMutation(
        TCtxRegisterTransactionActionsPtr context);

private:
    class TImpl;
    class TTransactionTypeHandler;

    const TIntrusivePtr<TImpl> Impl_;

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
        bool pingAncestors) override;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
