#pragma once

#include "public.h"

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/client/election/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionManager
    : public NTransactionSupervisor::ITransactionManager
{
    //! Raised when a new transaction is started.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionAborted);

    virtual void Initialize() = 0;

    virtual TTransaction* StartTransaction(
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        const NObjectClient::TCellTagList& replicatedToCellTags,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        const std::optional<TString>& title,
        const NYTree::IAttributeDictionary& attributes,
        bool isCypressTransaction,
        TTransactionId hintId = NullTransactionId) = 0;
    virtual void CommitMasterTransaction(
        TTransaction* transaction,
        const NTransactionSupervisor::TTransactionCommitOptions& options) = 0;
    virtual void AbortMasterTransaction(
        TTransaction* transaction,
        const NTransactionSupervisor::TTransactionAbortOptions& options) = 0;
    virtual TTransaction* StartUploadTransaction(
        TTransaction* parent,
        const NObjectClient::TCellTagList& replicatedToCellTags,
        std::optional<TDuration> timeout,
        const std::optional<TString>& title,
        TTransactionId hintId) = 0;
    virtual TTransactionId ExternalizeTransaction(
        TTransaction* transaction,
        NObjectClient::TCellTagList dstCellTags) = 0;
    virtual TTransactionId GetNearestExternalizedTransactionAncestor(
        TTransaction* transaction,
        NObjectClient::TCellTag dstCellTag) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction);

    virtual NHydra::TEntityMap<TTransaction>* MutableTransactionMap() = 0;

    virtual const THashSet<TTransaction*>& NativeTransactions() const = 0;
    virtual const THashSet<TTransaction*>& NativeTopmostTransactions() const = 0;

    //! Finds transaction by id, throws if nothing is found.
    virtual TTransaction* GetTransactionOrThrow(TTransactionId transactionId) = 0;

    //! Asynchronously returns the (approximate) moment when transaction with
    //! a given #transactionId was last pinged.
    virtual TFuture<TInstant> GetLastPingTime(const TTransaction* transaction) = 0;

    //! Sets the transaction timeout. Current lease is not renewed.
    virtual void SetTransactionTimeout(
        TTransaction* transaction,
        TDuration timeout) = 0;

    //! Registers and references the object with the transaction.
    //! The same object can only be staged once.
    virtual void StageObject(
        TTransaction* transaction,
        NObjectServer::TObject* object) = 0;

    //! Unregisters the object from its staging transaction (which must be equal to #transaction),
    //! calls IObjectTypeHandler::UnstageObject and
    //! unreferences the object. Throws on failure.
    /*!
     *  If #recursive is |true| then all child objects are also released.
     */
    virtual void UnstageObject(
        TTransaction* transaction,
        NObjectServer::TObject* object,
        bool recursive) = 0;

    //! Registers (and references) the node with the transaction.
    virtual void StageNode(
        TTransaction* transaction,
        NCypressServer::TCypressNode* trunkNode) = 0;

    //! Registers and references the object with the transaction.
    //! The reference is dropped if the transaction aborts
    //! but is preserved if the transaction commits.
    //! The same object as be exported more than once.
    virtual void ExportObject(
        TTransaction* transaction,
        NObjectServer::TObject* object,
        NObjectClient::TCellTag destinationCellTag) = 0;

    //! Registers and references the object with the transaction.
    //! The reference is dropped if the transaction aborts or commits.
    //! The same object as be exported more than once.
    virtual void ImportObject(
        TTransaction* transaction,
        NObjectServer::TObject* object) = 0;

    virtual void RegisterTransactionActionHandlers(
        const NTransactionSupervisor::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NTransactionSupervisor::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NTransactionSupervisor::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor) = 0;

    using TCtxStartTransaction = NRpc::TTypedServiceContext<
        NTransactionClient::NProto::TReqStartTransaction,
        NTransactionClient::NProto::TRspStartTransaction>;
    using TCtxStartTransactionPtr = TIntrusivePtr<TCtxStartTransaction>;
    virtual std::unique_ptr<NHydra::TMutation> CreateStartTransactionMutation(
        TCtxStartTransactionPtr context,
        const NTransactionServer::NProto::TReqStartTransaction& request) = 0;

    using TCtxStartCypressTransaction = NRpc::TTypedServiceContext<
        NCypressTransactionClient::NProto::TReqStartTransaction,
        NCypressTransactionClient::NProto::TRspStartTransaction>;
    using TCtxStartCypressTransactionPtr = TIntrusivePtr<TCtxStartCypressTransaction>;
    virtual std::unique_ptr<NHydra::TMutation> CreateStartCypressTransactionMutation(
        TCtxStartCypressTransactionPtr context,
        const NTransactionServer::NProto::TReqStartCypressTransaction& request) = 0;

    using TCtxRegisterTransactionActions = NRpc::TTypedServiceContext<
        NProto::TReqRegisterTransactionActions,
        NProto::TRspRegisterTransactionActions>;
    using TCtxRegisterTransactionActionsPtr = TIntrusivePtr<TCtxRegisterTransactionActions>;
    virtual std::unique_ptr<NHydra::TMutation> CreateRegisterTransactionActionsMutation(
        TCtxRegisterTransactionActionsPtr context) = 0;

    using TCtxReplicateTransactions = NRpc::TTypedServiceContext<
        NProto::TReqReplicateTransactions,
        NProto::TRspReplicateTransactions>;
    using TCtxReplicateTransactionsPtr = TIntrusivePtr<TCtxReplicateTransactions>;
    virtual std::unique_ptr<NHydra::TMutation> CreateReplicateTransactionsMutation(
        TCtxReplicateTransactionsPtr context) = 0;

    virtual void CreateOrRefTimestampHolder(TTransactionId transactionId) = 0;
    virtual void SetTimestampHolderTimestamp(TTransactionId transactionId, TTimestamp timestamp) = 0;
    virtual TTimestamp GetTimestampHolderTimestamp(TTransactionId transactionId) = 0;
    virtual void UnrefTimestampHolder(TTransactionId transactionId) = 0;

    virtual const TTransactionPresenceCachePtr& GetTransactionPresenceCache() = 0;

    virtual void StartCypressTransaction(const TCtxStartCypressTransactionPtr& context) = 0;

    using TCtxCommitCypressTransaction = NRpc::TTypedServiceContext<
        NCypressTransactionClient::NProto::TReqCommitTransaction,
        NCypressTransactionClient::NProto::TRspCommitTransaction>;
    using TCtxCommitCypressTransactionPtr = TIntrusivePtr<TCtxCommitCypressTransaction>;
    virtual void CommitCypressTransaction(const TCtxCommitCypressTransactionPtr& context) = 0;

    using TCtxAbortCypressTransaction = NRpc::TTypedServiceContext<
        NCypressTransactionClient::NProto::TReqAbortTransaction,
        NCypressTransactionClient::NProto::TRspAbortTransaction>;
    using TCtxAbortCypressTransactionPtr = TIntrusivePtr<TCtxAbortCypressTransaction>;
    virtual void AbortCypressTransaction(const TCtxAbortCypressTransactionPtr& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionManager)

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
