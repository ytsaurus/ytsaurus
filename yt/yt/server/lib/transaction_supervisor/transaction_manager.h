#pragma once

#include "public.h"

#include <yt/yt/ytlib/transaction_supervisor/proto/transaction_supervisor_service.pb.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionPrepareOptions
{
    bool Persistent = false;

    bool LatePrepare = false;

    bool StronglyOrdered = false;

    TTimestamp PrepareTimestamp = NHiveClient::NullTimestamp;
    NApi::TClusterTag PrepareTimestampClusterTag = NObjectClient::InvalidCellTag;

    std::vector<TTransactionId> PrerequisiteTransactionIds;
};

struct TTransactionCommitOptions
{
    TTimestamp CommitTimestamp = NHiveClient::NullTimestamp;
    NApi::TClusterTag CommitTimestampClusterTag = NObjectClient::InvalidCellTag;

    void Persist(const TStreamPersistenceContext& context);
};

struct TTransactionAbortOptions
{
    bool Force = false;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(
    NProto::TTransactionPrepareOptions* protoOptions,
    const TTransactionPrepareOptions& options);
void FromProto(
    TTransactionPrepareOptions* options,
    const NProto::TTransactionPrepareOptions& protoOptions);

void ToProto(
    NProto::TTransactionCommitOptions* protoOptions,
    const TTransactionCommitOptions& options);
void FromProto(
    TTransactionCommitOptions* options,
    const NProto::TTransactionCommitOptions& protoOptions);

void ToProto(
    NProto::TTransactionAbortOptions* protoOptions,
    const TTransactionAbortOptions& options);
void FromProto(
    TTransactionAbortOptions* options,
    const NProto::TTransactionAbortOptions& protoOptions);

////////////////////////////////////////////////////////////////////////////////

struct ITransactionManager
    : public virtual TRefCounted
{
    virtual TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        const std::vector<TCellId>& cellIdsToSyncWith) = 0;

    virtual void PrepareTransactionCommit(
        TTransactionId transactionId,
        const TTransactionPrepareOptions& options) = 0;

    virtual void PrepareTransactionAbort(
        TTransactionId transactionId,
        const TTransactionAbortOptions& options) = 0;

    //! Once #PrepareTransactionCommit succeeded, #CommitTransaction cannot throw.
    virtual void CommitTransaction(
        TTransactionId transactionId,
        const TTransactionCommitOptions& options) = 0;

    virtual void AbortTransaction(
        TTransactionId transactionId,
        const TTransactionAbortOptions& options) = 0;

    virtual TFuture<void> PingTransaction(
        TTransactionId transactionId,
        bool pingAncestors) = 0;

    //! These methods allows to override Commit and Abort RPC calls for some
    //! of the transactions (for example, Cypress transactions that does not require
    //! 2PC for commit).
    //! These methods either return false and default transaction supervisor logic
    //! is used or return true and then transaction manager is responsible for request
    //! handling.
    // TODO(gritukan): Probably this will become obsolete once Cypress Proxy is implemented
    // and all Cypress transactions are replicated to Sequoia.
    using TCtxCommitTransaction = NRpc::TTypedServiceContext<
        NProto::NTransactionSupervisor::TReqCommitTransaction,
        NProto::NTransactionSupervisor::TRspCommitTransaction>;
    using TCtxCommitTransactionPtr = TIntrusivePtr<TCtxCommitTransaction>;

    // COMPAT(h0pless): Remove this after CTxS will be used by clients to manipulate Cypress transactions.
    virtual bool CommitTransaction(TCtxCommitTransactionPtr context) = 0;

    using TCtxAbortTransaction = NRpc::TTypedServiceContext<
        NProto::NTransactionSupervisor::TReqAbortTransaction,
        NProto::NTransactionSupervisor::TRspAbortTransaction>;
    using TCtxAbortTransactionPtr = TIntrusivePtr<TCtxAbortTransaction>;

    // COMPAT(h0pless): Remove this after CTxS will be used by clients to manipulate Cypress transactions.
    virtual bool AbortTransaction(TCtxAbortTransactionPtr context) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
