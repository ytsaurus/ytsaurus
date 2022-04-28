#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/proto/transaction_supervisor_service.pb.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/optional.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionPrepareOptions
{
    bool Persistent = false;

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
};

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

    virtual void PingTransaction(
        TTransactionId transactionId,
        bool pingAncestors) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
