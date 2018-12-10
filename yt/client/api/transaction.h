#pragma once

#include "public.h"
#include "client.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/client/tablet_client/public.h>

#include <yt/core/actions/signal.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionPrepareResult
{
    std::vector<NElection::TCellId> ParticipantCellIds;
};

struct TTransactionFlushResult
{
    TFuture<void> AsyncResult;
    std::vector<NElection::TCellId> ParticipantCellIds;
};

//! Either a write or delete.
struct TRowModification
{
    //! Discriminates between writes and deletes.
    ERowModificationType Type;
    //! Either a row (for write; versioned or unversioned) or a key (for delete; always unversioned).
    NTableClient::TTypeErasedRow Row;
};

struct TModifyRowsOptions
{
    //! If this happens to be a modification of a replicated table,
    //! controls if at least one sync replica is required.
    bool RequireSyncReplica = true;

    //! For writes to replicas, this is the id of the replica at the upstream cluster.
    NTabletClient::TTableReplicaId UpstreamReplicaId;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a client-controlled transaction.
/*
 *  Transactions are created by calling IClientBase::Transaction.
 *  
 *  For some table operations (e.g. #WriteRows), the transaction instance
 *  buffers all modifications and flushes them during #Commit. This, in
 *  particular, explains why these methods return |void|.
 *  
 *  Thread affinity: any
 */
struct ITransaction
    : public virtual IClientBase
{
    virtual IClientPtr GetClient() const = 0;
    virtual NTransactionClient::ETransactionType GetType() const = 0;
    virtual const NTransactionClient::TTransactionId& GetId() const = 0;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const = 0;
    virtual NTransactionClient::EAtomicity GetAtomicity() const = 0;
    virtual NTransactionClient::EDurability GetDurability() const = 0;
    virtual TDuration GetTimeout() const = 0;

    virtual TFuture<void> Ping(const NApi::TTransactionPingOptions& options = {}) = 0;
    virtual TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options = TTransactionCommitOptions()) = 0;
    virtual TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions()) = 0;
    virtual void Detach() = 0;
    virtual TFuture<TTransactionPrepareResult> Prepare() = 0;
    virtual TFuture<TTransactionFlushResult> Flush() = 0;

    DECLARE_INTERFACE_SIGNAL(void(), Committed);
    DECLARE_INTERFACE_SIGNAL(void(), Aborted);

    // Tables

    // TODO(sandello): Make write & delete non-virtual wrappers around ModifyRows.
    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TModifyRowsOptions& options = TModifyRowsOptions()) = 0;

    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const TModifyRowsOptions& options = TModifyRowsOptions()) = 0;

    virtual void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TKey> keys,
        const TModifyRowsOptions& options = TModifyRowsOptions()) = 0;

    virtual void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options = TModifyRowsOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

