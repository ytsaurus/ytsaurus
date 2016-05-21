#pragma once

#include "public.h"
#include "client.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/actions/signal.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TWriteRowsOptions
{
    // Use inserted aggregate column values as delta for aggregation.
    bool Aggregate = false;
};

struct TDeleteRowsOptions
{ };

///////////////////////////////////////////////////////////////////////////////

//! Represents a client-controlled transaction.
/*
 *  Transactions are created by calling IClientBase::Transaction.
 *  
 *  For some table operations (e.g. #WriteRows), the transaction instance
 *  buffers all modifications and flushes them during #Commit. This, in
 *  particular, explains why these methods return |void|.
 *  
 *  Thread affinity: single
 *  
 */
struct ITransaction
    : public IClientBase
{
    virtual IClientPtr GetClient() const = 0;
    virtual NTransactionClient::ETransactionType GetType() const = 0;
    virtual const NTransactionClient::TTransactionId& GetId() const = 0;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const = 0;
    virtual NTransactionClient::EAtomicity GetAtomicity() const = 0;
    virtual NTransactionClient::EDurability GetDurability() const = 0;

    virtual TFuture<void> Ping() = 0;
    virtual TFuture<void> Commit(const TTransactionCommitOptions& options = TTransactionCommitOptions()) = 0;
    virtual TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions()) = 0;
    virtual void Detach() = 0;

    DECLARE_INTERFACE_SIGNAL(void(), Committed);
    DECLARE_INTERFACE_SIGNAL(void(), Aborted);

    // Tables

    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TWriteRowsOptions& options = TWriteRowsOptions()) = 0;


    virtual void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TKey> keys,
        const TDeleteRowsOptions& options = TDeleteRowsOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransaction)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

