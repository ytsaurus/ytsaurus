#pragma once

#include "public.h"
#include "client.h"

#include <ytlib/new_table_client/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TWriteRowsOptions
{
    NTabletClient::ELockMode LockMode = NTabletClient::ELockMode::Column;
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

    virtual TFuture<void> Commit(const TTransactionCommitOptions& options = TTransactionCommitOptions()) = 0;
    virtual TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions()) = 0;

    // Tables
    virtual void WriteRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        NVersionedTableClient::TUnversionedRow row,
        const TWriteRowsOptions& options = TWriteRowsOptions()) = 0;

    virtual void WriteRows(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        std::vector<NVersionedTableClient::TUnversionedRow> rows,
        const TWriteRowsOptions& options = TWriteRowsOptions()) = 0;
    
    virtual void DeleteRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TDeleteRowsOptions& options = TDeleteRowsOptions()) = 0;

    virtual void DeleteRows(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        std::vector<NVersionedTableClient::TKey> keys,
        const TDeleteRowsOptions& options = TDeleteRowsOptions()) = 0;

};

DEFINE_REFCOUNTED_TYPE(ITransaction)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

