#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/small_vector.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TColumnFilter
{
    TColumnFilter();
    TColumnFilter(const std::vector<Stroka>& columns);
    TColumnFilter(const TColumnFilter& other);

    bool All;
    SmallVector<Stroka, NVersionedTableClient::TypicalColumnCount> Columns;
};

struct TLookupOptions
{
    TLookupOptions();

    TColumnFilter ColumnFilter;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp;

};

///////////////////////////////////////////////////////////////////////////////

struct ITransaction
    : public virtual TRefCounted
{
    virtual const NTransactionClient::TTransactionId& GetId() = 0;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() = 0;

    virtual TAsyncError Commit() = 0;
    virtual TAsyncError Abort() = 0;

    virtual void WriteRow(
        const NYPath::TYPath& tablePath,
        NVersionedTableClient::TUnversionedRow row) = 0;
    virtual void WriteRows(
        const NYPath::TYPath& tablePath,
        std::vector<NVersionedTableClient::TUnversionedRow> rows) = 0;
    
    virtual void DeleteRow(
        const NYPath::TYPath& tablePath,
        NVersionedTableClient::TKey key) = 0;
    virtual void DeleteRows(
        const NYPath::TYPath& tablePath,
        std::vector<NVersionedTableClient::TKey> keys) = 0;

    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRow(
        const NYPath::TYPath& tablePath,
        NVersionedTableClient::TKey key,
        const TLookupOptions& options = TLookupOptions()) = 0;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

