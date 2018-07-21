#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct ITableWriter
    : public virtual TRefCounted
{
    //! Attempts to write a bunch of #rows. If false is returned then the rows
    //! are not accepted and the client must invoke #GetReadyEvent and wait.
    virtual bool Write(const TRange<NTableClient::TUnversionedRow>& rows) = 0;

    //! Returns an asynchronous flag enabling to wait until data is written.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Closes the writer. Must be the last call to the writer.
    virtual TFuture<void> Close() = 0;

    //! Returns the name table to be used for constructing rows.
    virtual const NTableClient::TNameTablePtr& GetNameTable() const = 0;

    //! Returns the schema to be used for constructing rows.
    virtual const NTableClient::TTableSchema& GetSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
