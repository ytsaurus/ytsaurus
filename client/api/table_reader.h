#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/core/actions/future.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct ITableReader
    : public virtual TRefCounted
{
    //! Returns the current row index within the table.
    virtual i64 GetTableRowIndex() const = 0;

    //! Returns the total (approximate) number of rows readable.
    virtual i64 GetTotalRowCount() const = 0;

    //! Returns various data statistics.
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

    //! Returns an asynchronous flag enabling to wait until data is avilable.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Attempts to read a bunch of #rows. If false is returned then the rows
    //! are not immediately available and the client must invoke #GetReadyEvent and wait.
    virtual bool Read(std::vector<NTableClient::TUnversionedRow>* rows) = 0;

    //! Returns the name table used for constructing rows.
    virtual const NTableClient::TNameTablePtr& GetNameTable() const = 0;

    //! Returns the schema used for constructing rows.
    virtual NTableClient::TKeyColumns GetKeyColumns() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
