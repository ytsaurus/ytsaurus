#pragma once

#include "unversioned_row.h"

#include <yt/client/chunk_client/writer_base.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/range.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Writes non-versioned, fixed-width, strictly typed rowset with given schema.
 *  Useful for: query engine.
 */
struct IUnversionedRowsetWriter
    : public virtual NChunkClient::IWriterBase
{
    /*!
     *  Every row must contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Write(TRange<TUnversionedRow> rows) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedRowsetWriter)

////////////////////////////////////////////////////////////////////////////////

//! Writes a schemaless unversioned rowset.
/*!
 *  Writes unversioned rowset with schema and variable columns.
 *  Useful for: mapreduce jobs, write command.
 */
struct IUnversionedWriter
    : public IUnversionedRowsetWriter
{
    virtual bool Write(TRange<TUnversionedRow> rows) = 0;

    virtual const TNameTablePtr& GetNameTable() const = 0;

    virtual const TTableSchema& GetSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
