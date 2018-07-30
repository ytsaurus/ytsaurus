#pragma once

#include "schema.h"
#include "unversioned_row.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/range.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Writes non-versioned, fixed-width, strictly typed rowset with given schema.
 *  Useful for: query engine.
 */
struct ISchemafulWriter
    : public virtual TRefCounted
{
    virtual TFuture<void> Close() = 0;

    /*!
     *  Every row must contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Write(TRange<TUnversionedRow> rows) = 0;

    virtual TFuture<void> GetReadyEvent() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemafulWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
