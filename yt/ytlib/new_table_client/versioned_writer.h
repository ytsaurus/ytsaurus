#pragma once

#include "public.h"
#include "versioned_row.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Writes versioned rowset with given schema.
 *  Useful for: compactions.
 */
struct IVersionedWriter
    : public virtual TRefCounted
{
    virtual TAsyncError Open() = 0;

    /*!
     *  Value ids must correspond to column indexes in schema.
     *  Values must be sorted in ascending order by ids, and then in descending order by timestamps.
     */
    virtual bool Write(const std::vector<TVersionedRow>& rows) = 0;

    virtual TAsyncError Close() = 0;

    virtual TAsyncError GetReadyEvent() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
