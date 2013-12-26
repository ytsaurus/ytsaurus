#pragma once

#include "public.h"
#include "versioned_row.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Reads versioned rowset with given schema.
 *  Versioned rowset implies that it is:
 *  1. Schemed.
 *  2. Sorted.
 *  3. No two rows share the same key.
 *
 *  Useful for: merging and compactions.
 */
struct IVersionedReader
    : public virtual TRefCounted
{
    virtual TAsyncError Open() = 0;

    /*!
     *  Depending on implementation, rows may come in two different flavours.
     *  1. Rows containing no more than one versioned value for each cell, 
     *     and exactly one timestamp, either tombstone or last committed (for merging).
     *  2. Rows containing all available versions for  and a list of timestamps (for compactions).
     *
     *  Value ids correspond to column indexes in schema.
     *  Values are sorted in ascending order by ids, and then in descending order by timestamps.
     *
     *  Returns false if reading is complete.
     *  If returns true and empty rowset, caller should wait for ready event, until new data is available.
     */
    virtual bool Read(std::vector<TVersionedRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
