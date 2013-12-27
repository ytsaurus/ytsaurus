#pragma once

#include "public.h"
#include "versioned_row.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reads a schemed versioned rowset.
/*!
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
    //! Initializes the reader. Must be called (and its result must be waited for)
    //! before making any other calls.
    virtual TAsyncError Open() = 0;

    //! Tries to read more rows from the reader.
    /*!
     *  Depending on implementation, rows may come in two different flavours.
     *  (A) Rows containing no more than one versioned value for each cell, 
     *      and exactly one timestamp, either tombstone or last committed (for merging).
     *  (B) Rows containing all available versions and a list of timestamps (for compactions).
     *
     *  Value ids correspond to column indexes in schema.
     *  Values are sorted in ascending order by ids, and then in descending order by timestamps.
     *
     *  If |false| is returned then the end of the rowset is reached.
     *  If |true| is returned but |rows| is empty then no more data is available at the moment.
     *  The caller must wait for asynchronous flag provided by #GetReadyEvent to become set.
     *  The latter may indicate an error occured while fetching more data.
     *
     *  In Case A above row timestamps have the following meaning:
     *  1. If the row is found and is known to be deleted then the deletion
     *     timestamp combined with |TombstoneTimestampMask| is provided.
     *  2. If the row is found and is known to exist then the earliest modification
     *     timestamp is provided.
     *  3. If the store has no tombstone for |key| (up to |timestamp|)
     *     then the timestamp is combined with |IncrementalTimestampMask|
     *     to indicate that the client may potentially need to consult other stores
     *     to get the full set of column values.
     */
    virtual bool Read(std::vector<TVersionedRow>* rows) = 0;

    //! Returns an asynchronous flag enabling to wait for more data to come.
    //! \see #Read.
    virtual TAsyncError GetReadyEvent() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
