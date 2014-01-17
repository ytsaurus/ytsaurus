#pragma once

#include "public.h"
#include "versioned_row.h"

#include <ytlib/chunk_client/writer_base.h>
#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Writes a schemed versioned rowset.
/*!
 *  Writes versioned rowset with given schema.
 *  Useful for: compactions.
 */
struct IVersionedWriter
    : public virtual NChunkClient::IWriterBase
{
    //! Initializes the writer. Must be called (and its result must be waited for)
    //! before making any other calls.
    // Inherited from IWriterBase.
    // virtual TAsyncError Open() override = 0;

    //! Enqueues more rows into the writer.
    /*!
     *  Value ids must correspond to column indexes in schema.
     *  Values must be sorted in ascending order by ids, and then in descending order by timestamps.
     *  
     *  If |false| is returned then the writer is overflowed (but the data is nevertheless accepted)
     *  The caller must wait for asynchronous flag provided by #GetReadyEvent to become set.
     *  The latter may indicate an error occurred while fetching more data.
     */
    virtual bool Write(const std::vector<TVersionedRow>& rows) = 0;

    //! Closes the writer.
    /*!
     *  Must be the last call to the writer.
     */
    // Inherited from IWriterBase.
    // virtual TAsyncError Close() override = 0;

    //! Returns an asynchronous flag enabling to wait until data is written.
    //! \see #Read.
    // Inherited from IWriterBase.
    // virtual TAsyncError GetReadyEvent() override = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
