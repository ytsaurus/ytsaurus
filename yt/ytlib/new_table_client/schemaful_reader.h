#pragma once

#include "public.h"

#include <core/actions/future.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reads a non-versioned, fixed-width, strictly typed rowset with given a schema.
/*!
 *  The contract is mostly same as in IVersionedReader.
 *  
 *  Useful for: query engine.
 */
struct ISchemafulReader 
    : public virtual TRefCounted
{
    //! Initializes the reader. Must be called (and its result must be waited for)
    //! before making any other calls.
    virtual TFuture<void> Open(const TTableSchema& schema) = 0;

    //! See #IVersionedReader::Read.
    /*!
     *  \note
     *  Every row will contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

    //! See #IVersionedReader::GetReadyEvent.
    virtual TFuture<void> GetReadyEvent() = 0;

};

DEFINE_REFCOUNTED_TYPE(ISchemafulReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
