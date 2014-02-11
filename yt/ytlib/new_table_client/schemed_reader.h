#pragma once

#include "public.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reads a non-versioned, fixed-width, strictly typed rowset with given a schema.
/*!
 *  The contract is mostly same as in IVersionedReader.
 *  
 *  Useful for: query engine.
 */
struct ISchemedReader 
    : public virtual TRefCounted
{
    //! Initializes the reader. Must be called (and its result must be waited for)
    //! before making any other calls.
    /*!
     *  \note 
     *  Read timestamp and read limits should be passed in constructor if applicable.
     */
    virtual TAsyncError Open(const TTableSchema& schema) = 0;

    //! See #IVersionedReader::Read.
    /*!
     *  \note
     *  Every row will contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

    //! See #IVersionedReader::GetReadyEvent.
    virtual TAsyncError GetReadyEvent() = 0;

};

DEFINE_REFCOUNTED_TYPE(ISchemedReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
