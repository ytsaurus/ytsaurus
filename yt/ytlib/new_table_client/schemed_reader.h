#pragma once

#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Reads non-versioned, fixed-width, strictly typed rowset with given schema.
 *  Useful for: query engine.
 */
struct ISchemedReader 
    : public virtual TRefCounted
{
    /*!
     *  \note 
     *  Read timestamp and read limits should be passed in constructor if applicable.
     */
    virtual TAsyncError Open() = 0;

    /*!
     *  Every row will contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
