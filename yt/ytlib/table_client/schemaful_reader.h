#pragma once

#include "public.h"

#include <core/actions/future.h>

namespace NYT {
namespace NTableClient {

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

} // namespace NTableClient
} // namespace NYT
