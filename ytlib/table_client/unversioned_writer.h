#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/core/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Writes a unschemaful unversioned rowset.
/*!
 *  Writes unversioned rowset with schema and variable columns.
 *  Useful for: mapreduce jobs, write command.
 */
struct IUnversionedWriter
    : public virtual NChunkClient::IWriterBase
{
    virtual bool Write(const TRange<TUnversionedRow>& rows) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
