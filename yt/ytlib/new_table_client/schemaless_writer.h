#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/chunk_writer_base.h>

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Writes a schemaless unversioned rowset.
/*!
 *  Writes unversioned rowset with schema and variable columns.
 *  Useful for: mapreduce jobs, write command.
 */
struct ISchemalessWriter
    : public virtual NChunkClient::IWriterBase
{
    virtual bool Write(const std::vector<TUnversionedRow>& rows) = 0;

};

DEFINE_REFCOUNTED_TYPE(ISchemalessWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
