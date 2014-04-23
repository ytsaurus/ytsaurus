#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/reader_base.h>

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessReader
    : public virtual NChunkClient::IReaderBase
{
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

};

DEFINE_REFCOUNTED_TYPE(ISchemalessReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
