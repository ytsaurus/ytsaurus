#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): deprecated, remove.
IWriterPtr CreateChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IAsyncWriterPtr asyncWriter);

////////////////////////////////////////////////////////////////////////////////

ISchemafulWriterPtr CreateSchemafulChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IAsyncWriterPtr asyncWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
