#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemafulWriterPtr CreateSchemafulChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
