#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedWriterPtr CreateVersionedChunkWriter(
    const TChunkWriterConfigPtr& config,
    const TChunkWriterOptionsPtr& options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const NChunkClient::IAsyncWriterPtr& asyncWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
