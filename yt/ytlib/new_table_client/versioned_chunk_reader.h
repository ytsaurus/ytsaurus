#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    const TChunkReaderConfigPtr& config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TReadLimit&& lowerLimit,
    TReadLimit&& upperLimit,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
