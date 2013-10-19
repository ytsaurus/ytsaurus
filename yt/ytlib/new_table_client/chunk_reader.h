#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const NChunkClient::NProto::TReadLimit& startLimit = NChunkClient::NProto::TReadLimit(),
    const NChunkClient::NProto::TReadLimit& endLimit = NChunkClient::NProto::TReadLimit(),
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
