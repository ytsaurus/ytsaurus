#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/chunk_client/input_chunk.h>

#pragma once

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TInputChunk& /* chunk */, std::ostream* /* os */);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
