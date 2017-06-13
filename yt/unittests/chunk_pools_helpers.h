#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/chunk_client/input_chunk.h>

#pragma once

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TInputChunk& /* chunk */, std::ostream* /* os */);

////////////////////////////////////////////////////////////////////////////////

} // namesapce NChunkClient
} // namespace NYT
