#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/chunk_client/input_chunk.h>

#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const TIntrusivePtr<NChunkClient::TInputChunk>& chunk, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
