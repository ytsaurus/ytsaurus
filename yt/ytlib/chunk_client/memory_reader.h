#pragma once

#include "public.h"

#include <core/misc/ref.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateMemoryReader(
    const NProto::TChunkMeta& meta,
    const std::vector<TSharedRef>& blocks);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
