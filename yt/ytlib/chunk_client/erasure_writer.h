#pragma once

#include "public.h"

#include <ytlib/erasure/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ICodec* codec,
    const std::vector<IAsyncWriterPtr>& writers);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

