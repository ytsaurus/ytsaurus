#pragma once

#include "public.h"

#include <ytlib/erasure_codecs/public.h>

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

