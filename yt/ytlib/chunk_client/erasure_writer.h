#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/erasure_codecs/codec.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr GetErasureWriter(
    const TErasureWriterConfigPtr& config,
    const NErasure::ICodec* codec,
    const std::vector<IAsyncWriterPtr>& writers);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

