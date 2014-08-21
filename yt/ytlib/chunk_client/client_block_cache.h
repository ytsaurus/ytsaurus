#pragma once

#include "public.h"

#include <core/misc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Creates a simple client-side block cache.
IBlockCachePtr CreateClientBlockCache(TSlruCacheConfigPtr config);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
