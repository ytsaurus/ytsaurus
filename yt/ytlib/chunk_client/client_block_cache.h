#pragma once

#include "public.h"

#include <core/misc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Creates a simple client-side block cache.
IBlockCachePtr CreateClientBlockCache(TSlruCacheConfigPtr config);

//! Returns an always-empty block cache.
IBlockCachePtr GetNullBlockCache();

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
