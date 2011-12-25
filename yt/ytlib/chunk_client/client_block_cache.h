#pragma once

#include "block_cache.h"
#include "../misc/configurable.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TClientCacheConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TClientCacheConfig> TPtr;

    //! The maximum number of bytes that block are allowed to occupy.
    //! Zero means that no blocks are cached.
    i64 MaxSize;

    TClientCacheConfig()
    {
        Register("max_size", MaxSize).Default(0).GreaterThanOrEqual(0);
    }
};

//! Creates a simple client-side block cache.
IBlockCache::TPtr CreateClientBlockCache(TClientCacheConfig* config);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
