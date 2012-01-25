#pragma once

#include "common.h"

#include "block_cache.h"
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TClientBlockCacheConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TClientBlockCacheConfig> TPtr;

    //! The maximum number of bytes that block are allowed to occupy.
    //! Zero means that no blocks are cached.
    i64 MaxSize;

    TClientBlockCacheConfig()
    {
        Register("max_size", MaxSize)
            .Default(0)
            .GreaterThanOrEqual(0);
    }
};

//! Creates a simple client-side block cache.
IBlockCache::TPtr CreateClientBlockCache(TClientBlockCacheConfig* config);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
