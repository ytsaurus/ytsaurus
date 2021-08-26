#pragma once

#include "public.h"

#include <yt/yt/core/misc/pool_allocator.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct TMedium
{
    DEFINE_BYVAL_RO_PROPERTY(TMediumId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TMediumIndex, Index);

    DEFINE_BYREF_RW_PROPERTY(TString, Name);

    DEFINE_BYREF_RW_PROPERTY(TMediumConfigPtr, Config);

    explicit TMedium(NChunkServer::TMedium* medium);

    static std::unique_ptr<TMedium> FromPrimary(NChunkServer::TMedium* medium);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
