#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionManager);
DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionCoordinator);

DECLARE_REFCOUNTED_STRUCT(TDistributedChunkSessionServiceConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
