#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NDistributedChunkSession {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionManager);
DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionCoordinator);

DECLARE_REFCOUNTED_CLASS(TDistributedChunkSessionServiceConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSession
