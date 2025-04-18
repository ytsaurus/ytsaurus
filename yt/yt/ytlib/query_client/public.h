#pragma once

#include <yt/yt/core/misc/guid.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDistributedSessionCoordinator)

DECLARE_REFCOUNTED_CLASS(TMemoryProviderMapByTag)
DECLARE_REFCOUNTED_CLASS(TTrackedMemoryChunkProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
