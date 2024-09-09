#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ChaosClientLogger("ChaosClient");
inline const NLogging::TLogger ReplicationCardWatcherLogger("ReplicationCardWatcher");
inline const NLogging::TLogger ReplicationCardWatcherClientLogger("ReplicationCardWatcherClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
