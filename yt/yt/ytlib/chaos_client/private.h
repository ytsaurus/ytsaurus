#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChaosClientLogger, "ChaosClient");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ReplicationCardWatcherLogger, "ReplicationCardWatcher");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ReplicationCardWatcherClientLogger, "ReplicationCardWatcherClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
