#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DiscoveryClientLogger, "DiscoveryClient");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TListMembersRequestSession)
DECLARE_REFCOUNTED_CLASS(TGetGroupMetaRequestSession)
DECLARE_REFCOUNTED_CLASS(THeartbeatSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
