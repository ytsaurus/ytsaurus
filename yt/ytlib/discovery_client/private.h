#pragma once

#include <yt/core/logging/log.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger DiscoveryClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TListGroupsRequestSession)
DECLARE_REFCOUNTED_CLASS(TListMembersRequestSession)
DECLARE_REFCOUNTED_CLASS(TGetGroupSizeRequestSession)
DECLARE_REFCOUNTED_CLASS(THeartbeatSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
