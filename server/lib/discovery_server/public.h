#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/logging/log.h>

#include <yt/ytlib/discovery_client/public.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger DiscoveryServerLogger;

////////////////////////////////////////////////////////////////////////////////

using TGroupId = NDiscoveryClient::TGroupId;
using TMemberId = NDiscoveryClient::TMemberId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMember)
DECLARE_REFCOUNTED_CLASS(TGroup)
DECLARE_REFCOUNTED_CLASS(TGroupManager)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServerConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServer)
DECLARE_REFCOUNTED_CLASS(TGroupTree)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((NoSuchGroup)    (2300))
    ((NoSuchMember)   (2301))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
