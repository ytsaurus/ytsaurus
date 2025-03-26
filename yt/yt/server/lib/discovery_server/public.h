#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/ytlib/discovery_client/public.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DiscoveryServerLogger, "DiscoveryServer");

////////////////////////////////////////////////////////////////////////////////

using TGroupId = NDiscoveryClient::TGroupId;
using TMemberId = NDiscoveryClient::TMemberId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMember)
DECLARE_REFCOUNTED_CLASS(TGroup)
DECLARE_REFCOUNTED_CLASS(TGroupManager)
DECLARE_REFCOUNTED_CLASS(TGroupTree)

DECLARE_REFCOUNTED_STRUCT(IDiscoveryServer)

DECLARE_REFCOUNTED_STRUCT(TDiscoveryServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
